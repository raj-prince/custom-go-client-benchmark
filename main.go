package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"

	// Register the pprof endpoints under the web server root at /debug/pprof
	_ "net/http/pprof"
	"os"
	"strconv"
	"time"

	control "cloud.google.com/go/storage/control/apiv2"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"cloud.google.com/go/profiler"
	"cloud.google.com/go/storage"
	"cloud.google.com/go/storage/experimental"
	"github.com/googleapis/gax-go/v2"
	"go.opencensus.io/stats"
	"golang.org/x/oauth2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
	"cloud.google.com/go/storage/control/apiv2/controlpb"
)

var (
	grpcConnPoolSize    = 1
	maxConnsPerHost     = 100
	maxIdleConnsPerHost = 100

	// MB means 1024 Kb.
	MB = 1024 * 1024

	numOfWorker = flag.Int("worker", 48, "Number of concurrent worker to read")

	numOfReadCallPerWorker = flag.Int("read-call-per-worker", 1000000, "Number of read call per worker")

	maxRetryDuration = 30 * time.Second

	retryMultiplier = 2.0

	bucketName = flag.String("bucket", "princer-working-dirs", "GCS bucket name.")

	// ProjectName denotes gcp project name.
	ProjectName = flag.String("project", "gcs-fuse-test", "GCP project name.")

	clientProtocol   = flag.String("client-protocol", "http", "Network protocol.")

	// Object name = objectNamePrefix + {thread_id} + objectNameSuffix
	objectNamePrefix = flag.String("obj-prefix", "princer_100M_files/file_", "Object prefix")
	objectNameSuffix = flag.String("obj-suffix", "", "Object suffix")

	tracerName      = "princer-storage-benchmark"
	enableTracing   = flag.Bool("enable-tracing", false, "Enable tracing with Cloud Trace export")
	enablePprof     = flag.Bool("enable-pprof", false, "Enable pprof server")
	traceSampleRate = flag.Float64("trace-sample-rate", 1.0, "Sampling rate for Cloud Trace")

	// Cloud profiler.
	enableCloudProfiler = flag.Bool("enable-cloud-profiler", false, "Enable cloud profiler")
	enableHeap          = flag.Bool("heap", false, "enable heap profile collection")
	enableCPU           = flag.Bool("cpu", true, "enable cpu profile collection")
	enableHeapAlloc     = flag.Bool("heap_alloc", false, "enable heap allocation profile collection")
	enableThread        = flag.Bool("thread", false, "enable thread profile collection")
	enableContention    = flag.Bool("contention", false, "enable contention profile collection")
	projectID           = flag.String("project_id", "", "project ID to run profiler with; only required when running outside of GCP.")
	version             = flag.String("version", "original", "version to run profiler with")

	// Enable read stall retry.
	enableReadStallRetry = flag.Bool("enable-read-stall-retry", false, "Enable read stall retry")

	eG errgroup.Group
)

// CreateHTTPClient create http storage client.
func CreateHTTPClient(ctx context.Context, isHTTP2 bool) (client *storage.Client, err error) {
	var transport *http.Transport
	// Using http1 makes the client more performant.
	if !isHTTP2 {
		transport = &http.Transport{
			MaxConnsPerHost:     maxConnsPerHost,
			MaxIdleConnsPerHost: maxIdleConnsPerHost,
			// This disables HTTP/2 in transport.
			TLSNextProto: make(
				map[string]func(string, *tls.Conn) http.RoundTripper,
			),
		}
	} else {
		// For http2, change in MaxConnsPerHost doesn't affect the performance.
		transport = &http.Transport{
			DisableKeepAlives: true,
			MaxConnsPerHost:   maxConnsPerHost,
			ForceAttemptHTTP2: true,
		}
	}

	tokenSource, err := GetTokenSource(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("while generating tokenSource, %v", err)
	}

	// Custom http client for Go Client.
	httpClient := &http.Client{
		Transport: &oauth2.Transport{
			Base:   transport,
			Source: tokenSource,
		},
		Timeout: 0,
	}

	// Setting UserAgent through RoundTripper middleware
	httpClient.Transport = &userAgentRoundTripper{
		wrapped:   httpClient.Transport,
		UserAgent: "prince",
	}

	if *enableReadStallRetry {
		return storage.NewClient(ctx, option.WithHTTPClient(httpClient),
			experimental.WithReadStallTimeout(&experimental.ReadStallTimeoutConfig{
				Min: time.Second,
				TargetPercentile: 0.99,
			}))
	}
	return storage.NewClient(ctx, option.WithHTTPClient(httpClient))
}

// CreateGrpcClient creates grpc client.
func CreateGrpcClient(ctx context.Context) (client *storage.Client, err error) {
	tokenSource, err := GetTokenSource(ctx, "")
	if err != nil {
		return nil, err
	}
	return storage.NewGRPCClient(ctx, option.WithGRPCConnectionPool(grpcConnPoolSize), option.WithTokenSource(tokenSource), storage.WithDisabledClientMetrics())
}

// CreateStorageControlClient creates control client.
func CreateAndPerformControlClientOperation(ctx context.Context) (err error) {
	tokenSource, err := GetTokenSource(ctx, "")
	if err != nil {
		return err
	}

	// , storage.WithDisabledClientMetrics()
	ctrlClient, err := control.NewStorageControlClient(ctx, option.WithGRPCConnectionPool(grpcConnPoolSize), option.WithTokenSource(tokenSource))
	if err != nil {
		return err
	}

	startTime := time.Now()
	var callOptions []gax.CallOption
	storageLayout, err := ctrlClient.GetStorageLayout(context.Background(), &controlpb.GetStorageLayoutRequest{
		Name:      fmt.Sprintf("projects/_/buckets/%s/storageLayout", *bucketName),
		Prefix:    "",
		RequestId: "",
	}, callOptions...)

	fmt.Println("Time taken (second) in storageLayoutFetch: ", time.Since(startTime).Seconds())
	timeCheck1 := time.Now()
	namespace := storageLayout.GetHierarchicalNamespace()
	fmt.Println("Time taken (second) in namespace fetch: ", time.Since(timeCheck1).Seconds())
	fmt.Println(namespace)

	return nil

}

// ReadObject creates reader object corresponding to workerID with the help of bucketHandle.
func ReadObject(ctx context.Context, workerID int, bucketHandle *storage.BucketHandle) (err error) {

	objectName := *objectNamePrefix + strconv.Itoa(workerID) + *objectNameSuffix

	for i := 0; i < *numOfReadCallPerWorker; i++ {
		var span trace.Span
		traceCtx, span := otel.GetTracerProvider().Tracer(tracerName).Start(ctx, "ReadObject")
		span.SetAttributes(
			attribute.KeyValue{Key: "bucket", Value: attribute.StringValue(*bucketName)},
		)
		start := time.Now()
		object := bucketHandle.Object(objectName)
		rc, err := object.NewReader(traceCtx)
		if err != nil {
			return fmt.Errorf("while creating reader object: %v", err)
		}
		firstByteTime := time.Since(start)
		stats.Record(ctx, firstByteReadLatency.M(float64(firstByteTime.Milliseconds())))

		// Calls Reader.WriteTo implicitly.
		_, err = io.Copy(io.Discard, rc)
		if err != nil {
			return fmt.Errorf("while reading and discarding content: %v", err)
		}

		duration := time.Since(start)
		stats.Record(ctx, readLatency.M(float64(duration.Milliseconds())))

		err = rc.Close()
		span.End()
		if err != nil {
			return fmt.Errorf("while closing the reader object: %v", err)
		}
	}

	return
}

func main() {
	flag.Parse()
	ctx := context.Background()

	if *enableTracing {
		cleanup := enableTraceExport(ctx, *traceSampleRate)
		defer cleanup()
	}

	// Start a pprof server.
	// Example usage (run the following command while the script is running):
	// go tool pprof http://localhost:8080/debug/pprof/profile?seconds=60
	if *enablePprof {
		go func() {
			if err := http.ListenAndServe("localhost:8080", nil); err != nil {
				log.Fatalf("error starting http server for pprof: %v", err)
			}
		}()
	}

	if *enableCloudProfiler {
		if err := profiler.Start(profiler.Config{
			Service:              "custom-go-benchmark",
			ServiceVersion:       *version,
			ProjectID:            *projectID,
			NoCPUProfiling:       !*enableCPU,
			NoHeapProfiling:      !*enableHeap,
			NoAllocProfiling:     !*enableHeapAlloc,
			NoGoroutineProfiling: !*enableThread,
			MutexProfiling:       *enableContention,
			DebugLogging:         true,
		}); err != nil {
			log.Fatalf("Failed to start profiler: %v", err)
		}
	}

	var client *storage.Client
	var err error
	if *clientProtocol == "http" {
		client, err = CreateHTTPClient(ctx, false)
	} else if *clientProtocol == "grpc" {
		client, err = CreateGrpcClient(ctx)
	} else {
		err := CreateAndPerformControlClientOperation(ctx)
		if err != nil {
			fmt.Printf("while creating the client: %v", err)
			os.Exit(1)	
		}
		os.Exit(0)
	}

	if err != nil {
		fmt.Printf("while creating the client: %v", err)
		os.Exit(1)
	}

	client.SetRetry(
		storage.WithBackoff(gax.Backoff{
			Max:        maxRetryDuration,
			Multiplier: retryMultiplier,
		}),
		storage.WithPolicy(storage.RetryAlways))

	// assumes bucket already exist
	bucketHandle := client.Bucket(*bucketName)

	// Enable stack-driver exporter.
	registerLatencyView()
	registerFirstByteLatencyView()

	err = enableSDExporter()
	if err != nil {
		fmt.Printf("while enabling stackdriver exporter: %v", err)
		os.Exit(1)
	}
	defer closeSDExporter()

	// Run the actual workload
	for i := 0; i < *numOfWorker; i++ {
		idx := i
		eG.Go(func() error {
			err = ReadObject(ctx, idx, bucketHandle)
			if err != nil {
				err = fmt.Errorf("while reading object %v: %w", *objectNamePrefix+strconv.Itoa(idx)+*objectNameSuffix, err)
				return err
			}
			return err
		})
	}

	err = eG.Wait()

	if err == nil {
		fmt.Println("Read benchmark completed successfully!")
	} else {
		fmt.Fprintf(os.Stderr, "Error while running benchmark: %v", err)
		os.Exit(1)
	}
}
