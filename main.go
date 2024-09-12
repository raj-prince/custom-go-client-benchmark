package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	// Register the pprof endpoints under the web server root at /debug/pprof
	_ "net/http/pprof"
	"os"
	"strconv"
	"time"

	"github.com/raj-prince/custom-go-client-benchmark/util"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"go.opencensus.io/stats"
	"golang.org/x/oauth2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"

	// Install google-c2p resolver, which is required for direct path.
	_ "google.golang.org/grpc/balancer/rls"
	_ "google.golang.org/grpc/xds/googledirectpath"

	"cloud.google.com/go/profiler"
)

var (
	GrpcConnPoolSize    = 1
	MaxConnsPerHost     = 100
	MaxIdleConnsPerHost = 100

	MB = 1024 * 1024

	NumOfWorker = flag.Int("worker", 48, "Number of concurrent worker to read")

	NumOfReadCallPerWorker = flag.Int("read-call-per-worker", 1000000, "Number of read call per worker")

	MaxRetryDuration = 30 * time.Second

	RetryMultiplier = 2.0

	BucketName = flag.String("bucket", "princer-working-dirs", "GCS bucket name.")

	ProjectName = flag.String("project", "gcs-fuse-test", "GCP project name.")

	clientProtocol = flag.String("client-protocol", "http", "Network protocol.")

	// ObjectNamePrefix<worker_id>ObjectNameSuffix is the object name format.
	// Here, worker id goes from <0 to NumberOfWorker>.
	ObjectNamePrefix = flag.String("object-prefix", "princer_100M_files/file_", "Object prefix")
	ObjectNameSuffix = flag.String("object-suffix", "", "Object suffix")

	tracerName      = "princer-storage-benchmark"
	enableTracing   = flag.Bool("enable-tracing", false, "Enable tracing with Cloud Trace export")
	enableCloudProfiler   = flag.Bool("enable-cloud-profiler", false, "Enable cloud profiler")
	enablePprof     = flag.Bool("enable-pprof", false, "Enable pprof server")
	traceSampleRate = flag.Float64("trace-sample-rate", 1.0, "Sampling rate for Cloud Trace")
	enableHeap       = flag.Bool("heap", false, "enable heap profile collection")
	enableHeapAlloc  = flag.Bool("heap_alloc", false, "enable heap allocation profile collection")
	enableThread     = flag.Bool("thread", false, "enable thread profile collection")
	enableContention = flag.Bool("contention", false, "enable contention profile collection")
	isRangeRead = flag.Bool("read-range", false, "Is range read")
	rangeLength = flag.Int64("range-len", int64(8 * MB), "Range read size in MB")
	fileSize = flag.Int64("file-size", int64(1024 * MB), "File size in MB")
	projectID        = flag.String("project_id", "", "project ID to run profiler with; only required when running outside of GCP.")
	version          = flag.String("version", "original", "version to run profiler with")
	eG errgroup.Group
)

var gPattern []int64
var gReqLatency *util.Result
var gReadLatency *util.Result
func init() {
	gReqLatency = &util.Result{Name: "ReqLatency"}
	gReadLatency = &util.Result{Name: "ReadLatency"}
}

func CreateHttpClient(ctx context.Context, isHttp2 bool) (client *storage.Client, err error) {
	var transport *http.Transport
	// Using http1 makes the client more performant.
	if isHttp2 == false {
		transport = &http.Transport{
			MaxConnsPerHost:     MaxConnsPerHost,
			MaxIdleConnsPerHost: MaxIdleConnsPerHost,
			// This disables HTTP/2 in transport.
			TLSNextProto: make(
				map[string]func(string, *tls.Conn) http.RoundTripper,
			),
		}
	} else {
		// For http2, change in MaxConnsPerHost doesn't affect the performance.
		transport = &http.Transport{
			DisableKeepAlives: true,
			MaxConnsPerHost:   MaxConnsPerHost,
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

	return storage.NewClient(ctx, option.WithHTTPClient(httpClient))
}

func CreateGrpcClient(ctx context.Context) (client *storage.Client, err error) {
	if err := os.Setenv("GOOGLE_CLOUD_ENABLE_DIRECT_PATH_XDS", "true"); err != nil {
		log.Fatalf("error setting direct path env var: %v", err)
	}

	client, err = storage.NewGRPCClient(ctx, option.WithGRPCConnectionPool(GrpcConnPoolSize))

	if err := os.Unsetenv("GOOGLE_CLOUD_ENABLE_DIRECT_PATH_XDS"); err != nil {
		log.Fatalf("error while unsetting direct path env var: %v", err)
	}
	return
}

func RangeReadHelper(ctx context.Context, objectName string, bh *storage.BucketHandle, offset, len int64) error {
	var span trace.Span
	traceCtx, span := otel.GetTracerProvider().Tracer(tracerName).Start(ctx, "ReadObject")
	span.SetAttributes(
		attribute.KeyValue{Key: "bucket", Value: attribute.StringValue(*BucketName)},
	)
	start := time.Now()
	object := bh.Object(objectName)
	rc, err := object.NewRangeReader(traceCtx, offset, len)
	if err != nil {
		return fmt.Errorf("while creating reader object: %v", err)
	}

	ttfbTime := time.Since(start)
	gReqLatency.Append(ttfbTime.Seconds(), 0)
	stats.Record(ctx, ttfbReadLatency.M(float64(ttfbTime.Milliseconds())))

	// Calls Reader.WriteTo implicitly.
	_, err = io.Copy(io.Discard, rc)
	if err != nil {
		return fmt.Errorf("while reading and discarding content: %v", err)
	}

	duration := time.Since(start)
	stats.Record(ctx, readLatency.M(float64(duration.Milliseconds())))
	gReadLatency.Append(duration.Seconds(), 0)
	err = rc.Close()
	span.End()
	if err != nil {
		return fmt.Errorf("while closing the reader object: %v", err)
	}

	return nil
}

func getRandReadPattern() []int64 {
	numOfRanges := int64(math.Ceil(float64((int64(*fileSize)) / (int64(*rangeLength)))))
	pattern := make([]int64, numOfRanges)
	indices := make([]int64, numOfRanges)
	for i := int64(0); i < numOfRanges; i++ {
		indices[int(i)] = i
	}
	for i := int64(0); i < numOfRanges; i++ {
		randNum := rand.Intn(len(indices))
		pattern[i] = indices[randNum] * (*rangeLength)
		indices = append(indices[:randNum], indices[randNum+1:]...)
		fmt.Println(pattern[i])
	}
	return pattern
}

func ReadObject(ctx context.Context, workerId int, bucketHandle *storage.BucketHandle) (err error) {

	objectName := *ObjectNamePrefix + strconv.Itoa(workerId) + *ObjectNameSuffix

	for i := 0; i < *NumOfReadCallPerWorker; i++ {
		if !*isRangeRead {
			err = RangeReadHelper(ctx, objectName, bucketHandle, 0, -1)
			if err != nil {
				return fmt.Errorf("while reading object: %v", err)
			}
		} else {
			for p := 0; p < len(gPattern); p++ {
				offset := gPattern[p]
				err = RangeReadHelper(ctx, objectName, bucketHandle, offset, *rangeLength)
				if err != nil {
					return fmt.Errorf("while reading object: %v", err)
				}
			}
		}
	}

	return
}

func main() {
	flag.Parse()
	gPattern = getRandReadPattern()
	for i, p := range gPattern {
		fmt.Println(i)
		fmt.Println(p)
	}
	ctx := context.Background()

	if *enableTracing {
		cleanup := enableTraceExport(ctx, *traceSampleRate)
		defer cleanup()
	}

	if *enableCloudProfiler {
		if err := profiler.Start(profiler.Config{
			Service:              "custom-go-benchmark",
			ServiceVersion:       *version,
			ProjectID:            *projectID,
			NoHeapProfiling:      !*enableHeap,
			NoAllocProfiling:     !*enableHeapAlloc,
			NoGoroutineProfiling: !*enableThread,
			MutexProfiling:       *enableContention,
			DebugLogging:         true,
		}); err != nil {
			log.Fatalf("Failed to start profiler: %v", err)
		}
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

	var client *storage.Client
	var err error
	if *clientProtocol == "http" {
		client, err = CreateHttpClient(ctx, false)
	} else {
		client, err = CreateGrpcClient(ctx)
	}

	if err != nil {
		fmt.Errorf("while creating the client: %v", err)
	}

	client.SetRetry(
		storage.WithBackoff(gax.Backoff{
			Max:        MaxRetryDuration,
			Multiplier: RetryMultiplier,
		}),
		storage.WithPolicy(storage.RetryAlways),
		//storage.WithReadDynamicTimeout(0.99, 15, 80*time.Millisecond, 50*time.Millisecond, 2*time.Minute),
		//storage.WithMinReadThroughput(1024, 1 * time.Second),
		)

	// assumes bucket already exist
	bucketHandle := client.Bucket(*BucketName)

	// Enable stack-driver exporter.
	registerLatencyView()
	registerTTFBLatencyView()

	err = enableSDExporter()
	if err != nil {
		fmt.Fprintf(os.Stderr, "while enabling stackdriver exporter: %v", err)
		os.Exit(1)
	}
	defer closeSDExporter()

	// Run the actual workload
	for i := 0; i < *NumOfWorker; i++ {
		idx := i
		eG.Go(func() error {
			err = ReadObject(ctx, idx, bucketHandle)
			if err != nil {
				err = fmt.Errorf("while reading object %v: %w", *ObjectNamePrefix+strconv.Itoa(idx), err)
				return err
			}
			return err
		})
	}

	err = eG.Wait()

	if err == nil {
		gReqLatency.PrintStats()
		gReadLatency.PrintStats()
		err = gReqLatency.DumpMetricsCSV("req.csv")
		if err != nil {
			fmt.Println("Error while dumping req latency as csv file")
		}
		err = gReadLatency.DumpMetricsCSV("read.csv")
		if err != nil {
			fmt.Println("Error while dumping read latency as CSV")
		}
		fmt.Println("Read benchmark completed successfully!")
	} else {
		fmt.Fprintf(os.Stderr, "Error while running benchmark: %v", err)
		os.Exit(1)
	}
}
