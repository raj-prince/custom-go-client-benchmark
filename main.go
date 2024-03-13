package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"time"

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

	// Register the pprof endpoints under the web server root at /debug/pprof
	_ "net/http/pprof"
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

	BucketName = flag.String("bucket", "golang-grpc-test-sdk-gcs-fuse-us-east", "GCS bucket name.")

	ProjectName = flag.String("project", "storage-sdk-prober-project", "GCP project name.")

	clientProtocol = flag.String("client-protocol", "http", "Network protocol.")

	ObjectNameSuffix = ""

	tracerName      = "princer-storage-benchmark"
	enableTracing   = flag.Bool("enable-tracing", false, "Enable tracing with Cloud Trace export")
	traceSampleRate = flag.Float64("trace-sample-rate", 1.0, "Sampling rate for Cloud Trace")

	enablePprof      = flag.Bool("enable-pprof", false, "Enable pprof server")
	recordGoroutines = flag.Bool("check-goroutines", false, "Print debug info")

	eG errgroup.Group

	// ObjectNamePrefix<worker_id>ObjectNameSuffix is the object name format.
	// Here, worker id goes from <0 to NumberOfWorker>.
	objectSize = flag.String("size", "100M", "Object size portion of prefix")

	noPresetBuffer   = flag.Bool("no-preset-buffer", false, "Do not use a specific buffer; use io.Copy insted of io.CopyBuffer")
	numObjsBetweenGC = flag.Int("gc-objs", 0, "Number of objects to read between calls to force garbage collection. If <1, no GC is forced")
	memFile          = flag.String("mem-file", "memstats", "File to output memory stats into")
)

var ObjectNamePrefix string

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

func CreateGrpcCloudpathClient(ctx context.Context) (*storage.Client, error) {
	return storage.NewGRPCClient(ctx, option.WithGRPCConnectionPool(GrpcConnPoolSize))
}

// for each worker
func ReadObject(ctx context.Context, workerId int, bucketHandle *storage.BucketHandle) (err error) {

	objectName := ObjectNamePrefix + strconv.Itoa(workerId) + ObjectNameSuffix

	// gRPC server contains 2 MB of data, hence increasing the buf size to 2 MB,
	// if we don't specify io.Copy uses 32 KB as buffer size.
	buf := make([]byte, 2*MB)

	var startMem *runtime.MemStats = &runtime.MemStats{}
	var endMem *runtime.MemStats = &runtime.MemStats{}

	var f *os.File
	if *numObjsBetweenGC > 0 {
		f, err = os.Create(*memFile)
		if err != nil {
			return err
		}
	}

	for i := 0; i < *NumOfReadCallPerWorker; i++ {
		var span trace.Span
		traceCtx, span := otel.GetTracerProvider().Tracer(tracerName).Start(ctx, "ReadObject")
		span.SetAttributes(
			attribute.KeyValue{"bucket", attribute.StringValue(*BucketName)},
		)

		// Capture mem stats every numObjsBetweenGC objects
		if *numObjsBetweenGC > 0 && i%*numObjsBetweenGC == 0 {
			runtime.ReadMemStats(endMem)

			if i > 0 {
				percentChangeHeap := (float64(endMem.HeapAlloc) - float64(startMem.HeapAlloc)) / float64(startMem.HeapAlloc) * 100.0
				percentChangeTotal := (float64(endMem.TotalAlloc) - float64(startMem.TotalAlloc)) / float64(startMem.TotalAlloc) * 100.0

				fmt.Fprintf(f, "============After %d reads============\n", i)
				fmt.Fprintf(f, "\t\t\t\t\t\t\t(after last forced GC --> currently allocated)\n")

				fmt.Fprintf(f, "Allocated heap objects:\t\t%s --> %s (%.2f%% change)\n",
					formatBytes(int64(startMem.HeapAlloc)), formatBytes(int64(endMem.HeapAlloc)), percentChangeHeap)
				fmt.Fprintf(f, "Total cumulative allocated:\t%s --> %s (%.2f%% change)\n",
					formatBytes(int64(startMem.TotalAlloc)), formatBytes(int64(endMem.TotalAlloc)), percentChangeTotal)
			}

			runtime.GC()
			runtime.ReadMemStats(startMem)
		}

		start := time.Now()
		object := bucketHandle.Object(objectName).Retryer(storage.WithPolicy(storage.RetryNever))
		rc, err := object.NewReader(traceCtx)
		if err != nil {
			fmt.Printf("while creating reader object: %v\n", err)
			return fmt.Errorf("while creating reader object: %v", err)
		}

		if *noPresetBuffer {
			_, err = io.Copy(io.Discard, rc)
		} else {
			_, err = io.CopyBuffer(io.Discard, rc, buf)
		}
		if err != nil {
			fmt.Printf("while reading and discarding content: %v\n", err)
			return fmt.Errorf("while reading and discarding content: %v", err)
		}

		duration := time.Since(start)
		stats.Record(ctx, readLatency.M(float64(duration.Milliseconds())))

		err = rc.Close()
		span.End()
		if err != nil {
			fmt.Printf("while closing the reader object: %v\n", err)
			return fmt.Errorf("while closing the reader object: %v", err)
		}
	}

	return
}

func main() {
	flag.Parse()
	ctx := context.Background()

	ObjectNamePrefix = fmt.Sprintf("princer_%s_files/file_", *objectSize)

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

	if *recordGoroutines {
		recordRunningRoutines()
	}

	var client *storage.Client
	var err error
	if *clientProtocol == "http" {
		client, err = CreateHttpClient(ctx, false)
	} else if *clientProtocol == "cp" {
		client, err = CreateGrpcCloudpathClient(ctx)
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
		storage.WithPolicy(storage.RetryAlways))

	// assumes bucket already exist
	bucketHandle := client.Bucket(*BucketName)

	// Enable stack-driver exporter.
	registerLatencyView()

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
				err = fmt.Errorf("while reading object %v: %w", ObjectNamePrefix+strconv.Itoa(idx), err)
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

func recordRunningRoutines() {
	fmt.Printf("runtime.GOMAXPROCS: %d\n", runtime.GOMAXPROCS(0))
	fmt.Printf("runtime.NumCPU: %d\n", runtime.NumCPU())

	go func() {
		for {
			time.Sleep(time.Second * 5)
			fmt.Printf("Number of Running goroutines: %d\n", runtime.NumGoroutine())
		}
	}()
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}
