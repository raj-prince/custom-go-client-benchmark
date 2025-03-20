package main

import (
	"context"
	"crypto/tls"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/profiler"
	"cloud.google.com/go/storage"
	"cloud.google.com/go/storage/experimental"
	"github.com/googleapis/gax-go/v2"
	"golang.org/x/oauth2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	// Register the pprof endpoints under the web server root at /debug/pprof
	_ "net/http/pprof"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	GrpcConnPoolSize     = 1
	MaxConnsPerHost      = 100
	MaxIdleConnsPerHost  = 100
	NumOfWorker          = flag.Int("worker", 32, "Number of concurrent workers to read")
	MaxRetryDuration     = 30 * time.Second
	RetryMultiplier      = 2.0
	BucketName           = flag.String("bucket", "vipin-us-central1", "GCS bucket name.")
	bucketDir            = flag.String("bucket-dir", "1B", "Directory in the bucket where files are stored.")
	ProjectName          = flag.String("project", "gcs-fuse-test", "GCP project name.")
	clientProtocol       = flag.String("client-protocol", "http", "Network protocol.")
	tracerName           = "vipinydv-read-tail-latency-investigation"
	enableTracing        = flag.Bool("enable-tracing", false, "Enable tracing with Cloud Trace export")
	enableCloudProfiler  = flag.Bool("enable-cloud-profiler", false, "Enable cloud profiler")
	enablePprof          = flag.Bool("enable-pprof", false, "Enable pprof server")
	traceSampleRate      = flag.Float64("trace-sample-rate", 1.0, "Sampling rate for Cloud Trace")
	enableHeap           = flag.Bool("heap", false, "enable heap profile collection")
	enableHeapAlloc      = flag.Bool("heap_alloc", false, "enable heap allocation profile collection")
	enableThread         = flag.Bool("thread", false, "enable thread profile collection")
	enableContention     = flag.Bool("contention", false, "enable contention profile collection")
	minDelay             = flag.Duration("min-delay", 1500*time.Millisecond, "min delay")
	projectID            = flag.String("project_id", "", "project ID to run profiler with; only required when running outside of GCP.")
	version              = flag.String("version", "original", "version to run profiler with")
	withReadStallTimeout = flag.Bool("with-read-stall-timeout", true, "Enable read stall timeout")
	targetPercentile     = flag.Float64("target-percentile", 0.999, "Target percentile for read dynamic timeout")
	outputBucketPath     = flag.String("output-bucket-path", "vipin-metrics/go-sdk/", "GCS bucket path to store the output CSV file")
	totalFilesToRead     = flag.Int("total-files-to-read", -1, "Number of files to read. If not set, all files in the bucket will be read.")
	eG                   errgroup.Group
)

const dynamicReadReqInitialTimeoutEnv = "DYNAMIC_READ_REQ_INITIAL_TIMEOUT"

func CreateHttpClient(ctx context.Context, isHttp2 bool) (client *storage.Client, err error) {
	var transport *http.Transport
	if !isHttp2 {
		transport = &http.Transport{
			MaxConnsPerHost:     MaxConnsPerHost,
			MaxIdleConnsPerHost: MaxIdleConnsPerHost,
			TLSNextProto:        make(map[string]func(string, *tls.Conn) http.RoundTripper),
		}
	} else {
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

	httpClient := &http.Client{
		Transport: &oauth2.Transport{
			Base:   transport,
			Source: tokenSource,
		},
		Timeout: 0,
	}

	httpClient.Transport = &userAgentRoundTripper{
		wrapped:   httpClient.Transport,
		UserAgent: "vipin",
	}

	clientOpts := []option.ClientOption{
		option.WithHTTPClient(httpClient),
	}

	if *withReadStallTimeout {
		// Hidden way to modify the initial-timeout of the dynamic delay algorithm in go-sdk.
		// Ref: https://github.com/googleapis/google-cloud-go/blob/main/storage/option.go#L62
		// Temporarily we kept an option to change the initial-timeout, will be removed
		// once we get a good default.
		err = os.Setenv(dynamicReadReqInitialTimeoutEnv, "1500ms")
		if err != nil {
			log.Printf("Error while setting the env %s: %v", dynamicReadReqInitialTimeoutEnv, err)
		}
		clientOpts = append(clientOpts, experimental.WithReadStallTimeout(&experimental.ReadStallTimeoutConfig{
			Min:              *minDelay,
			TargetPercentile: *targetPercentile,
		}))
	}

	return storage.NewClient(ctx, clientOpts...)
}

// CreateGrpcClient sets up a gRPC client for GCS using the default credentials.
func CreateGrpcClient(ctx context.Context) (*storage.Client, error) {
	clientOpts := []option.ClientOption{
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}
	return storage.NewClient(ctx, clientOpts...)
}

func getObjectNames(ctx context.Context, bucketHandle *storage.BucketHandle) ([]string, error) {
	var objectNames []string

	// If bucketDir is set, prepend it to the object names while listing
	prefix := *bucketDir
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	it := bucketHandle.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		objAttrs, err := it.Next()
		if err == iterator.Done || len(objectNames) >= *totalFilesToRead {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error listing objects: %v", err)
		}
		objectNames = append(objectNames, objAttrs.Name)
	}

	// Return the list of object names
	return objectNames, nil
}

func ReadObject(ctx context.Context, objectName string, bucketHandle *storage.BucketHandle) ([]string, error) {
	var span trace.Span
	traceCtx, span := otel.GetTracerProvider().Tracer(tracerName).Start(ctx, "ReadObject")
	defer span.End()

	span.SetAttributes(
		attribute.KeyValue{Key: "bucket", Value: attribute.StringValue(*BucketName)},
	)
	startTime := time.Now()
	object := bucketHandle.Object(objectName)
	log.Printf("Creating NewReader for object(%s)", objectName)
	rc, err := object.NewReader(traceCtx)
	log.Printf("NewReader for object(%s) created after %fms", objectName, float64(time.Since(startTime).Milliseconds()))
	if err != nil {
		return nil, fmt.Errorf("while creating reader object %s: %v", objectName, err)
	}
	ttfbTime := time.Since(startTime)

	_, err = io.Copy(io.Discard, rc)
	if err != nil {
		return nil, fmt.Errorf("while reading and discarding content from %s: %v", objectName, err)
	}
	duration := time.Since(startTime)

	err = rc.Close()
	log.Printf("NewReader for object(%s) closed after %fms", objectName, float64(time.Since(startTime).Milliseconds()))
	if err != nil {
		return nil, fmt.Errorf("while closing the reader object %s: %v", objectName, err)
	}

	record := []string{
		fmt.Sprintf("%f", float64(startTime.UnixNano())/1e9),
		fmt.Sprintf("%f", float64(ttfbTime.Nanoseconds())/1e6),
		fmt.Sprintf("%f", float64(duration.Nanoseconds())/1e6),
		objectName,
	}

	return record, nil
}

func ReadObjects(ctx context.Context, start int, end int, bucketHandle *storage.BucketHandle, objectNames []string) ([][]string, error) {
	records := [][]string{}
	objectCount := len(objectNames)

	for i := start; i < end; i++ {
		objectIndex := i % objectCount // Loop back to the beginning if needed
		objectName := objectNames[objectIndex]
		record, err := ReadObject(ctx, objectName, bucketHandle)
		if err != nil {
			return nil, fmt.Errorf("while reading object %s: %v", objectName, err)
		}
		records = append(records, record)
	}

	return records, nil
}

func makeCSV(records [][]string) (string, error) {
	var b strings.Builder
	w := csv.NewWriter(&b)

	// Write the header line
	header := []string{"Timestamp", "First Byte Latency", "Overall Latency", "Object Name"}
	if err := w.Write(header); err != nil {
		return "", fmt.Errorf("error writing header to csv: %v", err)
	}

	// Write the data records
	for _, record := range records {
		if err := w.Write(record); err != nil {
			return "", fmt.Errorf("error writing record to csv: %v", err)
		}
	}

	// Flush the writer to ensure everything is written to the string builder
	w.Flush()
	if err := w.Error(); err != nil {
		return "", fmt.Errorf("error flushing csv writer: %v", err)
	}

	return b.String(), nil
}

// ParseBucketAndObjectFromUri parses a GCS URI into a bucket name and object path.
// Example input: gs://bucket-name/path/to/file.txt
func ParseBucketAndObjectFromUri(uri string) (string, string, error) {

	uri = strings.TrimPrefix(uri, "gs://")
	// Split the URI into bucket name and object path
	parts := strings.SplitN(uri, "/", 2)
	if len(parts) != 2 {
		return "", "", errors.New("invalid GCS URI, expected format gs://bucket-name/object-path")
	}

	bucketName := parts[0]
	objectPath := parts[1]

	// Check if the bucket name is valid (not empty)
	if bucketName == "" {
		return "", "", errors.New("bucket name cannot be empty")
	}

	// Return the bucket name and object path
	return bucketName, objectPath, nil
}

func writeCSVToGCS(ctx context.Context, csvData string, bucketPath string) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
		return
	}
	defer client.Close()

	bucketName, objectPath, err := ParseBucketAndObjectFromUri(bucketPath)
	if err != nil {
		log.Fatalf("Failed to parse output bucket path %v", err)
		return
	}

	// Check if the objectPath ends with "/" (folder)
	if strings.HasSuffix(objectPath, "/") {
		objectPath = objectPath + time.Now().UTC().Format(time.RFC3339) + ".csv"
	}

	wc := client.Bucket(bucketName).Object(objectPath).NewWriter(ctx)

	_, err = wc.Write([]byte(csvData))
	if err != nil {
		log.Fatalf("Failed to write to object: %v", err)
		return
	}

	if err := wc.Close(); err != nil {
		log.Fatalf("Failed to close writer: %v", err)
		return
	}
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	flag.Parse()
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
		log.Fatalf("while creating the client: %v", err)
	}
	client.SetRetry(
		storage.WithBackoff(gax.Backoff{
			Max:        MaxRetryDuration,
			Multiplier: RetryMultiplier,
		}),
		storage.WithPolicy(storage.RetryAlways),
		storage.WithErrorFunc(ShouldRetry),
	)

	if strings.HasPrefix(*BucketName, "gs://") {
		*BucketName = strings.TrimPrefix(*BucketName, "gs://")
	}
	bucketHandle := client.Bucket(*BucketName)

	objectNames, err := getObjectNames(ctx, bucketHandle)
	if err != nil {
		log.Fatalf("Failed to list objects: %v", err)
	}

	// Setup goroutines for parallel processing based on worker count
	recordsChannel := make(chan [][]string, *NumOfWorker)

	if *totalFilesToRead == -1 {
		*totalFilesToRead = len(objectNames)
	}

	// Split the work for goroutines
	filesPerWorker := *totalFilesToRead / *NumOfWorker
	remainder := *totalFilesToRead % *NumOfWorker

	for i := range *NumOfWorker {
		start := i * filesPerWorker
		var end int

		if i < remainder {
			start += i
			end = start + filesPerWorker + 1
		} else {
			start += remainder
			end = start + filesPerWorker
		}
		eG.Go(func() error {
			records, err := ReadObjects(ctx, start, end, bucketHandle, objectNames)
			if err != nil {
				return fmt.Errorf("error reading objects in range %d-%d: %v", start, end, err)
			}
			recordsChannel <- records
			return nil
		})
	}

	// Wait for all workers to finish
	if err := eG.Wait(); err != nil {
		log.Fatalf("Error in worker goroutines: %v", err)
	}

	// Close the records channel and gather all records
	var allRecords [][]string
	close(recordsChannel)
	for records := range recordsChannel {
		allRecords = append(allRecords, records...)
	}

	csvData, err := makeCSV(allRecords)
	if err != nil {
		log.Fatalf("Failed to create CSV: %v", err)
	}

	writeCSVToGCS(ctx, csvData, *outputBucketPath)
}

func ShouldRetry(err error) (b bool) {
	b = storage.ShouldRetry(err)
	if b {
		log.Printf("Retrying for the error: %v", err)
	}
	return
}
