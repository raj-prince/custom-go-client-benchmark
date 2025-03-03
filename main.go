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
	"strconv"
	"time"

	"cloud.google.com/go/storage"
	"cloud.google.com/go/storage/experimental"
	"github.com/googleapis/gax-go/v2"
	"golang.org/x/oauth2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"

	"strings"

	"cloud.google.com/go/profiler"
)

var (
	GrpcConnPoolSize     = 1
	MaxConnsPerHost      = 100
	MaxIdleConnsPerHost  = 100
	NumOfWorker          = flag.Int("worker", 32, "Number of concurrent worker to read") // Set to 32
	MaxRetryDuration     = 30 * time.Second
	RetryMultiplier      = 2.0
	BucketName           = flag.String("bucket", "vipin-us-central1", "GCS bucket name.")
	ProjectName          = flag.String("project", "gcs-fuse-test", "GCP project name.")
	clientProtocol       = flag.String("client-protocol", "http", "Network protocol.")
	enableCloudProfiler  = flag.Bool("enable-cloud-profiler", false, "Enable cloud profiler")
	enablePprof          = flag.Bool("enable-pprof", false, "Enable pprof server")
	enableHeap           = flag.Bool("heap", false, "enable heap profile collection")
	enableHeapAlloc      = flag.Bool("heap_alloc", false, "enable heap allocation profile collection")
	enableThread         = flag.Bool("thread", false, "enable thread profile collection")
	enableContention     = flag.Bool("contention", false, "enable contention profile collection")
	minDelay             = flag.Duration("min-delay", 500*time.Millisecond, "min delay")
	projectID            = flag.String("project_id", "", "project ID to run profiler with; only required when running outside of GCP.")
	version              = flag.String("version", "original", "version to run profiler with")
	withReadStallTimeout = flag.Bool("with-read-stall-timeout", true, "Enable read stall timeout")
	targetPercentile     = flag.Float64("target-percentile", 0.999, "Target percentile for read dynamic timeout")
	outputBucketPath     = flag.String("output-bucket-path", "gs://vipin-metrics/go-sdk", "GCS bucket path to store the output CSV file")
	totalFilesToRead     = flag.Int("total-files-to-read", 0, "Number of files to read. If not set, all files in the bucket will be read.")
	eG                   errgroup.Group
)

func CreateHttpClient(ctx context.Context, isHttp2 bool) (client *storage.Client, err error) {
	var transport *http.Transport
	if !isHttp2 {
		transport = &http.Transport{
			MaxConnsPerHost:     MaxConnsPerHost,
			MaxIdleConnsPerHost: MaxIdleConnsPerHost,
			TLSNextProto: make(
				map[string]func(string, *tls.Conn) http.RoundTripper,
			),
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
		clientOpts = append(clientOpts, experimental.WithReadStallTimeout(&experimental.ReadStallTimeoutConfig{
			Min:              *minDelay,
			TargetPercentile: *targetPercentile,
		}))
	}

	return storage.NewClient(ctx, clientOpts...)
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

func getObjectNames(ctx context.Context, bucketHandle *storage.BucketHandle) ([]string, error) {
	var objectNames []string
	it := bucketHandle.Objects(ctx, nil)
	for {
		objAttrs, err := it.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error listing objects: %v", err)
		}
		objectNames = append(objectNames, objAttrs.Name)
	}
	return objectNames, nil
}

func ReadObject(ctx context.Context, start int, count int, bucketHandle *storage.BucketHandle, objectNames []string) ([][]string, error) {
	records := [][]string{}

	for i := start; i < start+count; i++ {
		objectName := objectNames[i]
		startTime := time.Now()
		object := bucketHandle.Object(objectName)
		rc, err := object.NewReader(ctx)
		if err != nil {
			return nil, fmt.Errorf("while creating reader object %s: %v", objectName, err)
		}

		ttfbTime := time.Since(startTime)

		_, err = io.Copy(io.Discard, rc)
		if err != nil {
			_ = rc.Close()
			return nil, fmt.Errorf("while reading and discarding content from %s: %v", objectName, err)
		}

		duration := time.Since(startTime)
		record := []string{
			strconv.FormatInt(startTime.UnixNano(), 10),
			fmt.Sprintf("%f", ttfbTime.Seconds()),
			fmt.Sprintf("%f", duration.Seconds()),
		}
		records = append(records, record)

		err = rc.Close()
		if err != nil {
			return nil, fmt.Errorf("while closing the reader object %s: %v", objectName, err)
		}
	}

	return records, nil
}

func makeCSV(records [][]string) string {
	var b strings.Builder
	w := csv.NewWriter(&b)
	for _, record := range records {
		if err := w.Write(record); err != nil {
			fmt.Println("error writing record to csv:", err)
			return "" // or handle the error as needed
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		fmt.Println("error flushing csv writer:", err)
		return ""
	}
	return b.String()
}

// ParseBucketAndObjectFromUri parses a GCS URI into a bucket name and object path.
// Example input: gs://bucket-name/path/to/file.txt
func ParseBucketAndObjectFromUri(uri string) (string, string, error) {
	if !strings.HasPrefix(uri, "gs://") {
		return "", "", errors.New("invalid GCS URI, must start with 'gs://'")
	}

	// Remove the "gs://" prefix
	uri = uri[5:]

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

	objectPath = objectPath + time.Now().UTC().Format(time.RFC3339) + ".csv"

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
	flag.Parse()
	ctx := context.Background()

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
	if *withReadStallTimeout {
		client.SetRetry(
			storage.WithBackoff(gax.Backoff{
				Max:        MaxRetryDuration,
				Multiplier: RetryMultiplier,
			}),
			storage.WithPolicy(storage.RetryAlways),
		)
	}

	// assumes bucket already exists
	bucketHandle := client.Bucket(*BucketName)

	var objectNames []string
	if *totalFilesToRead > 0 {
		objectNames, err = getObjectNames(ctx, bucketHandle)
		if err != nil {
			log.Fatalf("Failed to list objects: %v", err)
		}
		if len(objectNames) > *totalFilesToRead {
			objectNames = objectNames[:*totalFilesToRead]
		}
	} else {
		objectNames, err = getObjectNames(ctx, bucketHandle)
		if err != nil {
			log.Fatalf("Failed to list objects: %v", err)
		}
	}

	allRecords := [][]string{
		{"Timestamp", "FirstByteLatency", "OverallLatency"},
	}

	filesPerWorker := (len(objectNames) + *NumOfWorker - 1) / *NumOfWorker

	// Run the actual workload
	for i := 0; i < *NumOfWorker; i++ {
		start := i * filesPerWorker
		count := filesPerWorker
		if start+count > len(objectNames) {
			count = len(objectNames) - start
		}

		eG.Go(func() error {
			records, err := ReadObject(ctx, start, count, bucketHandle, objectNames)
			if err != nil {
				return err
			}
			allRecords = append(allRecords, records...)
			return nil
		})
	}

	err = eG.Wait()

	if err != nil {
		log.Fatalf("Error while running benchmark: %v", err)
	}

	// Write to in-memory CSV
	csvData := makeCSV(allRecords)

	// Write CSV to GCS bucket
	writeCSVToGCS(ctx, csvData, *outputBucketPath)

	fmt.Println("Read benchmark completed successfully!")
	fmt.Printf("Results written to: %s\n", *outputBucketPath)
}
