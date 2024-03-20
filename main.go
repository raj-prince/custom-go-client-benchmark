package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"

	// Install google-c2p resolver, which is required for direct path.
	_ "google.golang.org/grpc/balancer/rls"
	_ "google.golang.org/grpc/xds/googledirectpath"
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
	ObjectNamePrefix = "princer_100M_files/file_"
	ObjectNameSuffix = ""

	tracerName      = "princer-storage-benchmark"
	enableTracing   = flag.Bool("enable-tracing", false, "Enable tracing with Cloud Trace export")
	traceSampleRate = flag.Float64("trace-sample-rate", 1.0, "Sampling rate for Cloud Trace")

	eG errgroup.Group
)

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

func main() {
	flag.Parse()
	ctx := context.Background()

	var client *storage.Client
	var err error
	client, err = CreateGrpcClient(ctx)

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
	err = client.Bucket(*BucketName).Create(context.Background(), "retry-test", &storage.BucketAttrs{Name: *BucketName})
	bucketHandle := client.Bucket(*BucketName)
	//ctx = callctx.SetHeaders(ctx, "x-retry-test-id", "89f4a47969ca4ae5bdfa7a633d3464b3")
	objHandle := bucketHandle.Object("prince")
	wr := objHandle.NewWriter(ctx)
	wr.Write([]byte("content"))

	err1 := wr.Close()
	fmt.Println(err1)
}
