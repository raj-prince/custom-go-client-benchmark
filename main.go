package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

var (
	GrpcConnPoolSize    = 1
	MaxConnsPerHost     = 100
	MaxIdelConnsPerHost = 100

	NumOfWorker = 48

	NumOfReadCallPerWorker = 800

	wg sync.WaitGroup
)

func CreateHttpClient(ctx context.Context, isHttp2 bool) (client *storage.Client, err error) {
	var transport *http.Transport
	// Using http1 makes the client more performant.
	if isHttp2 == false {
		transport = &http.Transport{
			MaxConnsPerHost:     MaxConnsPerHost,
			MaxIdleConnsPerHost: MaxIdelConnsPerHost,
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
	if err := os.Setenv("STORAGE_USE_GRPC", "gRPC"); err != nil {
		log.Fatalf("error setting grpc env var: %v", err)
	}

	if err := os.Setenv("GOOGLE_CLOUD_ENABLE_DIRECT_PATH_XDS", "true"); err != nil {
		log.Fatalf("error setting direct path env var: %v", err)
	}

	client, err = storage.NewClient(ctx, option.WithGRPCConnectionPool(GrpcConnPoolSize))

	if err := os.Unsetenv("STORAGE_USE_GRPC"); err != nil {
		log.Fatalf("error while unsetting grpc env var: %v", err)
	}

	if err := os.Unsetenv("GOOGLE_CLOUD_ENABLE_DIRECT_PATH_XDS"); err != nil {
		log.Fatalf("error while unsetting direct path env var: %v", err)
	}
	return
}

func ReadObject(ctx context.Context, workerId int, bucketHandle *storage.BucketHandle) (err error) {
	defer wg.Done()

	objectName := "50mb/1_thread." + strconv.Itoa(workerId) + ".0"

	for i := 0; i < NumOfReadCallPerWorker; i++ {
		start := time.Now()
		object := bucketHandle.Object(objectName)
		rc, err := object.NewReader(ctx)
		if err != nil {
			return fmt.Errorf("while creating reader object: %v", err)
		}

		duration := time.Since(start)
		fmt.Println(duration)

		rc.Close()
	}

	return
}

func main() {
	clientProtocol := flag.String("client-protocol", "http", "# of iterations")
	flag.Parse()

	ctx := context.Background()

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

	bucketHandle := client.Bucket("golang-grpc-test-princer-us-west1")
	err = bucketHandle.Create(ctx, "gcs-fuse-test", nil)

	if err != nil {
		fmt.Errorf("while creating the bucket: %v", err)
	}

	wg.Add(NumOfWorker)

	for i := 0; i < NumOfWorker; i++ {
		go ReadObject(ctx, i, bucketHandle)
	}

	wg.Wait()
}
