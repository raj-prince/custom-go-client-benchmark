package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"text/tabwriter"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
)

var (
	bucketName    = flag.String("bucket", "princer-working-dirs", "GCS bucket name.")
	objectPrefix  = flag.String("obj-prefix", "grpc_test.txt", "Object prefix.")
	objectSuffix  = flag.String("obj-suffix", "", "Object suffix.")
	numOfWorkers  = flag.Int("workers", 4, "Number of concurrent workers (threads).")
	numOfCalls    = flag.Int("calls", 50, "Number of stat calls per worker.")
	maxConns      = flag.Int("max-conns", 100, "Max connections per host for HTTP client.")
)

type Result struct {
	Name       string
	TotalOps   int
	Duration   time.Duration
	QPS        float64
	AvgLatency float64
	P50Latency float64
	P90Latency float64
	P99Latency float64
	Error      string
}

func CreateHTTP1Client(ctx context.Context) (*storage.Client, error) {
	transport := &http.Transport{
		MaxConnsPerHost:     *maxConns,
		MaxIdleConnsPerHost: *maxConns,
		TLSNextProto:        make(map[string]func(string, *tls.Conn) http.RoundTripper),
	}

	tokenSource, err := google.DefaultTokenSource(ctx, "https://www.googleapis.com/auth/devstorage.full_control")
	if err != nil {
		return nil, err
	}

	httpClient := &http.Client{
		Transport: &oauth2.Transport{
			Base:   transport,
			Source: tokenSource,
		},
		Timeout: 0,
	}

	return storage.NewClient(ctx, option.WithHTTPClient(httpClient))
}

func CreateGrpcClient(ctx context.Context, directPath bool) (*storage.Client, error) {
	if directPath {
		os.Setenv("GOOGLE_CLOUD_DISABLE_DIRECT_PATH", "false")
	} else {
		os.Setenv("GOOGLE_CLOUD_DISABLE_DIRECT_PATH", "true")
	}

	tokenSource, err := google.DefaultTokenSource(ctx, "https://www.googleapis.com/auth/devstorage.full_control")
	if err != nil {
		return nil, err
	}

	return storage.NewGRPCClient(ctx, option.WithGRPCConnectionPool(1), option.WithTokenSource(tokenSource), storage.WithDisabledClientMetrics())
}

func runBenchmark(ctx context.Context, name string, client *storage.Client) *Result {
	client.SetRetry(
		storage.WithBackoff(gax.Backoff{
			Max:        15 * time.Second,
			Multiplier: 2.0,
		}),
		storage.WithPolicy(storage.RetryAlways))

	bucketHandle := client.Bucket(*bucketName)

	var mu sync.Mutex
	var latencies []time.Duration

	var eG errgroup.Group
	benchStart := time.Now()

	for i := 0; i < *numOfWorkers; i++ {
		idx := i
		eG.Go(func() error {
			// Construct object name matching worker index.
			// E.g., prefix0, prefix1, etc.
			objectName := *objectPrefix + strconv.Itoa(idx) + *objectSuffix
			object := bucketHandle.Object(objectName)

			localLatencies := make([]time.Duration, 0, *numOfCalls)
			for j := 0; j < *numOfCalls; j++ {
				start := time.Now()
				_, err := object.Attrs(ctx)
				if err != nil {
					return fmt.Errorf("attrs failed for object %s: %w", objectName, err)
				}
				localLatencies = append(localLatencies, time.Since(start))
			}

			mu.Lock()
			latencies = append(latencies, localLatencies...)
			mu.Unlock()
			return nil
		})
	}

	err := eG.Wait()
	benchDuration := time.Since(benchStart)
	if err != nil {
		return &Result{
			Name:  name,
			Error: err.Error(),
		}
	}

	totalOps := len(latencies)
	if totalOps == 0 {
		return &Result{
			Name:  name,
			Error: "No operations performed",
		}
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	var totalDuration time.Duration
	for _, l := range latencies {
		totalDuration += l
	}

	avgLatency := float64(totalDuration/time.Microsecond) / 1000.0 / float64(totalOps)
	p50 := float64(latencies[totalOps*50/100]/time.Microsecond) / 1000.0
	p90 := float64(latencies[totalOps*90/100]/time.Microsecond) / 1000.0
	p99 := float64(latencies[totalOps*99/100]/time.Microsecond) / 1000.0
	qps := float64(totalOps) / benchDuration.Seconds()

	return &Result{
		Name:       name,
		TotalOps:   totalOps,
		Duration:   benchDuration,
		QPS:        qps,
		AvgLatency: avgLatency,
		P50Latency: p50,
		P90Latency: p90,
		P99Latency: p99,
	}
}

func main() {
	flag.Parse()
	ctx := context.Background()

	fmt.Println("=======================================================================")
	fmt.Printf("Starting StatObject Benchmark\n")
	fmt.Printf("Bucket: %s, Workers: %d, Calls/Worker: %d\n", *bucketName, *numOfWorkers, *numOfCalls)
	fmt.Println("=======================================================================")

	var results []*Result

	// 1. Run HTTP/1.1
	fmt.Print("Running HTTP/1.1 benchmark... ")
	httpClient, err := CreateHTTP1Client(ctx)
	if err != nil {
		results = append(results, &Result{Name: "HTTP/1.1", Error: err.Error()})
		fmt.Println("FAILED")
	} else {
		res := runBenchmark(ctx, "HTTP/1.1", httpClient)
		results = append(results, res)
		if res.Error != "" {
			fmt.Println("FAILED")
		} else {
			fmt.Println("DONE")
		}
		httpClient.Close()
	}

	// 2. Run gRPC Cloud-Path (DirectPath disabled)
	fmt.Print("Running gRPC Cloud-Path benchmark... ")
	grpcCloudClient, err := CreateGrpcClient(ctx, false)
	if err != nil {
		results = append(results, &Result{Name: "gRPC Cloud-Path", Error: err.Error()})
		fmt.Println("FAILED")
	} else {
		res := runBenchmark(ctx, "gRPC Cloud-Path", grpcCloudClient)
		results = append(results, res)
		if res.Error != "" {
			fmt.Println("FAILED")
		} else {
			fmt.Println("DONE")
		}
		grpcCloudClient.Close()
	}

	// 3. Run gRPC Direct-Path (DirectPath enabled)
	fmt.Print("Running gRPC Direct-Path benchmark... ")
	grpcDirectClient, err := CreateGrpcClient(ctx, true)
	if err != nil {
		results = append(results, &Result{Name: "gRPC Direct-Path", Error: err.Error()})
		fmt.Println("FAILED")
	} else {
		res := runBenchmark(ctx, "gRPC Direct-Path", grpcDirectClient)
		results = append(results, res)
		if res.Error != "" {
			fmt.Println("FAILED")
		} else {
			fmt.Println("DONE")
		}
		grpcDirectClient.Close()
	}

	// Output Comparison Table
	fmt.Println("\n========================= BENCHMARK RESULTS =========================\n")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.AlignRight|tabwriter.Debug)
	fmt.Fprintln(w, "Protocol\tTotal Ops\tElapsed Time\tQPS\tAvg Latency\tP50 Latency\tP90 Latency\tP99 Latency\tStatus/Error")
	fmt.Fprintln(w, "--------\t---------\t------------\t---\t-----------\t-----------\t-----------\t-----------\t------------")

	for _, r := range results {
		if r.Error != "" {
			fmt.Fprintf(w, "%s\t%d\t%s\t%.2f\t%.2f ms\t%.2f ms\t%.2f ms\t%.2f ms\tError: %s\n",
				r.Name, r.TotalOps, r.Duration, r.QPS, r.AvgLatency, r.P50Latency, r.P90Latency, r.P99Latency, r.Error)
		} else {
			fmt.Fprintf(w, "%s\t%d\t%s\t%.2f\t%.2f ms\t%.2f ms\t%.2f ms\t%.2f ms\tSuccess\n",
				r.Name, r.TotalOps, r.Duration.Round(time.Millisecond), r.QPS, r.AvgLatency, r.P50Latency, r.P90Latency, r.P99Latency)
		}
	}
	w.Flush()
	fmt.Println("\n=====================================================================")
}
