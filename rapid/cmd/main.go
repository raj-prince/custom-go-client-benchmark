package main

import (
	"bytes"
	"context"
	"flag"
	"log"
	"os"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"cloud.google.com/go/storage/experimental"
	"github.com/raj-prince/custom-go-client-benchmark/rapid"
	"github.com/raj-prince/custom-go-client-benchmark/rapid/workerpool"
	"google.golang.org/api/option"

	// Side effect to run grpc client with direct-path on gcp machine.
	_ "google.golang.org/grpc/balancer/rls"
	_ "google.golang.org/grpc/xds/googledirectpath"
)

var (
	// Command-line flags
	fIoSize          = flag.Int64("io-size", 4*1024*1024, "IO size in bytes (default: 4MB)")
	fBucketName      = flag.String("bucket", "", "GCS bucket name (required)")
	fObjectName      = flag.String("object", "", "GCS object name (required)")
	fDuration        = flag.Duration("duration", 60*time.Second, "Test duration (default: 60s)")
	fPoolSize        = flag.Int("pool-size", 1, "MRD pool size (default: 1)")
	fPriorityWorkers = flag.Int("priority-workers", 0, "Number of priority workers (default: 2)")
	fNormalWorkers   = flag.Int("normal-workers", 7, "Number of normal workers (default: 7)")
	fReadMaxBlocks   = flag.Int64("read-max-blocks", 100, "Maximum concurrent read blocks (default: 100)")

	// Metrics
	totalBytesRead  uint64
	totalOperations uint64
	totalErrors     uint64
)

func CreateGrpcClient(ctx context.Context) (client *storage.Client, err error) {
	tokenSource, err := rapid.GetTokenSource(ctx, "")
	if err != nil {
		return nil, err
	}
	return storage.NewGRPCClient(ctx,
		option.WithGRPCConnectionPool(1),
		option.WithTokenSource(tokenSource),
		storage.WithDisabledClientMetrics(),
		experimental.WithGRPCBidiReads(),
	)
}

// getObjectSize retrieves the size of the object from GCS
func getObjectSize(ctx context.Context, client *storage.Client, bucket, object string) (int64, error) {
	objectHandle := client.Bucket(bucket).Object(object)
	attrs, err := objectHandle.Attrs(ctx)
	if err != nil {
		return 0, err
	}
	return attrs.Size, nil
}

// calculateRanges computes the download ranges based on object size and IO size
func calculateRanges(objectSize, ioSize int64) []rapid.Range {
	numRanges := (objectSize + ioSize - 1) / ioSize
	ranges := make([]rapid.Range, 0, numRanges)

	for rangeIdx := int64(0); rangeIdx < numRanges; rangeIdx++ {
		offset := rangeIdx * ioSize
		length := ioSize

		// Adjust length for the last range
		if offset+length > objectSize {
			length = objectSize - offset
		}

		ranges = append(ranges, rapid.Range{
			Offset: offset,
			Length: length,
		})
	}

	return ranges
}

// createDownloadCallback creates a callback function for a download task
func createDownloadCallback(rangeID int64) func(int64, int64, error) {
	return func(off, len int64, err error) {
		if err != nil {
			atomic.AddUint64(&totalErrors, 1)
			log.Printf("Range %d (offset %d, length %d) failed: %v", rangeID, off, len, err)
		} else {
			atomic.AddUint64(&totalBytesRead, uint64(len))
			atomic.AddUint64(&totalOperations, 1)
		}
	}
}

// scheduleDownloadTasks schedules all download tasks to the worker pool
func scheduleDownloadTasks(
	ctx context.Context,
	ranges []rapid.Range,
	pool *rapid.MRDPool,
	workerPool workerpool.WorkerPool,
) int64 {
	tasksScheduled := int64(0)

	for rangeIdx, downloadRange := range ranges {
		// Check if context is done
		select {
		case <-ctx.Done():
			log.Printf("Context cancelled, stopping task scheduling after %d tasks", tasksScheduled)
			return tasksScheduled
		default:
		}

		// Create a buffer for this range
		writer := &bytes.Buffer{}

		// Create callback for this range
		callback := createDownloadCallback(int64(rangeIdx))

		// Create download task
		task := rapid.NewDownloadTask(downloadRange, pool, writer, callback)

		// Schedule task to worker pool (use normal priority)
		workerPool.Schedule(false, task)
		tasksScheduled++

		// Throttle if too many tasks are pending
		if tasksScheduled%1000 == 0 {
			log.Printf("Scheduled %d tasks...", tasksScheduled)
		}
	}

	return tasksScheduled
}

// waitForCompletion waits for all downloads to complete and checks for errors
func waitForCompletion(pool *rapid.MRDPool) error {
	log.Printf("Waiting for all tasks to complete...")

	// Wait for any pending downloads in the MRD pool
	pool.Wait()

	// Check for pool errors
	return pool.Error()
}

func main() {
	// Parse and validate configuration
	if err := parseAndValidateConfig(); err != nil {
		log.Fatal("--bucket and --object are required")
	}

	// Print configuration
	printConfig()

	// Create go storage client.
	ctx := context.Background()
	client, err := CreateGrpcClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create storage client: %v", err)
	}
	defer client.Close()
	log.Printf("Created storage client successfully")

	// Get object size
	objectSize, err := getObjectSize(ctx, client, *fBucketName, *fObjectName)
	if err != nil {
		log.Fatalf("Failed to get object attributes: %v", err)
	}
	log.Printf("Object size: %d bytes (%.2f MB)", objectSize, float64(objectSize)/(1024*1024))

	// Generate download ranges based on IO size.
	ranges := calculateRanges(objectSize, *fIoSize)
	log.Printf("Will download %d ranges of size %d bytes", len(ranges), *fIoSize)

	// Create MRD pool.
	poolConfig := &rapid.MRDPoolConfig{
		PoolSize: *fPoolSize,
		Client:   client,
		Bucket:   *fBucketName,
		Object:   *fObjectName,
	}
	pool, err := rapid.NewMRDPool(poolConfig)
	if err != nil {
		log.Fatalf("Failed to create MRD pool: %v", err)
	}
	defer pool.Close()
	log.Printf("Created MRD pool with %d instances", *fPoolSize)

	// Create static worker pool
	workerPool, err := workerpool.NewStaticWorkerPool(uint32(*fPriorityWorkers), uint32(*fNormalWorkers), *fReadMaxBlocks)
	if err != nil {
		log.Fatalf("Failed to create worker pool: %v", err)
	}
	workerPool.Start()
	defer workerPool.Stop()
	log.Printf("Created worker pool with %d priority and %d normal workers", *fPriorityWorkers, *fNormalWorkers)

	// Create context with timeout for the benchmark
	benchCtx, cancel := context.WithTimeout(ctx, *fDuration)
	defer cancel()

	// Start stats reporter
	go statsReporter(benchCtx, 5*time.Second)

	startTime := time.Now()
	log.Printf("Starting download tasks...")

	// Schedule download tasks
	tasksScheduled := scheduleDownloadTasks(benchCtx, ranges, pool, workerPool)
	log.Printf("Scheduled %d total tasks", tasksScheduled)

	// Wait for completion
	if err := waitForCompletion(pool); err != nil {
		log.Printf("Pool reported errors: %v", err)
	}

	elapsed := time.Since(startTime)

	// Print final statistics
	printFinalStatistics(elapsed, pool)

	if totalErrors > 0 {
		log.Printf("\nBenchmark completed with %d errors", totalErrors)
		os.Exit(1)
	}

	log.Printf("\nBenchmark completed successfully!")
}

// printFinalStatistics prints the benchmark results
func printFinalStatistics(elapsed time.Duration, pool *rapid.MRDPool) {
	// Print final statistics
	log.Printf("\n=== Benchmark Complete ===")
	log.Printf("Duration: %v", elapsed)
	log.Printf("Total Bytes Read: %.2f MB", float64(totalBytesRead)/(1024*1024))
	log.Printf("Total Operations: %d", totalOperations)
	log.Printf("Total Errors: %d", totalErrors)
	log.Printf("Average Throughput: %.2f MB/s", float64(totalBytesRead)/elapsed.Seconds()/(1024*1024))
	log.Printf("Average IOPS: %.2f", float64(totalOperations)/elapsed.Seconds())

	// Print pool statistics
	poolStats := pool.GetStats()
	log.Printf("\n=== Pool Statistics ===")
	log.Printf("Pool Size: %d", poolStats.PoolSize)
	log.Printf("Total Requests: %d", poolStats.RequestCount)
	log.Printf("Pool Closed: %v", poolStats.Closed)
}

// parseAndValidateConfig parses command-line flags and validates the configuration
func parseAndValidateConfig() error {
	flag.Parse()

	// Validate required flags
	if *fBucketName == "" {
		return flag.ErrHelp
	}
	if *fObjectName == "" {
		return flag.ErrHelp
	}

	return nil
}

// printConfig prints the benchmark configuration
func printConfig() {
	log.Printf("Starting MRD Pool Benchmark with Worker Pool")
	log.Printf("Configuration:")
	log.Printf("  IO Size: %d bytes (%.2f MB)", *fIoSize, float64(*fIoSize)/(1024*1024))
	log.Printf("  MRD Pool Size: %d", *fPoolSize)
	log.Printf("  Priority Workers: %d", *fPriorityWorkers)
	log.Printf("  Normal Workers: %d", *fNormalWorkers)
	log.Printf("  Read Max Blocks: %d", *fReadMaxBlocks)
	log.Printf("  Duration: %v", *fDuration)
	log.Printf("  Bucket: %s", *fBucketName)
	log.Printf("  Object: %s", *fObjectName)
}

// statsReporter periodically reports throughput statistics
func statsReporter(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lastBytes := uint64(0)
	lastOps := uint64(0)
	lastTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			elapsed := now.Sub(lastTime).Seconds()

			currentBytes := atomic.LoadUint64(&totalBytesRead)
			currentOps := atomic.LoadUint64(&totalOperations)
			currentErrors := atomic.LoadUint64(&totalErrors)

			bytesDelta := currentBytes - lastBytes
			opsDelta := currentOps - lastOps

			throughputMBps := float64(bytesDelta) / elapsed / (1024 * 1024)
			iops := float64(opsDelta) / elapsed

			log.Printf("Throughput: %.2f MB/s | IOPS: %.2f | Total: %.2f MB | Operations: %d | Errors: %d",
				throughputMBps,
				iops,
				float64(currentBytes)/(1024*1024),
				currentOps,
				currentErrors,
			)

			lastBytes = currentBytes
			lastOps = currentOps
			lastTime = now
		}
	}
}
