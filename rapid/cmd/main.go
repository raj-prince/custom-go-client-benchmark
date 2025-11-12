package main

import (
	"bytes"
	"context"
	"flag"
	"log"
	"os"
	"sync"
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
	ioSize          = flag.Int64("io-size", 4*1024*1024, "IO size in bytes (default: 4MB)")
	bucketName      = flag.String("bucket", "", "GCS bucket name (required)")
	objectName      = flag.String("object", "", "GCS object name (required)")
	duration        = flag.Duration("duration", 60*time.Second, "Test duration (default: 60s)")
	poolSize        = flag.Int("pool-size", 6, "MRD pool size (default: 5)")
	priorityWorkers = flag.Int("priority-workers", 2, "Number of priority workers (default: 2)")
	normalWorkers   = flag.Int("normal-workers", 8, "Number of normal workers (default: 8)")
	readMaxBlocks   = flag.Int64("read-max-blocks", 100, "Maximum concurrent read blocks (default: 100)")

	// Metrics
	totalBytesRead  uint64
	totalOperations uint64
	totalErrors     uint64
)

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

func main() {
	flag.Parse()

	// Validate required flags
	if *bucketName == "" {
		log.Fatal("--bucket is required")
	}
	if *objectName == "" {
		log.Fatal("--object is required")
	}

	log.Printf("Starting MRD Pool Benchmark with Worker Pool")
	log.Printf("Configuration:")
	log.Printf("  IO Size: %d bytes (%.2f MB)", *ioSize, float64(*ioSize)/(1024*1024))
	log.Printf("  MRD Pool Size: %d", *poolSize)
	log.Printf("  Priority Workers: %d", *priorityWorkers)
	log.Printf("  Normal Workers: %d", *normalWorkers)
	log.Printf("  Read Max Blocks: %d", *readMaxBlocks)
	log.Printf("  Duration: %v", *duration)
	log.Printf("  Bucket: %s", *bucketName)
	log.Printf("  Object: %s", *objectName)

	ctx := context.Background()

	client, err := CreateGrpcClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create storage client: %v", err)
	}
	defer client.Close()

	log.Printf("Created storage client successfully")

	// Get object attributes to determine size
	objectHandle := client.Bucket(*bucketName).Object(*objectName)
	attrs, err := objectHandle.Attrs(ctx)
	if err != nil {
		log.Fatalf("Failed to get object attributes: %v", err)
	}

	objectSize := attrs.Size
	log.Printf("Object size: %d bytes (%.2f MB)", objectSize, float64(objectSize)/(1024*1024))

	// Calculate number of blocks
	numBlocks := (objectSize + *ioSize - 1) / *ioSize
	log.Printf("Will download %d blocks of size %d bytes", numBlocks, *ioSize)

	// Create MRD pool
	poolConfig := &rapid.MRDPoolConfig{
		PoolSize: *poolSize,
		Client:   client,
		Bucket:   *bucketName,
		Object:   *objectName,
	}

	pool, err := rapid.NewMRDPool(poolConfig)
	if err != nil {
		log.Fatalf("Failed to create MRD pool: %v", err)
	}
	defer pool.Close()

	log.Printf("Created MRD pool with %d instances", *poolSize)

	// Create static worker pool
	workerPool, err := workerpool.NewStaticWorkerPool(uint32(*priorityWorkers), uint32(*normalWorkers), *readMaxBlocks)
	if err != nil {
		log.Fatalf("Failed to create worker pool: %v", err)
	}
	workerPool.Start()
	defer workerPool.Stop()

	log.Printf("Created worker pool with %d priority and %d normal workers", *priorityWorkers, *normalWorkers)

	// Create context with timeout for the benchmark
	benchCtx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	// Start stats reporter
	go statsReporter(benchCtx, 5*time.Second)

	startTime := time.Now()
	log.Printf("Starting download tasks...")

	// Track active tasks
	var activeTasks sync.WaitGroup
	tasksScheduled := int64(0)

	// Schedule download tasks for each block
	for blockIdx := int64(0); blockIdx < numBlocks; blockIdx++ {
		// Check if context is done
		select {
		case <-benchCtx.Done():
			log.Printf("Context cancelled, stopping task scheduling after %d tasks", tasksScheduled)
			goto waitForCompletion
		default:
		}

		offset := blockIdx * (*ioSize)
		length := *ioSize

		// Adjust length for the last block
		if offset+length > objectSize {
			length = objectSize - offset
		}

		block := rapid.Block{
			Offset: offset,
			Length: length,
		}

		// Create a buffer for this block
		writer := &bytes.Buffer{}

		// Create callback for this block
		blockID := blockIdx
		callback := func(off, len int64, err error) {
			defer activeTasks.Done()

			if err != nil {
				atomic.AddUint64(&totalErrors, 1)
				log.Printf("Block %d (offset %d, length %d) failed: %v", blockID, off, len, err)
			} else {
				atomic.AddUint64(&totalBytesRead, uint64(len))
				atomic.AddUint64(&totalOperations, 1)
			}
		}

		// Create download task
		task := rapid.NewDownloadTask(block, pool, writer, callback)

		// Track this task
		activeTasks.Add(1)

		// Schedule task to worker pool (use normal priority)
		workerPool.Schedule(false, task)
		tasksScheduled++

		// Throttle if too many tasks are pending
		if tasksScheduled%1000 == 0 {
			log.Printf("Scheduled %d tasks...", tasksScheduled)
		}
	}

waitForCompletion:
	log.Printf("Scheduled %d total tasks", tasksScheduled)
	log.Printf("Waiting for all tasks to complete...")

	// Wait for all tasks to finish
	activeTasks.Wait()

	// Wait for any pending downloads in the MRD pool
	pool.Wait()

	// Check for pool errors
	if err := pool.Error(); err != nil {
		log.Printf("Pool reported errors: %v", err)
	}

	elapsed := time.Since(startTime)

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

	if totalErrors > 0 {
		log.Printf("\nBenchmark completed with %d errors", totalErrors)
		os.Exit(1)
	}

	log.Printf("\nBenchmark completed successfully!")
}
