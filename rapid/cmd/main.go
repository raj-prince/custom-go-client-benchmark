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
	"github.com/raj-prince/custom-go-client-benchmark/rapid"
	"google.golang.org/api/option"
)

var (
	// Command-line flags
	numThreads = flag.Int("threads", 10, "Number of concurrent threads/goroutines")
	ioSize     = flag.Int64("io-size", 4*1024*1024, "IO size in bytes (default: 4MB)")
	queueDepth = flag.Int("queue-depth", 10, "Queue depth - number of concurrent requests per thread")
	bucketName = flag.String("bucket", "", "GCS bucket name (required)")
	objectName = flag.String("object", "", "GCS object name (required)")
	duration   = flag.Duration("duration", 60*time.Second, "Test duration (default: 60s)")
	poolSize   = flag.Int("pool-size", 5, "MRD pool size (default: 5)")
	projectID  = flag.String("project", "", "GCP project ID")

	// Metrics
	totalBytesRead  uint64
	totalOperations uint64
	totalErrors     uint64
)

// WorkerStats tracks statistics for a single worker
type WorkerStats struct {
	bytesRead  uint64
	operations uint64
	errors     uint64
}

// worker performs downloads using the MRD pool
func worker(ctx context.Context, pool *rapid.MRDPool, workerID int, queueDepth int, ioSize int64, stats *WorkerStats) error {
	semaphore := make(chan struct{}, queueDepth)
	var wg sync.WaitGroup

	offset := int64(workerID * 100 * 1024 * 1024) // Start each worker at different offset

	for {
		select {
		case <-ctx.Done():
			// Wait for in-flight requests to complete
			wg.Wait()
			return ctx.Err()
		default:
			// Acquire semaphore slot
			semaphore <- struct{}{}
			wg.Add(1)

			// Capture current offset for this request
			currentOffset := offset
			offset += ioSize

			go func(reqOffset int64) {
				defer wg.Done()
				defer func() { <-semaphore }() // Release semaphore

				var buf bytes.Buffer
				completed := false
				var downloadErr error

				err := pool.Add(&buf, reqOffset, ioSize, func(offset, length int64, err error) {
					if err != nil {
						downloadErr = err
						atomic.AddUint64(&stats.errors, 1)
						atomic.AddUint64(&totalErrors, 1)
					} else {
						completed = true
						atomic.AddUint64(&stats.bytesRead, uint64(length))
						atomic.AddUint64(&totalBytesRead, uint64(length))
					}
					atomic.AddUint64(&stats.operations, 1)
					atomic.AddUint64(&totalOperations, 1)
				})

				if err != nil {
					log.Printf("Worker %d: Add failed: %v", workerID, err)
					atomic.AddUint64(&stats.errors, 1)
					atomic.AddUint64(&totalErrors, 1)
					return
				}

				// Note: We don't call Wait() here - it will be called periodically
				// or when context is done
				_ = completed
				_ = downloadErr
			}(currentOffset)
		}
	}
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

func main() {
	flag.Parse()

	// Validate required flags
	if *bucketName == "" {
		log.Fatal("--bucket is required")
	}
	if *objectName == "" {
		log.Fatal("--object is required")
	}

	log.Printf("Starting MRD Pool Benchmark")
	log.Printf("Configuration:")
	log.Printf("  Threads: %d", *numThreads)
	log.Printf("  IO Size: %d bytes (%.2f MB)", *ioSize, float64(*ioSize)/(1024*1024))
	log.Printf("  Queue Depth: %d", *queueDepth)
	log.Printf("  Pool Size: %d", *poolSize)
	log.Printf("  Duration: %v", *duration)
	log.Printf("  Bucket: %s", *bucketName)
	log.Printf("  Object: %s", *objectName)
	log.Printf("  Total concurrent requests: %d", *numThreads**queueDepth)

	ctx := context.Background()

	// Create GCS client
	var opts []option.ClientOption

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		log.Fatalf("Failed to create storage client: %v", err)
	}
	defer client.Close()

	log.Printf("Created storage client successfully")

	// Create MRD pool
	poolConfig := &rapid.MRDPoolConfig{
		PoolSize: *poolSize,
		Client:   client,
	}

	pool, err := rapid.NewMRDPool(poolConfig)
	if err != nil {
		log.Fatalf("Failed to create MRD pool: %v", err)
	}
	defer pool.Close()

	log.Printf("Created MRD pool with %d instances", *poolSize)

	// Create context with timeout for the benchmark
	benchCtx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	// Start stats reporter
	go statsReporter(benchCtx, 5*time.Second)

	// Start workers
	var wg sync.WaitGroup
	workerStats := make([]*WorkerStats, *numThreads)

	log.Printf("Starting %d workers...", *numThreads)
	startTime := time.Now()

	for i := 0; i < *numThreads; i++ {
		workerStats[i] = &WorkerStats{}
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()
			err := worker(benchCtx, pool, workerID, *queueDepth, *ioSize, workerStats[workerID])
			if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
				log.Printf("Worker %d error: %v", workerID, err)
			}
		}(i)
	}

	// Wait for all workers to complete
	wg.Wait()

	// Wait for any pending downloads in the pool
	log.Printf("Waiting for pending downloads to complete...")
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

	// Print per-worker statistics
	log.Printf("\n=== Per-Worker Statistics ===")
	for i, stats := range workerStats {
		log.Printf("Worker %d: Operations=%d, Bytes=%.2f MB, Errors=%d",
			i,
			atomic.LoadUint64(&stats.operations),
			float64(atomic.LoadUint64(&stats.bytesRead))/(1024*1024),
			atomic.LoadUint64(&stats.errors),
		)
	}

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
