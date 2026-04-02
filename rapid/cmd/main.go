package main

import (
	"bytes"
	"context"
	"flag"
	"io"
	"log"
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

// Logger provides structured logging with INFO and DEBUG levels
type Logger struct {
	debugEnabled bool
}

// Info logs an INFO level message
func (l *Logger) Info(format string, args ...interface{}) {
	log.Printf("[INFO] "+format, args...)
}

// Debug logs a DEBUG level message (only if debug is enabled)
func (l *Logger) Debug(format string, args ...interface{}) {
	if l.debugEnabled {
		log.Printf("[DEBUG] "+format, args...)
	}
}

// Error logs an ERROR level message
func (l *Logger) Error(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}

// Fatalf logs a FATAL level message and exits
func (l *Logger) Fatalf(format string, args ...interface{}) {
	log.Fatalf("[FATAL] "+format, args...)
}

// Print logs without a level prefix (for backwards compatibility)
func (l *Logger) Print(format string, args ...interface{}) {
	log.Printf(format, args...)
}

var logger *Logger

var (
	// Command-line flags
	fIoSize          = flag.Int64("io-size", 2*1024*1024, "IO size in bytes (default: 4MB)")
	fBucketName      = flag.String("bucket", "", "GCS bucket name (required)")
	fObjectName      = flag.String("object", "", "GCS object name (required)")
	fDuration        = flag.Duration("duration", 60*time.Second, "Test duration (default: 60s)")
	fPoolSize        = flag.Int("pool-size", 1, "MRD pool size (default: 1)")
	fPriorityWorkers = flag.Int("priority-workers", 0, "Number of priority workers (default: 2)")
	fNormalWorkers   = flag.Int("normal-workers", 10, "Number of normal workers (default: 10)")
	fDiscardIO       = flag.Bool("discard-io", false, "Discard downloaded IO instead of storing in buffer")
	fDebug           = flag.Bool("debug", false, "Enable debug logging")

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
			logger.Error("Range %d (offset %d, length %d) failed: %v", rangeID, off, len, err)
		} else {
			atomic.AddUint64(&totalBytesRead, uint64(len))
			atomic.AddUint64(&totalOperations, 1)
			logger.Debug("Range %d (offset %d, length %d) completed successfully", rangeID, off, len)
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
			logger.Info("Context cancelled, stopping task scheduling after %d tasks", tasksScheduled)
			return tasksScheduled
		default:
		}

		// Create a buffer for this range
		var writer io.Writer
		if *fDiscardIO {
			writer = io.Discard
		} else {
			writer = &bytes.Buffer{}
		}

		// Create callback for this range
		callback := createDownloadCallback(int64(rangeIdx))

		// Create download task
		task := rapid.NewDownloadTask(downloadRange, pool, writer, callback)

		// Schedule task to worker pool (use normal priority)
		workerPool.Schedule(false, task)
		tasksScheduled++

		// Throttle if too many tasks are pending
		if tasksScheduled%1000 == 0 {
			logger.Debug("Scheduled %d tasks...", tasksScheduled)
		}
	}

	return tasksScheduled
}

// waitForCompletion waits for all downloads to complete and checks for errors
func waitForCompletion(pool *rapid.MRDPool) error {
	logger.Info("Waiting for all tasks to complete...")

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

	// Initialize logger
	logger = &Logger{debugEnabled: *fDebug}

	if *fDebug {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
		logger.Info("Debug logging enabled")
	}

	// Print configuration
	printConfig()

	// Create go storage client.
	ctx := context.Background()
	client, err := CreateGrpcClient(ctx)
	if err != nil {
		logger.Fatalf("Failed to create storage client: %v", err)
	}
	defer client.Close()
	logger.Debug("Created storage client successfully")

	// Get object size
	objectSize, err := getObjectSize(ctx, client, *fBucketName, *fObjectName)
	if err != nil {
		logger.Fatalf("Failed to get object attributes: %v", err)
	}
	logger.Debug("Object size: %d bytes (%.2f MB)", objectSize, float64(objectSize)/(1024*1024))

	// Generate download ranges based on IO size.
	ranges := calculateRanges(objectSize, *fIoSize)
	logger.Debug("Will download %d ranges of size %d bytes", len(ranges), *fIoSize)

	// Create MRD pool.
	poolConfig := &rapid.MRDPoolConfig{
		PoolSize: *fPoolSize,
		Client:   client,
		Bucket:   *fBucketName,
		Object:   *fObjectName,
	}
	pool, err := rapid.NewMRDPool(poolConfig)
	if err != nil {
		logger.Fatalf("Failed to create MRD pool: %v", err)
	}
	logger.Debug("Created MRD pool with %d instances", *fPoolSize)

	// Create static worker pool
	workerPool, err := workerpool.NewStaticWorkerPool(uint32(*fPriorityWorkers), uint32(*fNormalWorkers), 10000000)
	if err != nil {
		logger.Fatalf("Failed to create worker pool: %v", err)
	}
	workerPool.Start()
	logger.Debug("Created worker pool with %d priority and %d normal workers", *fPriorityWorkers, *fNormalWorkers)

	// Create context with timeout for the benchmark
	benchCtx, cancel := context.WithTimeout(ctx, *fDuration)
	defer cancel()

	// Start stats reporter
	go statsReporter(benchCtx, 5*time.Second)

	startTime := time.Now()
	logger.Debug("Starting download tasks...")

	// Schedule download tasks
	tasksScheduled := scheduleDownloadTasks(benchCtx, ranges, pool, workerPool)
	logger.Debug("Scheduled %d total tasks", tasksScheduled)

	// Wait for completion
	if err := waitForCompletion(pool); err != nil {
		logger.Error("Pool reported errors: %v", err)
	}

	elapsed := time.Since(startTime)

	workerPool.Stop()
	pool.Close()

	// Print final statistics
	printFinalStatistics(elapsed, pool)

	if totalErrors > 0 {
		logger.Fatalf("\nBenchmark completed with %d errors", totalErrors)
	}

	logger.Info("\nBenchmark completed successfully!")
}

// printFinalStatistics prints the benchmark results
func printFinalStatistics(elapsed time.Duration, pool *rapid.MRDPool) {
	// Print final statistics
	logger.Info("\n=== Benchmark Complete ===")
	logger.Info("Duration: %v", elapsed)
	logger.Info("Total Bytes Read: %.2f MB", float64(totalBytesRead)/(1024*1024))
	logger.Info("Total Operations: %d", totalOperations)
	logger.Info("Total Errors: %d", totalErrors)
	logger.Info("Average Throughput: %.2f MB/s", float64(totalBytesRead)/elapsed.Seconds()/(1024*1024))
	logger.Info("Average IOPS: %.2f", float64(totalOperations)/elapsed.Seconds())

	// Print pool statistics
	poolStats := pool.GetStats()
	logger.Info("\n=== Pool Statistics ===")
	logger.Info("Pool Size: %d", poolStats.PoolSize)
	logger.Info("Total Requests: %d", poolStats.RequestCount)
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
	logger.Info("Starting MRD Pool Benchmark with Worker Pool")
	logger.Info("Configuration:")
	logger.Info("  IO Size: %d bytes (%.2f MB)", *fIoSize, float64(*fIoSize)/(1024*1024))
	logger.Info("  MRD Pool Size: %d", *fPoolSize)
	logger.Info("  Priority Workers: %d", *fPriorityWorkers)
	logger.Info("  Normal Workers: %d", *fNormalWorkers)
	logger.Info("  Duration: %v", *fDuration)
	logger.Info("  Bucket: %s", *fBucketName)
	logger.Info("  Object: %s", *fObjectName)
	logger.Info("  Debug Mode: %v", *fDebug)
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

			logger.Info("Throughput: %.2f MB/s | IOPS: %.2f | Total: %.2f MB | Operations: %d | Errors: %d",
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
