# MRD Pool Benchmark Application

A benchmarking tool that measures GCS read throughput using a pool of Multi-Range Downloaders (MRD) from go-storage.

## Features

- **Configurable concurrency**: Set number of threads and queue depth
- **Adjustable IO size**: Test with different read sizes
- **Real-time metrics**: Live throughput and IOPS reporting every 5 seconds
- **MRD pool management**: Round-robin distribution across multiple MRD instances
- **Per-worker statistics**: Detailed breakdown of performance per thread

## Building

```bash
cd rapid/cmd
go build -o mrd_benchmark
```

## Usage

```bash
./mrd_benchmark [options]
```

### Required Flags

- `--bucket`: GCS bucket name
- `--object`: GCS object name (file to read)

### Optional Flags

- `--threads`: Number of concurrent threads/goroutines (default: 10)
- `--io-size`: IO size in bytes (default: 4194304 = 4MB)
- `--queue-depth`: Queue depth - number of concurrent requests per thread (default: 10)
- `--pool-size`: MRD pool size - number of MultiRangeDownloader instances (default: 5)
- `--duration`: Test duration (default: 60s)
- `--project`: GCP project ID (optional)

## Examples

### Basic test with defaults
```bash
./mrd_benchmark \
  --bucket=my-bucket \
  --object=large-file.bin
```

### High-throughput test
```bash
./mrd_benchmark \
  --bucket=my-bucket \
  --object=large-file.bin \
  --threads=50 \
  --io-size=8388608 \
  --queue-depth=20 \
  --pool-size=10 \
  --duration=120s
```

### Low-concurrency, large IO test
```bash
./mrd_benchmark \
  --bucket=my-bucket \
  --object=large-file.bin \
  --threads=5 \
  --io-size=16777216 \
  --queue-depth=5 \
  --pool-size=3
```

### Small IO, high IOPS test
```bash
./mrd_benchmark \
  --bucket=my-bucket \
  --object=large-file.bin \
  --threads=100 \
  --io-size=65536 \
  --queue-depth=50 \
  --pool-size=20
```

## Output

The application provides:

1. **Configuration summary** at startup
2. **Real-time metrics** every 5 seconds:
   - Throughput (MB/s)
   - IOPS (operations per second)
   - Total data read
   - Total operations
   - Error count

3. **Final statistics** including:
   - Total duration
   - Total bytes read
   - Total operations
   - Average throughput
   - Average IOPS
   - Per-worker breakdown
   - Pool statistics

### Example Output

```
2025/11/12 10:30:00 Starting MRD Pool Benchmark
2025/11/12 10:30:00 Configuration:
2025/11/12 10:30:00   Threads: 10
2025/11/12 10:30:00   IO Size: 4194304 bytes (4.00 MB)
2025/11/12 10:30:00   Queue Depth: 10
2025/11/12 10:30:00   Pool Size: 5
2025/11/12 10:30:00   Duration: 1m0s
2025/11/12 10:30:00   Bucket: my-bucket
2025/11/12 10:30:00   Object: large-file.bin
2025/11/12 10:30:00   Total concurrent requests: 100
2025/11/12 10:30:00 Created storage client successfully
2025/11/12 10:30:00 Created MRD pool with 5 instances
2025/11/12 10:30:00 Starting 10 workers...
2025/11/12 10:30:05 Throughput: 450.23 MB/s | IOPS: 112.56 | Total: 2251.15 MB | Operations: 563 | Errors: 0
2025/11/12 10:30:10 Throughput: 478.45 MB/s | IOPS: 119.61 | Total: 4643.40 MB | Operations: 1161 | Errors: 0
...
2025/11/12 10:31:00 Waiting for pending downloads to complete...
2025/11/12 10:31:00 
=== Benchmark Complete ===
2025/11/12 10:31:00 Duration: 1m0.5s
2025/11/12 10:31:00 Total Bytes Read: 28156.25 MB
2025/11/12 10:31:00 Total Operations: 7039
2025/11/12 10:31:00 Total Errors: 0
2025/11/12 10:31:00 Average Throughput: 465.12 MB/s
2025/11/12 10:31:00 Average IOPS: 116.28
```

## How It Works

1. **Initialization**:
   - Creates a GCS storage client
   - Initializes an MRD pool with the specified number of instances
   - Spawns worker goroutines (threads)

2. **Worker Operation**:
   - Each worker maintains a queue of concurrent requests (queue depth)
   - Workers issue `Add()` calls to the MRD pool
   - The pool distributes requests round-robin across MRD instances
   - Each worker reads from different offsets to avoid contention

3. **Metrics Collection**:
   - Atomic counters track bytes read, operations, and errors
   - Stats reporter goroutine prints metrics every 5 seconds
   - Final statistics computed after all workers complete

4. **Cleanup**:
   - Workers stop after duration expires
   - Pool waits for all pending downloads
   - Final statistics displayed

## Performance Tuning

### Maximizing Throughput

- **Increase threads**: More concurrent readers
- **Increase IO size**: Larger reads per operation
- **Increase pool size**: More MRD instances for parallelism
- **Increase queue depth**: More concurrent requests per thread

### Maximizing IOPS

- **Decrease IO size**: Smaller, more frequent reads
- **Increase threads**: More concurrent operations
- **Increase queue depth**: Higher concurrency per thread
- **Increase pool size**: Better distribution

### Recommended Starting Points

| Scenario | Threads | IO Size | Queue Depth | Pool Size |
|----------|---------|---------|-------------|-----------|
| High Throughput | 50 | 8 MB | 20 | 10 |
| Balanced | 20 | 4 MB | 10 | 5 |
| High IOPS | 100 | 64 KB | 50 | 20 |
| Low Latency | 5 | 1 MB | 5 | 3 |

## Notes

- The application requires a GCS bucket and object that already exist
- Each worker reads from different offsets to simulate realistic workload
- Total concurrent requests = threads × queue depth
- Pool size should typically be much smaller than threads for efficiency
- Monitor memory usage with large IO sizes and queue depths

## Troubleshooting

### High Error Rate
- Check bucket/object permissions
- Verify object size is sufficient for all threads
- Reduce concurrency (threads, queue depth)

### Low Throughput
- Increase IO size
- Increase threads
- Increase pool size
- Check network bandwidth

### Out of Memory
- Reduce IO size
- Reduce queue depth
- Reduce number of threads

## Integration with go-storage

To integrate with the actual go-storage MultiRangeDownloader:

1. Update `createMultiRangeDownloader` in `mrd_pool.go` to use the real MRD implementation
2. Ensure the MultiRangeDownloader interface matches your go-sdk version
3. Configure MRD-specific options as needed

See the rapid package README for more details on MRD pool implementation.
