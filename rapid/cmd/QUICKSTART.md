# Quick Start Guide

## Prerequisites

1. GCP credentials configured (gcloud auth application-default login)
2. Go 1.19+ installed
3. Access to a GCS bucket with a test file

## Build

```bash
cd rapid/cmd
go build -o mrd_benchmark
```

## Quick Test

```bash
# Using the binary directly
./mrd_benchmark \
  --bucket=your-bucket \
  --object=your-test-file

# Using the convenience script
./run_benchmark.sh \
  --bucket=your-bucket \
  --object=your-test-file \
  --scenario=balanced
```

## Common Scenarios

### 1. Maximum Throughput Test
```bash
./run_benchmark.sh \
  --bucket=your-bucket \
  --object=your-test-file \
  --scenario=high-throughput
```

**Configuration:**
- 50 threads
- 8 MB IO size
- Queue depth: 20
- Pool size: 10
- Duration: 2 minutes

### 2. Maximum IOPS Test
```bash
./run_benchmark.sh \
  --bucket=your-bucket \
  --object=your-test-file \
  --scenario=high-iops
```

**Configuration:**
- 100 threads
- 64 KB IO size
- Queue depth: 50
- Pool size: 20
- Duration: 1 minute

### 3. Low Latency Test
```bash
./run_benchmark.sh \
  --bucket=your-bucket \
  --object=your-test-file \
  --scenario=low-latency
```

**Configuration:**
- 5 threads
- 1 MB IO size
- Queue depth: 5
- Pool size: 3
- Duration: 1 minute

### 4. Custom Configuration
```bash
./mrd_benchmark \
  --bucket=your-bucket \
  --object=your-test-file \
  --threads=25 \
  --io-size=2097152 \
  --queue-depth=15 \
  --pool-size=8 \
  --duration=90s
```

## Understanding the Parameters

### Threads (`--threads`)
- Number of concurrent goroutines reading data
- More threads = higher concurrency
- Start with 10-20 for most workloads

### IO Size (`--io-size`)
- Size of each read operation in bytes
- Common values:
  - 64 KB (65536) - High IOPS
  - 1 MB (1048576) - Balanced
  - 4 MB (4194304) - Default, good throughput
  - 8 MB (8388608) - Maximum throughput

### Queue Depth (`--queue-depth`)
- Number of concurrent requests per thread
- Total concurrency = threads × queue depth
- Higher values = more memory usage
- Start with 10

### Pool Size (`--pool-size`)
- Number of MRD instances in the pool
- Should be much smaller than threads
- Typical range: 3-20
- More instances = better distribution but more overhead

### Duration (`--duration`)
- How long to run the test
- Format: 60s, 2m, 1h30m
- Default: 60s

## Reading the Output

### Real-time Metrics (every 5 seconds)
```
Throughput: 450.23 MB/s | IOPS: 112.56 | Total: 2251.15 MB | Operations: 563 | Errors: 0
```
- **Throughput**: Data transfer rate in MB/s
- **IOPS**: Operations per second
- **Total**: Cumulative data read
- **Operations**: Total number of read operations
- **Errors**: Number of failed operations

### Final Statistics
```
=== Benchmark Complete ===
Duration: 1m0.5s
Total Bytes Read: 28156.25 MB
Total Operations: 7039
Total Errors: 0
Average Throughput: 465.12 MB/s
Average IOPS: 116.28
```

### Per-Worker Stats
Shows performance breakdown for each thread:
```
Worker 0: Operations=704, Bytes=2816.00 MB, Errors=0
Worker 1: Operations=703, Bytes=2812.00 MB, Errors=0
...
```

### Pool Statistics
```
Pool Size: 5
Total Requests: 7039
Pool Closed: false
```
- Shows how the MRD pool was utilized

## Tips for Good Results

1. **File Size**: Use a file larger than total_threads × queue_depth × io_size
2. **Warm-up**: First run might be slower due to cold caches
3. **Consistency**: Run multiple times and average results
4. **Monitoring**: Watch for errors - high error rate indicates configuration issues
5. **Resources**: Monitor CPU and memory usage on your machine

## Troubleshooting

### "Failed to create storage client"
- Check GCP credentials: `gcloud auth application-default login`
- Verify project permissions

### "Failed to create MRD pool"
- This likely means the go-storage integration needs to be completed
- See the main README for integration instructions

### High error rate
- Reduce concurrency (threads or queue depth)
- Check if file is large enough
- Verify bucket/object permissions

### Low throughput
- Increase IO size
- Increase threads
- Check network bandwidth
- Increase pool size

### Out of memory
- Reduce IO size
- Reduce queue depth
- Reduce threads

## Example Complete Run

```bash
$ ./mrd_benchmark --bucket=test-bucket --object=100GB-file --threads=30 --io-size=4194304

2025/11/12 10:30:00 Starting MRD Pool Benchmark
2025/11/12 10:30:00 Configuration:
2025/11/12 10:30:00   Threads: 30
2025/11/12 10:30:00   IO Size: 4194304 bytes (4.00 MB)
2025/11/12 10:30:00   Queue Depth: 10
2025/11/12 10:30:00   Pool Size: 5
2025/11/12 10:30:00   Duration: 1m0s
2025/11/12 10:30:00   Bucket: test-bucket
2025/11/12 10:30:00   Object: 100GB-file
2025/11/12 10:30:00   Total concurrent requests: 300
2025/11/12 10:30:00 Created storage client successfully
2025/11/12 10:30:00 Created MRD pool with 5 instances
2025/11/12 10:30:00 Starting 30 workers...
2025/11/12 10:30:05 Throughput: 892.45 MB/s | IOPS: 223.11 | Total: 4462.25 MB | Operations: 1116 | Errors: 0
2025/11/12 10:30:10 Throughput: 915.23 MB/s | IOPS: 228.81 | Total: 9038.40 MB | Operations: 2260 | Errors: 0
[... more stats every 5 seconds ...]
2025/11/12 10:31:00 Waiting for pending downloads to complete...
2025/11/12 10:31:00 
=== Benchmark Complete ===
2025/11/12 10:31:00 Duration: 1m0.3s
2025/11/12 10:31:00 Total Bytes Read: 54892.50 MB
2025/11/12 10:31:00 Total Operations: 13723
2025/11/12 10:31:00 Total Errors: 0
2025/11/12 10:31:00 Average Throughput: 910.25 MB/s
2025/11/12 10:31:00 Average IOPS: 227.56

=== Per-Worker Statistics ===
Worker 0: Operations=458, Bytes=1832.00 MB, Errors=0
Worker 1: Operations=457, Bytes=1828.00 MB, Errors=0
[... stats for each worker ...]

=== Pool Statistics ===
Pool Size: 5
Total Requests: 13723
Pool Closed: false

Benchmark completed successfully!
```

## Next Steps

1. Run with your actual GCS data
2. Compare different configurations
3. Integrate with real go-storage MRD implementation
4. Add custom metrics or export to monitoring systems
5. Tune parameters for your specific workload
