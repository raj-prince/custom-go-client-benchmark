# Multi-Range Downloader (MRD) Pool

This package provides a pool implementation for managing multiple `MultiRangeDownloader` instances with round-robin load distribution.

## Overview

The MRD Pool allows you to:
- Create a pool of multiple `MultiRangeDownloader` instances
- Distribute download requests across the pool using round-robin scheduling
- Handle concurrent downloads efficiently
- Monitor pool statistics and usage

## Features

- **Round-Robin Distribution**: Requests are automatically distributed across all MRD instances in the pool
- **Thread-Safe**: Safe for concurrent access from multiple goroutines
- **Configurable Pool Size**: Choose the optimal number of MRD instances for your workload
- **Resource Management**: Proper cleanup and closing of all pool resources
- **Statistics**: Track pool usage and request counts

## Usage

### Basic Usage

```go
package main

import (
    "context"
    "log"
    
    "cloud.google.com/go/storage"
    "github.com/raj-prince/custom-go-client-benchmark/rapid"
)

func main() {
    ctx := context.Background()
    
    // Create GCS client
    client, err := storage.NewClient(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()
    
    // Create MRD pool
    config := &rapid.MRDPoolConfig{
        PoolSize: 10,
        Client:   client,
    }
    
    pool, err := rapid.NewMRDPool(config)
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close()
    
    // Use the pool
    output, err := pool.DownloadObjectRanges(ctx, &rapid.DownloadObjectRangesParams{
        Bucket: "my-bucket",
        Key:    "my-file.bin",
        Ranges: []rapid.ByteRange{
            {Start: 0, End: 1024},
            {Start: 1024, End: 2048},
        },
    })
    
    if err != nil {
        log.Fatal(err)
    }
    
    // Process the readers
    for _, reader := range output.Readers {
        defer reader.Close()
        // Read and process data
    }
}
```

### Concurrent Downloads

```go
// Multiple goroutines can safely use the pool
numWorkers := 50
for i := 0; i < numWorkers; i++ {
    go func(workerID int) {
        params := &rapid.DownloadObjectRangesParams{
            Bucket: "my-bucket",
            Key:    fmt.Sprintf("file-%d.bin", workerID),
            Ranges: []rapid.ByteRange{
                {Start: 0, End: 4096},
            },
        }
        
        output, err := pool.DownloadObjectRanges(ctx, params)
        if err != nil {
            log.Printf("Worker %d failed: %v", workerID, err)
            return
        }
        
        // Process output
        for _, reader := range output.Readers {
            io.Copy(io.Discard, reader)
            reader.Close()
        }
    }(i)
}
```

### Pool Statistics

```go
stats := pool.GetStats()
fmt.Printf("Pool Size: %d\n", stats.PoolSize)
fmt.Printf("Total Requests: %d\n", stats.RequestCount)
fmt.Printf("Closed: %v\n", stats.Closed)
```

## Configuration

### MRDPoolConfig

| Field | Type | Description |
|-------|------|-------------|
| `PoolSize` | `int` | Number of MRD instances in the pool (must be > 0) |
| `Client` | `*storage.Client` | GCS storage client used to create MRD instances |

## API Reference

### NewMRDPool

```go
func NewMRDPool(config *MRDPoolConfig) (*MRDPool, error)
```

Creates a new pool of MultiRangeDownloader instances.

**Parameters:**
- `config`: Configuration for the pool

**Returns:**
- Pool instance or error if creation fails

### DownloadObjectRanges

```go
func (p *MRDPool) DownloadObjectRanges(ctx context.Context, params *DownloadObjectRangesParams) (*DownloadObjectRangesOutput, error)
```

Downloads object ranges using round-robin distribution across the pool.

**Parameters:**
- `ctx`: Context for the request
- `params`: Download parameters including bucket, key, and ranges

**Returns:**
- Download output with readers or error

### Close

```go
func (p *MRDPool) Close() error
```

Closes all MultiRangeDownloader instances in the pool. Safe to call multiple times.

### GetStats

```go
func (p *MRDPool) GetStats() PoolStats
```

Returns current pool statistics including size, request count, and status.

## Round-Robin Algorithm

The pool uses an atomic counter to distribute requests:

1. Each request increments an atomic counter
2. The counter modulo pool size determines which MRD instance to use
3. This ensures even distribution across all instances
4. Thread-safe for concurrent access

## Performance Considerations

### Choosing Pool Size

- **Small workloads**: Pool size 3-5 may be sufficient
- **High concurrency**: Pool size 10-20 for many concurrent requests
- **Memory constraints**: Each MRD instance consumes memory; balance pool size with available resources
- **Network limits**: Consider bandwidth and connection limits

### Best Practices

1. **Reuse the pool**: Create one pool and reuse it across your application
2. **Close properly**: Always defer pool.Close() to release resources
3. **Context management**: Use appropriate context timeouts for downloads
4. **Error handling**: Implement retry logic for transient failures
5. **Monitor statistics**: Use GetStats() to understand pool usage patterns

## Integration with go-sdk

This implementation is designed to work with the GCS go-sdk's `MultiRangeDownloader`. To integrate:

1. Update the `createMultiRangeDownloader` function in `mrd_pool.go` to use the actual go-sdk MRD creation
2. Ensure the `MultiRangeDownloader` interface matches your go-sdk version
3. Add any additional configuration options needed by your MRD instances

Example integration:

```go
func createMultiRangeDownloader(client *storage.Client) (MultiRangeDownloader, error) {
    // Replace with actual go-sdk MRD creation
    return storage.NewMultiRangeDownloader(client, &storage.MRDOptions{
        // Configure as needed
    })
}
```

## Testing

Run the tests:

```bash
go test ./rapid/...
```

Run with coverage:

```bash
go test -cover ./rapid/...
```

Run benchmarks:

```bash
go test -bench=. ./rapid/...
```

## Examples

See `mrd_pool_example.go` for complete examples including:
- Basic usage
- Concurrent downloads
- Retry logic
- Chunked downloads

## Thread Safety

All methods are thread-safe and can be called concurrently from multiple goroutines:
- `DownloadObjectRanges`: Thread-safe using atomic operations for round-robin
- `Close`: Protected by mutex, safe to call multiple times
- `GetStats`: Thread-safe read access
- `IsClosed`: Thread-safe read access

## Error Handling

The pool handles various error conditions:
- Invalid configuration (nil config, invalid pool size, nil client)
- Closed pool access (returns error)
- Failed MRD creation (cleans up partial initialization)
- Individual MRD failures during close (collects all errors)

## License

See LICENSE file for details.
