package rapid

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"sync"

	"cloud.google.com/go/storage"
)

// ExampleUsage demonstrates how to use the MRDPool for concurrent downloads.
func ExampleUsage() {
	ctx := context.Background()

	// Create a GCS storage client
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create MRD pool with desired size
	// Larger pool size allows more concurrent downloads
	config := &MRDPoolConfig{
		PoolSize: 10, // Create 10 MultiRangeDownloader instances
		Client:   client,
	}

	pool, err := NewMRDPool(config)
	if err != nil {
		log.Fatalf("Failed to create MRD pool: %v", err)
	}
	defer pool.Close()

	// Example 1: Single download request
	// The pool automatically selects an MRD instance using round-robin
	var buf bytes.Buffer
	err = pool.Add(&buf, 0, 1024*1024, func(offset, length int64, err error) {
		if err != nil {
			log.Printf("Download failed: %v", err)
			return
		}
		fmt.Printf("Downloaded %d bytes at offset %d\n", length, offset)
	})
	if err != nil {
		log.Fatalf("Add failed: %v", err)
	}

	// Wait for downloads to complete
	pool.Wait()

	// Check for errors
	if err := pool.Error(); err != nil {
		log.Fatalf("Pool error: %v", err)
	}

	// Example 2: Concurrent downloads
	// Multiple goroutines can safely use the pool
	// Each request will be distributed across the pool using round-robin
	numWorkers := 50
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			var buf bytes.Buffer
			err := pool.Add(&buf, int64(workerID*4096), 4096, func(offset, length int64, err error) {
				if err != nil {
					log.Printf("Worker %d failed: %v", workerID, err)
				}
			})

			if err != nil {
				log.Printf("Worker %d add failed: %v", workerID, err)
			}
		}(i)
	}

	// Wait for all workers
	wg.Wait()
	pool.Wait()

	// Check for errors
	if err := pool.Error(); err != nil {
		log.Printf("Pool error: %v", err)
	}

	// Get pool statistics
	stats := pool.GetStats()
	fmt.Printf("Pool Statistics:\n")
	fmt.Printf("  Pool Size: %d\n", stats.PoolSize)
	fmt.Printf("  Total Requests: %d\n", stats.RequestCount)
	fmt.Printf("  Closed: %v\n", stats.Closed)
}

// ExampleWithRetry demonstrates using the pool with retry logic.
func ExampleWithRetry() {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	config := &MRDPoolConfig{
		PoolSize: 5,
		Client:   client,
	}

	pool, err := NewMRDPool(config)
	if err != nil {
		log.Fatalf("Failed to create MRD pool: %v", err)
	}
	defer pool.Close()

	// Download with retry logic
	maxRetries := 3
	var buf bytes.Buffer

	for attempt := 0; attempt < maxRetries; attempt++ {
		err = pool.Add(&buf, 0, 10*1024*1024, func(offset, length int64, err error) {
			if err != nil {
				log.Printf("Download failed: %v", err)
			}
		})

		if err == nil {
			pool.Wait()
			if pool.Error() == nil {
				break
			}
		}
		log.Printf("Attempt %d failed: %v", attempt+1, err)
	}

	if err != nil {
		log.Fatalf("Failed after %d attempts: %v", maxRetries, err)
	}

	fmt.Printf("Successfully downloaded %d bytes\n", buf.Len())
}

// ExampleChunkedDownload demonstrates downloading a large file in chunks.
func ExampleChunkedDownload() {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	config := &MRDPoolConfig{
		PoolSize: 8,
		Client:   client,
	}

	pool, err := NewMRDPool(config)
	if err != nil {
		log.Fatalf("Failed to create MRD pool: %v", err)
	}
	defer pool.Close()

	// Download a 1GB file in 10MB chunks
	fileSizeBytes := int64(1024 * 1024 * 1024) // 1GB
	chunkSize := int64(10 * 1024 * 1024)       // 10MB
	numChunks := (fileSizeBytes + chunkSize - 1) / chunkSize

	// Download chunks concurrently
	type chunkResult struct {
		index  int
		buffer *bytes.Buffer
		err    error
	}

	results := make(chan chunkResult, numChunks)
	var wg sync.WaitGroup

	for i := int64(0); i < numChunks; i++ {
		wg.Add(1)
		go func(chunkIndex int64) {
			defer wg.Done()

			start := chunkIndex * chunkSize
			length := chunkSize
			if start+length > fileSizeBytes {
				length = fileSizeBytes - start
			}

			buf := &bytes.Buffer{}
			err := pool.Add(buf, start, length, func(offset, length int64, err error) {
				if err != nil {
					results <- chunkResult{index: int(chunkIndex), err: err}
				}
			})

			if err != nil {
				results <- chunkResult{index: int(chunkIndex), err: err}
			} else {
				results <- chunkResult{index: int(chunkIndex), buffer: buf}
			}
		}(i)
	}

	// Wait for all chunks to be queued
	wg.Wait()

	// Wait for all downloads to complete
	pool.Wait()

	// Check for errors
	if err := pool.Error(); err != nil {
		log.Printf("Pool error: %v", err)
	}

	// Collect results
	chunks := make([]*bytes.Buffer, numChunks)
	for i := int64(0); i < numChunks; i++ {
		result := <-results
		if result.err != nil {
			log.Printf("Chunk %d failed: %v", result.index, result.err)
			continue
		}
		chunks[result.index] = result.buffer
		fmt.Printf("Downloaded chunk %d (%d bytes)\n", result.index, result.buffer.Len())
	}

	// Process or assemble chunks...
	totalBytes := int64(0)
	for _, chunk := range chunks {
		if chunk != nil {
			totalBytes += int64(chunk.Len())
		}
	}
	fmt.Printf("Downloaded %d chunks, total %d bytes\n", numChunks, totalBytes)
}
