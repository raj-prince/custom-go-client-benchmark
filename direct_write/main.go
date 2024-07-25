package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/ncw/directio"
)

var (
	fChunkSize     = flag.Int("chunk-size", 1048576, "Chunks size for write syscall")
	fNumGoroutines = flag.Int("routine", 8, "Chunks size for write syscall")
)

const (
	filePath      = "test.txt"        // Replace with your actual file path
	totalFileSize = 1024 * 1024 * 1024 // Total desired file size

	oneMiB = 1024 * 1024
)

func main() {
	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	flag.Parse()

	// Open file in append mode with permissions
	file, err := directio.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Truncate or extend the file to the desired size
	if err := file.Truncate(totalFileSize); err != nil {
		fmt.Println("Error truncating file:", err)
		return
	}

	fileSizePerRoutine := totalFileSize / *fNumGoroutines

	var wg sync.WaitGroup

	startTime := time.Now()
	for i := 0; i < *fNumGoroutines; i++ {
		wg.Add(1)
		go func(offset int64) {
			defer wg.Done()
			data := directio.AlignedBlock(*fChunkSize)
			writer := io.NewOffsetWriter(file, offset)
			// Determine number of chunks to write
			chunksToWrite := fileSizePerRoutine / *fChunkSize

			// Write data in chunks
			for j := 0; j < chunksToWrite; j++ {
				rand.Read(data)
				if _, err := writer.Write(data); err != nil {
					fmt.Println("Error writing to file:", err)
					return
				}
			}
		}(int64(i * fileSizePerRoutine)) // Pass the offset to each goroutin
	}

	// Signal goroutines to stop
	wg.Wait()

	duration := time.Since(startTime)

	fmt.Printf("Total time (sec)  : %f\n", duration.Seconds())
	fmt.Printf("Throughput (MiB/s): %f\n", (totalFileSize / oneMiB) / duration.Seconds())
	fmt.Println("Writing completed!")
}
