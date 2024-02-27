package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"syscall"
	"time"
)

var (
	fDir = flag.String("dir", "", "Directory file to be opened.")

	fileCount = flag.Int("file-count", 10, "Number of files to read")

	FileSize = 512 * OneKB
	OneKB = 1024
	openLatency []int64
	readLatency []int64
	closeLatency []int64
	totalReadLatency []int64
)

func ReadFilesSequentially(fileCount int) (err error) {
	startTime := time.Now()

	b := make([]byte, FileSize*1024)

	for i := 0; i < fileCount; i++ {

		openStart := time.Now()
		// Open file
		fileName := path.Join(*fDir, "Workload.0/" + strconv.Itoa(i))
		var fileHandle *os.File
		fileHandle, err = os.OpenFile(fileName, os.O_RDONLY|syscall.O_DIRECT, 0600)
		if err != nil {
			err = fmt.Errorf("while opening file: %w", err)
			return
		}
		openFileTime := time.Since(openStart)
		openLatency = append(openLatency, openFileTime.Microseconds())
		readStart := time.Now()

		_, err = fileHandle.Read(b)
		if err != nil {
			err = fmt.Errorf("while reading file: %w", err)
			return
		}
		readFileTime := time.Since(readStart)
		readLatency = append(readLatency, readFileTime.Microseconds())
		closeStart := time.Now()

		// Close file
		err = fileHandle.Close()
		if err != nil {
			err = fmt.Errorf("while closing file: %w", err)
		}
		closeTime := time.Since(closeStart)
		closeLatency = append(closeLatency, closeTime.Microseconds())
		totalReadTime := time.Since(openStart)
		totalReadLatency = append(totalReadLatency, totalReadTime.Microseconds())
	}

	totalTime := time.Since(startTime)
	fmt.Println("Total time: ", totalTime)

	return
}

func Report(latency []int64, prefix string) {
	sort.Slice(latency, func(i, j int) bool {
		return latency[i] < latency[j]
	})

	sum := int64(0)
	sz := len(latency)
	for i := 0; i < sz; i++ {
		sum += latency[i]
	}

	avg := sum / int64(sz)
	fmt.Printf("Avg %s latency: %d us\n", prefix, avg)
}

func main() {
	flag.Parse()

	err := ReadFilesSequentially(*fileCount)
	if err != nil {
		fmt.Println(os.Stderr, err)
		os.Exit(1)
	} else {
		// Print the stats
		Report(totalReadLatency, "total read")
		Report(openLatency, "open-file")
		Report(readLatency, "read-file")
		Report(closeLatency, "close-file")
	}
}
