package main

import (
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
)

const chunkSize = 1024 // Size of the chunk to read

func ReadFile(startOffset int64, file *os.File) {
	// Create a buffer to hold the chunk data
	buffer := make([]byte, chunkSize)

	_, err := file.ReadAt(buffer, startOffset)
	if err != nil && err != io.EOF {
		fmt.Println("Error reading chunk:", err)
		return
	} else {
		if err == io.EOF {
			fmt.Println("read completed with Error: ", err)
		} else {
			fmt.Println("content: ", string(buffer[:]))
			fmt.Println("read completed, successfully with startOffset: ", startOffset)
		}
	}
}

func main() {
	file, err := os.OpenFile("/usr/local/google/home/princer/gcs/ssd_pd_1_epoch_time.log", os.O_RDWR|syscall.O_DIRECT, 0666)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		index := i
		go func() {
			defer wg.Done()
			index := int64(index * chunkSize)
			ReadFile(index, file)
		}()
	}

	// Wait for workers to finish
	wg.Wait()
}
