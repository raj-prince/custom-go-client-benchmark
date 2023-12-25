package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	fDir          = flag.String("dir", "", "Directory file to be opened.")
	fNumOfThreads = flag.Int("threads", 1, "Number of threads to read parallel")

	fBlockSize = flag.Int("block-size", 256, "Block size in KB")

	fFileSize = flag.Int64("file-size", 256, "File size in KB")

	fReadType = flag.String("read-type", "seq", "Read access pattern")

	fileHandles []*os.File

	eG errgroup.Group

	OneKB = 1024

	fNumberOfRead = flag.Int("read-count", 1, "number of read iteration")
)

func openFile(index int) (err error) {
	fileName := path.Join(*fDir, "file_"+strconv.Itoa(index))
	fileHandle, err := os.OpenFile(fileName, os.O_RDONLY|syscall.O_DIRECT, 0600)
	if err != nil {
		err = fmt.Errorf("while opening file: %w", err)
		return
	}

	fileInfo, err := fileHandle.Stat()
	if err != nil {
		err = fmt.Errorf("bad fileHandle: %w", err)
		return err
	}

	size := fileInfo.Size()
	if size != *fFileSize*1024 {
		err = fmt.Errorf("file present is not equal to given file-size")
		return err
	}

	fileHandles[index] = fileHandle
	return
}

// Expect file is already opened, otherwise throws error
func readAlreadyOpenedFile(threadIndex int, accessPat []int) (err error) {
	for i := 0; i < *fNumberOfRead; i++ {
		r := bufio.NewReader(fileHandles[threadIndex])
		b := make([]byte, *fBlockSize*1024)

		_, err = io.CopyBuffer(io.Discard, r, b)
		if err != nil {
			return fmt.Errorf("while reading and discarding content: %v", err)
		}
	}

	return
}

func runReadFileOperations() (err error) {
	if *fDir == "" {
		err = fmt.Errorf("you must set --dir flag")
		return
	}

	if *fNumOfThreads <= 0 {
		err = fmt.Errorf("threads count not valid")
		return
	}

	fileHandles = make([]*os.File, *fNumOfThreads)

	for i := 0; i < *fNumOfThreads; i++ {
		err = openFile(i)
		if err != nil {
			err = fmt.Errorf("while opening file: %w", err)
			return err
		}
	}

	totalIOPerThread := *fFileSize / int64(*fBlockSize)
	remainingIO := *fFileSize % int64(*fBlockSize)
	if remainingIO != 0 {
		return fmt.Errorf("block-size should be multiple of file-size")
	}

	offset := make([]int, totalIOPerThread)
	if *fReadType == "seq" {
		for i := 0; i < int(totalIOPerThread); i++ {
			offset[i] = i
		}
	} else { // random read
		for i := range offset {
			j := rand.Intn(i + 1)
			offset[i], offset[j] = offset[j], offset[i]
		}
	}

	for i := 0; i < *fNumOfThreads; i++ {
		index := i
		eG.Go(func() error {
			err := readAlreadyOpenedFile(index)
			if err != nil {
				err = fmt.Errorf("while reading file: %w", err)
				return err
			}
			return err
		})
	}

	err = eG.Wait()

	if err == nil {
		fmt.Println("read benchmark completed successfully!")
		fmt.Println("Waiting for 10 seconds")

		time.Sleep(10 * time.Second)
	}

	for i := 0; i < *fNumOfThreads; i++ {
		if err = fileHandles[i].Close(); err != nil {
			err = fmt.Errorf("while closing the fileHandle: %w", err)
			return
		}
	}

	return
}

func main() {
	flag.Parse()

	err := runReadFileOperations()
	if err != nil {
		fmt.Println(os.Stderr, err)
		os.Exit(1)
	}

}
