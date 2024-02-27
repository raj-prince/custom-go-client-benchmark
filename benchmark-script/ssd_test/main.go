package main

import (
	"flag"
	"fmt"
	"golang.org/x/sync/errgroup"
	"os"
	"path"
	"strconv"
	"syscall"
)

var (
	fDir = flag.String("dir", "", "Directory file to be opened.")

	fNumOfThreads = flag.Int("threads", 1, "Number of threads to read parallel")

	fBlockSize = flag.Int("block-size", 512, "Block size in KB")

	fFileSize = flag.Int64("file-size", 524288, "File size in KB")

	fReadType = flag.String("read-type", "seq", "Read access pattern")

	fileHandles []*os.File

	eG errgroup.Group

	OneKB = 1024

	fNumberOfRead = flag.Int("read-count", 1, "number of read iteration")

	readTime []int64
)

func openFile(index int) (err error) {
	fileName := path.Join(*fDir, "Workload."+strconv.Itoa(index)+"/0")
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
func statFile(threadIndex int) (err error) {
	for i := 0; i < 100; i++ {
		fileName := path.Join(*fDir, "Workload.0"+"/"+strconv.Itoa(i))
		_, err = os.Stat(fileName)
		if err != nil && os.IsNotExist(err) {
			return
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

	for i := 0; i < *fNumOfThreads; i++ {
		index := i
		eG.Go(func() error {
			err := statFile(index)
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
	}

	return
}

func MicroSecondsToMilliSecond(microSecond int64) float64 {
	return 0.001 * float64(microSecond)
}

func main() {
	flag.Parse()

	err := runReadFileOperations()
	if err != nil {
		fmt.Println(os.Stderr, err)
		os.Exit(1)
	}

}
