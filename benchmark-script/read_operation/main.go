package main

import (
	"flag"
	"fmt"
	"io"
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

	fBlockSizeKB = flag.Int("block-size-kb", 1024, "Block size in KB")

	fFileSizeMB = flag.Int64("file-size-mb", 1024, "File size in MB")

	fileHandles []*os.File

	eG errgroup.Group

	OneKB = 1024

	fNumberOfRead = flag.Int("read-count", 1, "number of read iteration")

	fOutputDir = flag.String("output-dir", "", "Directory to dump the output")
	fFilePrefix = flag.String("file-prefix", "", "Prefix file")
)

var gResult *Result
func init() {
	gResult = &Result{}
}

func openFile(index int) (err error) {
	fileName := path.Join(*fDir, *fFilePrefix+strconv.Itoa(index))
	fileHandle, err := os.OpenFile(fileName, os.O_RDONLY|syscall.O_DIRECT, 0600)
	if err != nil {
		err = fmt.Errorf("while opening file: %w", err)
		return
	}
	fileHandles[index] = fileHandle
	return
}

// Expect file is already opened, otherwise throws error
func readAlreadyOpenedFile(index int) (err error) {
	b := make([]byte, *fBlockSizeKB*1024)
	for i := 0; i < *fNumberOfRead; i++ {
		readStart := time.Now()
		_, _ = fileHandles[index].Seek(0, 0)

		for {
			_, err = fileHandles[index].Read(b)
			if err != nil {
				if err == io.EOF {
					err = nil
				}
				break
			}
		}

		if err != nil {
			return fmt.Errorf("while reading and discarding content: %v", err)
		}

		readLatency := time.Since(readStart)

		throughput := float64(*fFileSizeMB) / readLatency.Seconds()
		gResult.Append(readLatency.Seconds(), throughput)
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

	if *fFileSizeMB <= 0 {
		err = fmt.Errorf("file size is not valid")
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

	groupError := eG.Wait()

	for i := 0; i < *fNumOfThreads; i++ {
		if err = fileHandles[i].Close(); err != nil {
			err = fmt.Errorf("while closing the fileHandle: %w", err)
			return
		}
	}

	if groupError != nil {
		err = groupError
	} else {
		fmt.Println("read benchmark completed successfully!")
	}
	return
}

func main() {
	flag.Parse()
	fmt.Println("\n******* Passed flags: *******")
	flag.VisitAll(func(f *flag.Flag) {
		fmt.Printf("Flag: %s, Value: %v\n", f.Name, f.Value)
	})

	err := runReadFileOperations()
	if err != nil {
		fmt.Println(os.Stderr, err)
		os.Exit(1)
	}
	if *fOutputDir == "" {
		*fOutputDir, _ = os.Getwd()
	}
	gResult.DumpMetricsCSV(path.Join(*fOutputDir, "metrics.csv"))
	gResult.PrintStats()

}
