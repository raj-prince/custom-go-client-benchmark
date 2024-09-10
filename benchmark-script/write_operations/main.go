package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	fPrefix       = flag.String("prefix", "", "prefix for each file.")
	fDir          = flag.String("dir", "", "Directory file to be opened.")
	fNumOfThreads = flag.Int("threads", 1, "Number of threads to read parallel")

	fBlockSize = flag.Int("block-size", 256, "Block size in KB")

	fileHandles []*os.File

	eG errgroup.Group

	OneKB = 1024

	fFileSize = flag.Int("file-size", 1, "in KB")

	fNumOfWrite = flag.Int("write-count", 1, "number of write iteration")
	fOutputDir  = flag.String("output-dir", "", "Directory to dump the output")
)

var gResult *Result

func init() {
	gResult = &Result{}
}

func openFile(fileSuffix string) (fileHandle *os.File, err error) {
	fileName := path.Join(*fDir, *fPrefix+"_file_"+fileSuffix)
	fileHandle, err = os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC|syscall.O_DIRECT, 0644)
	if err != nil {
		err = fmt.Errorf("while opening file: %w", err)
		return
	}

	return
}

// Expect file is already opened, otherwise throws error
func overWriteAlreadyOpenedFile(index int) (err error) {
	b := make([]byte, *fBlockSize*OneKB)
	_, err = rand.Read(b)

	for cnt := 0; cnt < *fNumOfWrite; cnt++ {
		writeStart := time.Now()
		fileName := strconv.Itoa(index) + "__" + strconv.Itoa(cnt)
		fh, err := openFile(fileName)
		if err != nil {
			return fmt.Errorf("Error while creating the file %v", err)
		}

		for i := 0; i < (*fFileSize / *fBlockSize); i++ {
			if err != nil {
				return fmt.Errorf("while generating random bytest: %v", err)
			}

			_, err = fh.Write(b)
			if err != nil {
				return fmt.Errorf("while overwriting the file: %v", err)
			}
		}

		err = fh.Close()
		if err != nil {
			return fmt.Errorf("while closing the file: %v", err)
		}

		writeLatency := time.Since(writeStart)

		throughput := float64(*fFileSize) / writeLatency.Seconds()
		gResult.Append(writeLatency.Seconds(), throughput)
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
			err := overWriteAlreadyOpenedFile(index)
			if err != nil {
				err = fmt.Errorf("while reading file: %w", err)
				return err
			}
			return err
		})
	}

	err = eG.Wait()

	if err != nil {
		return err
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
