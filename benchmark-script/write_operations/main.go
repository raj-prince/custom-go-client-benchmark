package main

import (
	"crypto/rand"
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

	fBlockSize = flag.Int("block-size", 256, "Block size in KB")

	fileHandles []*os.File

	eG errgroup.Group

	// OneKB means 1024 bytes.
	OneKB = 1024

	fFileSize = flag.Int("file-size", 1, "in KB")

	fNumOfWrite = flag.Int("write-count", 1, "number of write iteration")
)

func openFile(index int) (err error) {
	fileName := path.Join(*fDir, "file_"+strconv.Itoa(index))
	fileHandle, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|syscall.O_DIRECT, 0644)
	if err != nil {
		err = fmt.Errorf("while opening file: %w", err)
		return
	}
	fileHandles[index] = fileHandle
	return
}

// Expect file is already opened, otherwise throws error
func overWriteAlreadyOpenedFile(index int) (err error) {
	for cnt := 0; cnt < *fNumOfWrite; cnt++ {
		for i := 0; i < (*fFileSize / *fBlockSize); i++ {
			b := make([]byte, *fBlockSize*OneKB)

			startByte := int64(i * (*fBlockSize * OneKB))

			_, err = rand.Read(b)
			if err != nil {
				return fmt.Errorf("while generating random bytest: %v", err)
			}

			_, err = fileHandles[index].Seek(startByte, io.SeekStart)
			if err != nil {
				return fmt.Errorf("while changing the seek position")
			}

			_, err = fileHandles[index].Write(b)
			if err != nil {
				return fmt.Errorf("while overwriting the file: %v", err)
			}

			err = fileHandles[index].Sync()
			if err != nil {
				return fmt.Errorf("while syncing the file: %v", err)
			}
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

	if err == nil {
		fmt.Println("write benchmark completed successfully!")
		fmt.Println("Waiting for 3 minutes")

		time.Sleep(3 * time.Minute)
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
		fmt.Println(err)
		os.Exit(1)
	}

}
