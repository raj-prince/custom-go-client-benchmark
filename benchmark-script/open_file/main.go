package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"
	"syscall"
	"time"
)

var (
	fDir        = flag.String("dir", "", "Directory file to be opened.")
	fNumOfFiles = flag.Int("open-files", 1, "Number of files to open")

	fileHandles []*os.File
)

func openFile(index int) (err error) {
	fileName := path.Join(*fDir, "file_"+strconv.Itoa(index))
	fileHandle, err := os.OpenFile(fileName, os.O_RDONLY|syscall.O_DIRECT, 0600)
	if err != nil {
		err = fmt.Errorf("while opening file: %w", err)
		return
	}
	fileHandles[index] = fileHandle
	return
}

func runOpenFileOperations() (err error) {
	if *fDir == "" {
		err = fmt.Errorf("you must set --dir flag")
		return
	}

	if *fNumOfFiles <= 0 {
		err = fmt.Errorf("count not valid")
		return
	}

	fileHandles = make([]*os.File, *fNumOfFiles)

	for i := 0; i < *fNumOfFiles; i++ {
		err = openFile(i)
		if err != nil {
			err = fmt.Errorf("while opening file: %w", err)
			return err
		}
	}

	fmt.Println("All the files are opened now")
	fmt.Println("Waiting for 3 minutes")

	time.Sleep(3 * time.Minute)

	for i := 0; i < *fNumOfFiles; i++ {
		if err = fileHandles[i].Close(); err != nil {
			err = fmt.Errorf("while closing the fileHandle: %w", err)
			return
		}
	}

	return
}

func main() {
	flag.Parse()

	err := runOpenFileOperations()
	if err != nil {
		fmt.Println(os.Stderr, err)
		os.Exit(1)
	}

}
