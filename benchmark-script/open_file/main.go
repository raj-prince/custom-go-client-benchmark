package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"
	"syscall"
)

var (
fDir = flag.String("dir", "", "Directory file to be opened.")
fNumFiles = flag.Int("num_of_files", 1, "Number of files in the directory")
fileHandles []*os.File

var wg sync.WaitGroup
)





func openfile(i int, wg *sync.WaitGroup) []*os.File {
	   var file []*os.File
	   fileName := path.Join("gcs", "test"+strconv.Itoa(i))
	   f, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|syscall.O_DIRECT, 0600)
	   if err != nil {
		       fmt.Println("Open file for append: %v", err)
		   }
	   file = append(file, f)
	   wg.Done()

	   return file
}

func openFile(index int) (err error) {
	fileName := path.Join(*fDir, "file_" + strconv.Itoa(index))
	fileHandle, err := os.OpenFile(fileName, os.O_APPEND | os.O_WRONLY | syscall.O_DIRECT, 0600)
	if err != nil {
		err = fmt.Errorf("while opening file: %w", err)
		return
	}
	fileHandles[index] = fileHandle
	wg.Done()

	return
}

func runOpenFileOperations() (err error) {
	if *fDir == "" {
		err = fmt.Errorf("you must set --dir flag")
		return
	}

	if *fNumFiles <= 0 {
		err = fmt.Errorf("files count not valid")
		return
	}

	fileHandles = make([]*os.File, *fNumFiles)

	for i := 0; i < *fNumFiles; i++ {
		wg.Add(1)
		err = openFile(i)
		if err != nil {
			err = fmt.Errorf("while opening file: %w", err)
			return err
		}

	}
}

func main() {

}