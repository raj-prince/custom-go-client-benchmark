package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"

	"golang.org/x/sync/errgroup"
)

var (
	fDir = flag.String("dir", "", "Directory file to be opened.")

	fNumOfThreads = flag.Int("threads", 1, "Number of threads to read parallel")

	eG errgroup.Group

	// OneKB means 1024 bytes.
	OneKB = 1024
)

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

func main() {
	flag.Parse()

	err := runReadFileOperations()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

}
