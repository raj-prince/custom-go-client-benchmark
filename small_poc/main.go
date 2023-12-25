package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"syscall"
)

func main() {
	// Open the file.
	file, err := os.OpenFile("/usr/local/google/home/princer/gcs/a/b/c/d/e.txt", os.O_RDWR|syscall.O_DIRECT, os.FileMode(0644))
	if err != nil {
		fmt.Println(err)
		return
	}

	// Create a buffered reader for the file.
	reader := bufio.NewReader(file)

	// Read the file contents line by line.
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println(err)
			return
		}

		// Print the line to the console.
		fmt.Println(line)
	}

	// Close the file.
	file.Close()
}
