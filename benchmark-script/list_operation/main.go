package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"time"
)

var fDir = flag.String("dir", "", "Directory within which listing performed.")
var fGoList = flag.Bool("go-list", false, "Directory within which listing performed using go-list.")

// This doesn't match with
func runListingGoScript() (err error) {
	if *fDir == "" {
		err = fmt.Errorf("you must set --dir flag")
		return
	}

	files, err := os.ReadDir(*fDir)
	if err != nil {
		err = fmt.Errorf("error while readir call: %w", err)
		return
	}

	for _, file := range files {
		fileInfo, infoError := file.Info()
		if infoError != nil {
			err = fmt.Errorf("while fetching fileInfo: %w", infoError)
			return
		}
		fmt.Println(file.Name(), fileInfo.Size())
	}

	return
}

// On check the behaviour on GCP machine.
// On cloud-top machine (ls and ls through go-command are different).
// ls in the cloud-top terminal creates more logs.
func runListingCommandLine() (err error) {
	if *fDir == "" {
		err = fmt.Errorf("you must set --dir flag")
		return
	}

	app := "ls"

	cmd := exec.Command(app, "-lah", *fDir)
	stdout, err := cmd.Output()

	if err != nil {
		err = fmt.Errorf("error while executing list command")
		return
	}

	fmt.Println("Listing completed...")
	fmt.Println("Waiting for 3 minutes")

	time.Sleep(3 * time.Minute)

	// Print the output
	fmt.Println(string(stdout))

	return
}

func main() {
	flag.Parse()

	var err error
	if *fGoList {
		err = runListingGoScript()
	} else {
		err = runListingCommandLine()
	}
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

}
