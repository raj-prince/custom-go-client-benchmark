package rapid

import (
	"io"
	"log"
)

type readResult struct {
	bytesRead int
	err       error
}

// Range represents a contiguous range of data to be downloaded.
type Range struct {
	Offset int64 // Starting byte offset in the object
	Length int64 // Number of bytes to read
}

// DownloadTask represents a task that downloads a range using an MRD pool
// and writes the result to an io.Writer. It implements the workerpool.Task interface.
type DownloadTask struct {
	downloadRange Range
	pool          *MRDPool
	writer        io.Writer
	callback      func(int64, int64, error)
}

// NewDownloadTask creates a new download task that can be scheduled to a worker pool.
func NewDownloadTask(downloadRange Range, pool *MRDPool, writer io.Writer, callback func(int64, int64, error)) *DownloadTask {
	return &DownloadTask{
		downloadRange: downloadRange,
		pool:          pool,
		writer:        writer,
		callback:      callback,
	}
}

// Execute implements the workerpool.Task interface.
// It schedules the download of the range using the MRD pool.
func (dt *DownloadTask) Execute() {
	done := make(chan readResult, 1)
	err := dt.pool.Add(dt.writer, dt.downloadRange.Offset, dt.downloadRange.Length,
		func(off, len int64, err error) {
			done <- readResult{int(len), err}
			if dt.callback != nil {
				dt.callback(off, int64(len), err)
			}
		})
	if err != nil {
		log.Printf("Failed to add download task to MRD pool: %v\n", err)
	}
	<-done // Ensure we wait for completion
}
