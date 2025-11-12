package rapid

import "io"

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
	err := dt.pool.Add(dt.writer, dt.downloadRange.Offset, dt.downloadRange.Length, dt.callback)
	if err != nil && dt.callback != nil {
		// If Add() fails, invoke the callback with the error
		dt.callback(dt.downloadRange.Offset, dt.downloadRange.Length, err)
	}
}
