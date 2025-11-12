package rapid

import "io"

// Block represents a contiguous block of data to be downloaded.
type Block struct {
	Offset int64 // Starting byte offset in the object
	Length int64 // Number of bytes to read
}

// DownloadTask represents a task that downloads a block using an MRD pool
// and writes the result to an io.Writer. It implements the workerpool.Task interface.
type DownloadTask struct {
	block    Block
	pool     *MRDPool
	writer   io.Writer
	callback func(int64, int64, error)
}

// NewDownloadTask creates a new download task that can be scheduled to a worker pool.
func NewDownloadTask(block Block, pool *MRDPool, writer io.Writer, callback func(int64, int64, error)) *DownloadTask {
	return &DownloadTask{
		block:    block,
		pool:     pool,
		writer:   writer,
		callback: callback,
	}
}

// Execute implements the workerpool.Task interface.
// It schedules the download of the block using the MRD pool.
func (dt *DownloadTask) Execute() {
	err := dt.pool.Add(dt.writer, dt.block.Offset, dt.block.Length, dt.callback)
	if err != nil && dt.callback != nil {
		// If Add() fails, invoke the callback with the error
		dt.callback(dt.block.Offset, dt.block.Length, err)
	}
}
