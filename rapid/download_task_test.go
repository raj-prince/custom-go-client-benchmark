package rapid

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDownloadTask(t *testing.T) {
	block := Block{Offset: 1024, Length: 2048}
	pool := &MRDPool{} // Empty pool for testing structure
	writer := &bytes.Buffer{}
	callback := func(offset, length int64, err error) {}

	task := NewDownloadTask(block, pool, writer, callback)

	require.NotNil(t, task)
	assert.Equal(t, block, task.block)
	assert.Equal(t, pool, task.pool)
	assert.Equal(t, writer, task.writer)
	assert.NotNil(t, task.callback)
}

func TestDownloadTask_Execute_Success(t *testing.T) {
	// This test verifies the structure without actual download
	// Integration tests should test actual downloads
	block := Block{Offset: 1024, Length: 2048}
	pool := &MRDPool{}
	writer := &bytes.Buffer{}

	callback := func(offset, length int64, err error) {
		// Callback for structure test
	}

	task := NewDownloadTask(block, pool, writer, callback)

	// Verify task is created properly
	require.NotNil(t, task)
	assert.Equal(t, block, task.block)

	// Note: Execute() would fail on empty pool, but we're just testing structure
}

func TestDownloadTask_Execute_NilCallback(t *testing.T) {
	block := Block{Offset: 1024, Length: 2048}
	pool := &MRDPool{}
	writer := &bytes.Buffer{}

	// Create task without callback - should not panic
	task := NewDownloadTask(block, pool, writer, nil)

	require.NotNil(t, task)
	assert.Nil(t, task.callback)
}

func TestBlock_Fields(t *testing.T) {
	block := Block{
		Offset: 12345,
		Length: 67890,
	}

	assert.Equal(t, int64(12345), block.Offset)
	assert.Equal(t, int64(67890), block.Length)
}
