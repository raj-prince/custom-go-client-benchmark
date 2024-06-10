package main

import (
	"bufio"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"cloud.google.com/go/storage/transfermanager"
	"github.com/googleapis/gax-go/v2"
	"golang.org/x/sync/errgroup"
)

type Downloader struct {
	attrs  *storage.ObjectAttrs
	bucketHandle *storage.BucketHandle
	eG     errgroup.Group
}

var globalDownloader *Downloader
var globalTmDownloader *transfermanager.Downloader

const (
MaxRetryDuration = 30 * time.Second

RetryMultiplier = 2.0

MiB = 1024 * 1024

ObjectName = "Workload.18"
BucketName = "princer-empty-bucket"

ThreadPoolSize = 50
RequestSize = 50 * MiB
Parallelism = 32
)

func init() {
	globalDownloader = NewDownloader(BucketName, ObjectName)
	globalTmDownloader = NewTMDownloader(BucketName, ObjectName)
}

func NewTMDownloader(bucketName string, objectName string) *transfermanager.Downloader {
	client, err := CreateHttpClient(context.Background(), false)
	downloader, err := transfermanager.NewDownloader(client,
		transfermanager.WithWorkers(ThreadPoolSize), transfermanager.WithCallbacks())
		if err != nil {
			log.Fatalf("error in creation downloader")
			downloader = nil
		}
		return downloader
}

func NewDownloader(bucketName string, objectName string) *Downloader {
	client, err := CreateHttpClient(context.Background(), false)

	if err != nil {
		fmt.Errorf("while creating the client: %v", err)
	}

	client.SetRetry(
		storage.WithBackoff(gax.Backoff{
			Max:        MaxRetryDuration,
			Multiplier: RetryMultiplier,
		}),
		storage.WithPolicy(storage.RetryAlways))

	// assumes bucket already exist
	err = client.Bucket(bucketName).Create(context.Background(), "retry-test", &storage.BucketAttrs{Name: bucketName})
	bucketHandle := client.Bucket(bucketName)

	bucketHandle.Object(objectName)
	attrs, err := bucketHandle.Object(objectName).Attrs(context.Background())
	if err != nil {
		log.Fatalf("Failure in fetching object attributes: %v", err)
	}

	return &Downloader{
		attrs:  attrs,
		bucketHandle: bucketHandle,
	}
}

func (d *Downloader) SingleThreadFullFileDownload(fileName string) (err error) {
	return d.rangeDownload(fileName, 0, uint64(d.attrs.Size))
}

func (d *Downloader) MultiThreadFullFileDownload(fileName string) (err error) {
	start := uint64(0)
	end := uint64(d.attrs.Size)
	log.Printf("Start: %d", end)
	batch := 1
	for start < end {
		limit := start + RequestSize * Parallelism
		log.Printf("Batch No.: %d", batch)
		batch++

		if limit > end {
			limit = end
		}

		log.Printf("Start: %d", start)
		log.Printf("End: %d", limit)

		err = d.multiThreadRangeDownload(fileName, start, limit - start)
		if err != nil {
			log.Fatalf("while parallely downloading: %d to %d", start, end)
		} else {
			start = limit
		}
	}
	log.Printf("Successfully completed: download")
	return
}

func (d *Downloader) MultiThreadFullFileDownloadWithTm(fileName string) (err error) {
	start := uint64(0)
	end := uint64(d.attrs.Size)
	log.Printf("Start: %d", end)
	batch := 1
	for start < end {
		limit := start + RequestSize * Parallelism
		log.Printf("Batch No.: %d", batch)
		batch++

		if limit > end {
			limit = end
		}

		log.Printf("Start: %d", start)
		log.Printf("End: %d", limit)

		err = d.multiThreadRangeDownloadWithTm(fileName, start, limit - start)
		if err != nil {
			log.Fatalf("while parallely downloading: %d to %d", start, end)
		} else {
			start = limit
		}
	}
	log.Printf("Successfully completed: download")
	return
}


func (d *Downloader) IncrementalMultiThreadFullFileDownload(fileName string) (err error) {
	start := uint64(0)
	multiplier := uint64(8)
	downloadSize := uint64(8 * MiB)
	end := uint64(d.attrs.Size)

	for start < end {
		availableEnd := min(start+downloadSize, end)
		downloadSize = availableEnd - start
		fmt.Printf("Downloading %d MiB \n", downloadSize/MiB)
		err = d.multiThreadRangeDownload(fileName, start, downloadSize)
		if err != nil {
			err = fmt.Errorf("while incremental download: %d to %d", start, availableEnd)
			return
		}

		start += downloadSize
		downloadSize *= multiplier
	}
	return nil
}

func (d *Downloader) multiThreadRangeDownload(fileName string, offset uint64, len uint64) (err error) {
	end := offset + len
	for s := offset; s < end; s += RequestSize {
		ss := s
		ee := min(end, s+50*MiB)
		d.eG.Go(func() error {
			errS := d.rangeDownload(fileName, ss, ee-ss)
			if errS != nil {
				errS = fmt.Errorf("error in downloading: %d to %d: %w", ss, ee, errS)
				return errS
			}
			return nil
		})
	}
	err = d.eG.Wait()
	if err != nil {
		return fmt.Errorf("error while parallel download: %w", err)
	}
	return nil
}

func (d *Downloader) multiThreadRangeDownloadWithTm(fileName string, offset uint64, len uint64) (err error) {
	f, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			err = fmt.Errorf("error in closing the fileHandle")
		}
	}(f)

	end := offset + len

	wg := sync.WaitGroup{}

	callback := func(out *transfermanager.DownloadOutput) {
		wg.Done()
		if out.Err != nil {
			log.Fatalf("parallel download failed: %v", out.Err)
		} else {
			log.Printf("download succeeded at offset: %d", out.Attrs.StartOffset)
		}
	}


	for s := offset; s < end; s += RequestSize {
		ss := s
		ee := min(end, s+RequestSize)
		w := io.NewOffsetWriter(f, int64(ss))
		//log.Printf("Start: %d, End: %d", ss, int64(ee - ss))

		in := &transfermanager.DownloadObjectInput{
			Bucket:      BucketName,
			Object:      ObjectName,
			Destination: w,
			Range: &transfermanager.DownloadRange{
				Offset: int64(ss),
				Length: int64(ee - ss),
			},
			Callback: callback,
		}
		wg.Add(1)
		err := globalTmDownloader.DownloadObject(context.Background(), in)
		if err != nil {
			log.Fatalf("Issue in downloading via tm: %v", err)
		}
	}

	wg.Wait()
	return nil
}

func (d *Downloader) rangeDownload(fileName string, offset uint64, len uint64) (err error) {
	f, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			err = fmt.Errorf("error in closing the fileHandle")
		}
	}(f)

	_, err = f.Seek(int64(offset), 0)

	object := d.bucketHandle.Object(ObjectName)
	rc, err := object.NewRangeReader(context.Background(), int64(offset), int64(len))

	copiedData, err := io.Copy(f, rc)
	if copiedData != int64(len) || (err != nil && err != io.EOF) {
		err = fmt.Errorf("error while downloading")
		return
	}

	return nil
}

const bufferSize = 65536

// CRCReader returns CRC-32-Castagnoli checksum of content in reader
func CRCReader(reader io.Reader) (uint32, error) {
	table := crc32.MakeTable(crc32.Castagnoli)
	checksum := crc32.Checksum([]byte(""), table)
	buf := make([]byte, bufferSize)
	for {
		switch n, err := reader.Read(buf); err {
		case nil:
			checksum = crc32.Update(checksum, table, buf[:n])
		case io.EOF:
			return checksum, nil
		default:
			return 0, err
		}
	}
}

func CRC32(filename string) (uint32, error) {
	if info, err := os.Stat(filename); err != nil || info.IsDir() {
		return 0, err
	}

	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer func() { _ = file.Close() }()

	return CRCReader(bufio.NewReader(file))
}

func main() {
	startTime := time.Now()
	fileName := "parallel_download.txt"
	//err := globalDownloader.SingleThreadFullFileDownload("single_download.txt")
	err := globalDownloader.MultiThreadFullFileDownload(fileName)
	//err := globalDownloader.MultiThreadFullFileDownloadWithTm(fileName)
	//err := globalDownloader.IncrementalMultiThreadFullFileDownload("incremental_download.txt")
	if err != nil {
		fmt.Printf("error while downloaing file")
	}

	totalTime := time.Since(startTime)

	fmt.Println("Total time to download file: ", totalTime)
	startTime = time.Now()

	crc32, err := CRC32(fileName)
	fmt.Println("Downloaded crc32: ", crc32)
	fmt.Println("Actual crc32: ", globalDownloader.attrs.CRC32C)
	totalCrcCalculationTime := time.Since(startTime)
	fmt.Println("CRC-32 calculation time: ", totalCrcCalculationTime)
}