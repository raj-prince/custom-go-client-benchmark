
// Sample storage-quickstart creates a Google Cloud Storage bucket using
// gRPC API.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/storage"
)

func main() {
	ctx := context.Background()

	// Use your Google Cloud Platform project ID and Cloud Storage bucket
	projectID := "gcs-fuse-test"
	bucketName := "princer-grpc-read-test-uc1a"

	// Creates a gRPC enabled client.
	client, err := storage.NewGRPCClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Creates the new bucket.
	if err := client.Bucket(bucketName).Create(ctx, projectID, nil); err != nil {
		log.Printf("Failed to create bucket: %v", err)
	}
	
	log.Printf("Waiting for 5 minutes...")
	time.Sleep(5 * time.Minute)

	fmt.Printf("Bucket %v created.\n", bucketName)	
}