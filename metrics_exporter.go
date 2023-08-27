package main

import (
	"fmt"
	"log"
	"time"

	"contrib.go.opencensus.io/exporter/stackdriver"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
)

var (
	// The restaurant rating in number of stars.
	readLatency = stats.Int64("readLatency", "Complete read latency", "ms")
)

var sdExporter *stackdriver.Exporter

func registerLatencyView() {
	v := &view.View{
		Name:        "read_latency",
		Measure:     readLatency,
		Description: "Complete read latency for a given go-client",
		Aggregation: view.Sum(),
	}

	if err := view.Register(v); err != nil {
		log.Fatalf("Failed to register the readLatency view: %v", err)
	}
}

func enableSDExporter() (err error) {
	sdExporter, err := stackdriver.NewExporter(stackdriver.Options{
		// ProjectID <change this value>
		ProjectID: "gcs-fuse-test-ml",
		// MetricPrefix helps uniquely identify your metrics. <change this value>
		MetricPrefix: "custom-go-client",
		// ReportingInterval sets the frequency of reporting metrics
		// to the Cloud Monitoring backend.
		ReportingInterval: 30 * time.Second,
	})

	if err != nil {
		err = fmt.Errorf("while creating stackdriver exporter: %w", err)
		return
	}

	if err = sdExporter.StartMetricsExporter(); err != nil {
		return fmt.Errorf("start stackdriver exporter: %w", err)
	}

	fmt.Println("Stack driver agent started successfully!!")
	return nil
}

func closeSDExporter() {
	if sdExporter != nil {
		sdExporter.StopMetricsExporter()
		sdExporter.Flush()
	}

	sdExporter = nil
}
