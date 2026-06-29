package main

import (
	"fmt"
	"log"
	"time"

	"contrib.go.opencensus.io/exporter/stackdriver"
	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	// The restaurant rating in number of stars.
	readLatency = stats.Float64("readLatency", "Complete read latency", stats.UnitMilliseconds)
	firstByteReadLatency = stats.Float64("firstByteReadLatency", "First byte read latency", stats.UnitMilliseconds)
	allocSize = stats.Int64("allocSize", "Buffer pool allocation size", stats.UnitBytes)
)

var sdExporter *stackdriver.Exporter

func registerLatencyView() {
	v := &view.View{
		Name:        "princer_go_client_read_latency",
		Measure:     readLatency,
		Description: "Complete read latency for a given go-client",
		TagKeys:     []tag.Key{tag.MustNewKey("princer_read_latency")},
		Aggregation: ochttp.DefaultLatencyDistribution,
	}

	if err := view.Register(v); err != nil {
		log.Fatalf("Failed to register the readLatency view: %v", err)
	}
}
func registerFirstByteLatencyView() {
	v := &view.View{
		Name:        "princer_go_client_first_byte_read_latency",
		Measure:     firstByteReadLatency,
		Description: "First byte read latency for a given go-client",
		TagKeys:     []tag.Key{tag.MustNewKey("princer_first_byte_read_latency")},
		Aggregation: ochttp.DefaultLatencyDistribution,
	}

	if err := view.Register(v); err != nil {
		log.Fatalf("Failed to register the firstByteReadLatency view: %v", err)
	}
}
func registerAllocSizeView() {
	bounds := []float64{
		4 * 1024,
		16 * 1024,
		64 * 1024,
		256 * 1024,
		512 * 1024,
		1 * 1024 * 1024,
		2 * 1024 * 1024,
		4 * 1024 * 1024,
		8 * 1024 * 1024,
		16 * 1024 * 1024,
	}
	v := &view.View{
		Name:        "princer_go_client_alloc_size",
		Measure:     allocSize,
		Description: "Buffer pool allocation sizes",
		TagKeys:     []tag.Key{tag.MustNewKey("princer_alloc_size")},
		Aggregation: view.Distribution(bounds...),
	}

	if err := view.Register(v); err != nil {
		log.Fatalf("Failed to register the allocSize view: %v", err)
	}
}

func enableSDExporter() (err error) {
	sdExporter, err := stackdriver.NewExporter(stackdriver.Options{
		// ProjectID <change this value>
		ProjectID: *ProjectName,
		// MetricPrefix helps uniquely identify your metrics. <change this value>
		MetricPrefix: "custom.googleapis.com/custom-go-client/",
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
