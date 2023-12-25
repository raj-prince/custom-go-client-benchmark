package main

import (
	"context"
	texporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	octrace "go.opencensus.io/trace"
	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/bridge/opencensus"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"log"
)

// enableTraceExport turns on Open Telemetry tracing with export to Cloud Trace.
func enableTraceExport(ctx context.Context, sampleRate float64) func() {
	exporter, err := texporter.New(texporter.WithProjectID(*ProjectName))
	if err != nil {
		log.Fatalf("texporter.New: %v", err)
	}

	// Identify your application using resource detection
	res, err := resource.New(ctx,
		// Use the GCP resource detector to detect information about the GCP platform
		resource.WithDetectors(gcp.NewDetector()),
		// Keep the default detectors
		resource.WithTelemetrySDK(),
		// Add your own custom attributes to identify your application
		resource.WithAttributes(
			semconv.ServiceName(tracerName),
			attribute.KeyValue{"transport", attribute.StringValue(*clientProtocol)},
		),
	)
	if err != nil {
		log.Fatalf("resource.New: %v", err)
	}

	// Create trace provider with the exporter.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(sampleRate)),
	)

	otel.SetTracerProvider(tp)

	// Use opencensus bridge to pick up OC traces from the storage library.
	// TODO: remove this when migration to OpenTelemetry is complete.
	tracer := otel.GetTracerProvider().Tracer(tracerName)
	octrace.DefaultTracer = opencensus.NewTracer(tracer)
	log.Printf("Cloud trace export enabled")

	return func() {
		tp.ForceFlush(ctx)
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Fatal(err)
		}
	}
}
