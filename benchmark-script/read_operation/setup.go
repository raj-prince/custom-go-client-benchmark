// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"fmt"

	cloudmetric "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func metricFormatter(m metricdata.Metrics) string {
	return "custom.googleapis.com/gcsfuse-scale-test/" + strings.ReplaceAll(m.Name, ".", "/")
}

// setupOpenTelemetry sets up the OpenTelemetry SDK and exporters for metrics and
// traces. If it does not return an error, call shutdown for proper cleanup.
// [START opentelemetry_instrumentation_setup_opentelemetry]
func setupOpenTelemetry(ctx context.Context) (shutdown func(context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error

	// shutdown combines shutdown functions from multiple OpenTelemetry
	// components into a single function.
	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	options := []cloudmetric.Option{
		cloudmetric.WithMetricDescriptorTypeFormatter(metricFormatter),
		cloudmetric.WithFilteredResourceAttributes(func(kv attribute.KeyValue) bool {
			// Ensure that PID is available as a metric label on metrics explorer.
			return cloudmetric.DefaultResourceAttributesFilter(kv) ||
				kv.Key == semconv.ProcessPIDKey
		}),
		cloudmetric.WithProjectID("gcs-tess"),
	}

	exporter, err := cloudmetric.New(options...)
	if err != nil {
		fmt.Printf("Error while creating Google Cloud exporter:%v\n", err)
		return nil, nil
	}

	r := metric.NewPeriodicReader(exporter, metric.WithInterval(60 * time.Second))

	mp := metric.NewMeterProvider(
		metric.WithReader(r),
	)
	shutdownFuncs = append(shutdownFuncs, mp.Shutdown)
	otel.SetMeterProvider(mp)

	return shutdown, nil
}