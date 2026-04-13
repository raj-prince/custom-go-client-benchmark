package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

type Metric struct {
	latency    float64
	throughput float64
	unixTime   int64
}

type Result struct {
	metrics []Metric // List to store metrics
	mutex   sync.Mutex
}

func (r *Result) Append(latency float64, throughput float64) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	newMetric := Metric{
		latency:    latency,
		throughput: throughput,
		unixTime:   time.Now().Unix(),
	}
	r.metrics = append(r.metrics, newMetric)
}

func (r *Result) GetMetrics() []Metric {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	metricsCopy := make([]Metric, len(r.metrics))
	copy(metricsCopy, r.metrics)
	return metricsCopy
}

func (r *Result) DumpMetricsJson(filePath string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	fmt.Print(r.metrics)

	// Marshal the metrics int64o JSON format
	jsonData, err := json.Marshal(r.metrics)
	if err != nil {
		return err
	}

	// Write the JSON data to the file
	err = os.WriteFile(filePath, jsonData, 0644)
	if err != nil {
		return err
	}

	return nil
}

func (r *Result) DumpMetricsCSV(filePath string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Create the CSV file
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the CSV header
	err = writer.Write([]string{"Timestamp", "ReadLatency(s)", "Throughput(MiB/s)"})
	if err != nil {
		return err
	}

	for _, metric := range r.metrics {
		err = writer.Write([]string{
			fmt.Sprintf("%d", metric.unixTime),
			fmt.Sprintf("%.3f", metric.latency),
			fmt.Sprintf("%.3f", metric.throughput),
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Result) PrintStats() {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if len(r.metrics) == 0 {
		fmt.Println("No metrics collected yet.")
		return
	}

	// Calculate averages
	var totalLatency, totalThroughput float64
	for _, metric := range r.metrics {
		totalLatency += metric.latency
		totalThroughput += metric.throughput
	}
	avgLatency := totalLatency / float64(len(r.metrics))
	avgThroughput := totalThroughput / float64(len(r.metrics))

	// Calculate percentiles (e.g., 50th, 90th, 95th, 99th)
	latencyValues := make([]float64, len(r.metrics))
	throughputValues := make([]float64, len(r.metrics))
	for i, metric := range r.metrics {
		latencyValues[i] = metric.latency
		throughputValues[i] = metric.throughput
	}

	sort.Float64s(latencyValues)
	sort.Float64s(throughputValues)

	fmt.Println("\n******* Metrics Summary: WriteLatency (s) *******")
	fmt.Printf("Average Latency: %.2f\n", avgLatency)
	fmt.Printf("p0: %.2f\n", percentileFloat64(latencyValues, 0))
	fmt.Printf("p50: %.2f\n", percentileFloat64(latencyValues, 50))
	fmt.Printf("p90: %.2f\n", percentileFloat64(latencyValues, 90))
	fmt.Printf("p95: %.2f\n", percentileFloat64(latencyValues, 95))
	fmt.Printf("p99: %.2f\n", percentileFloat64(latencyValues, 99))
	fmt.Printf("p100: %.2f\n", percentileFloat64(latencyValues, 100))

	fmt.Println("\n******* Metrics Summary: Throughput (MiB/s): *******")
	fmt.Printf("Average Throughput: %.2f\n", avgThroughput)
	fmt.Printf("p0: %.2f\n", percentileFloat64(throughputValues, 0))
	fmt.Printf("p50: %.2f\n", percentileFloat64(throughputValues, 50))
	fmt.Printf("p90: %.2f\n", percentileFloat64(throughputValues, 90))
	fmt.Printf("p95: %.2f\n", percentileFloat64(throughputValues, 95))
	fmt.Printf("p99: %.2f\n", percentileFloat64(throughputValues, 99))
	fmt.Printf("p100: %.2f\n", percentileFloat64(throughputValues, 100))
}

func percentileFloat64(values []float64, p int) float64 {
	if p < 0 || p > 100 {
		panic("Percentile must be between 1 and 100")
	}

	index := int((float32(p) / float32(100)) * float32(len(values)))
	if index == len(values) {
		index--
	}
	return values[index]
}
