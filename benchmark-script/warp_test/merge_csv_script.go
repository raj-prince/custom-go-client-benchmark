package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

type DataRow struct {
	Timestamp    int64
	ReadLatency  float64
	Throughput   float64
}

func main() {
	// Define the folder containing the CSV files
	folder := "/usr/local/google/home/princer/csv/metrics"
	
	// Store all data rows from all files
	var allDataRows []DataRow

	cmnStartTime := int64(0)
	cmnEndTime := int64(math.MaxInt64)

	// Iterate over all CSV files in the folder
	err := filepath.Walk(folder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == ".csv" {
			dataRows, err := readCSV(path)
			n := len(dataRows)
			if n == 0 {
				return fmt.Errorf("empty metrics-file")
			}

			// Get the time interval when all the nodes are active.
			if cmnStartTime < dataRows[0].Timestamp {
				cmnStartTime = dataRows[0].Timestamp
			}
			if cmnEndTime > dataRows[n - 1].Timestamp {
				cmnEndTime = dataRows[n - 1].Timestamp
			}

			if err != nil {
				return err
			}
			allDataRows = append(allDataRows, dataRows...)
		}
		return nil
	})

	fmt.Println(cmnStartTime)
	fmt.Println(cmnEndTime)
	if err != nil {
		fmt.Printf("Error reading CSV files: %v\n", err)
		return
	}

	// Sort all data rows by Timestamp
	sort.Slice(allDataRows, func(i, j int) bool {
		return allDataRows[i].Timestamp < allDataRows[j].Timestamp
	})

	// Filter out the data which is not part of common time interval.
	var tmpFilteredRows []DataRow
	var filteredCnt int
	for _, data := range allDataRows {
		if data.Timestamp < cmnStartTime || data.Timestamp > cmnEndTime {
			filteredCnt++
			continue
		}
		tmpFilteredRows = append(tmpFilteredRows, data)
	}
	allDataRows = tmpFilteredRows

	fmt.Printf("total filtered: %v\n", filteredCnt)
	printPercentiles(allDataRows)

	// Write the filtered data to a new CSV file
	outputFile := "/usr/local/google/home/princer/csv/output/output.csv"
	err = writeCSV(outputFile, allDataRows)
	fmt.Printf("Len: %d", len(allDataRows))
	if err != nil {
		fmt.Printf("Error writing CSV file: %v\n", err)
		return
	}

	fmt.Printf("Merged and filtered data written to %s\n", outputFile)
}

// readCSV reads a CSV file and returns a slice of DataRow
func readCSV(path string) ([]DataRow, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Skip the header row
	_, err = reader.Read()
	if err != nil {
		return nil, err
	}

	var dataRows []DataRow
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}
		timestamp, _ := strconv.ParseInt(record[0], 10, 64)
		readLatency, _ := strconv.ParseFloat(record[1], 64)
		throughput, _ := strconv.ParseFloat(record[2], 64)

		dataRows = append(dataRows, DataRow{
			Timestamp:   timestamp,
			ReadLatency: readLatency,
			Throughput:  throughput,
		})
	}

	return dataRows, nil
}

// writeCSV writes a slice of DataRow to a CSV file
func writeCSV(path string, data []DataRow) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header row
	err = writer.Write([]string{"Timestamp", "ReadLatency(s)", "Throughput(MiB/s)"})
	if err != nil {
		return err
	}

	// Write data rows
	for _, row := range data {
		err := writer.Write([]string{
			strconv.FormatInt(row.Timestamp, 10),
			strconv.FormatFloat(row.ReadLatency, 'f', 3, 64),
			strconv.FormatFloat(row.Throughput, 'f', 3, 64),
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func printPercentiles(rows []DataRow) {
	// Calculate averages
	var totalLatency float64
	for _, metric := range rows {
		totalLatency += metric.ReadLatency
	}
	avgLatency := totalLatency / float64(len(rows))

	// Calculate standard deviation
	var squaredDiffSum float64
	for _, metric := range rows {
		squaredDiff := math.Pow(metric.ReadLatency-avgLatency, 2)
		squaredDiffSum += squaredDiff
	}

	// Calculate the variance
	variance := squaredDiffSum / float64(len(rows))

	// Calculate the standard deviation
	standardDeviation := math.Sqrt(variance)

	// Calculate percentiles (e.g., 50th, 90th, 95th, 99th)
	latencyValues := make([]float64, len(rows))
	for i, metric := range rows {
		latencyValues[i] = metric.ReadLatency
	}

	sort.Float64s(latencyValues)

	fmt.Println("\n******* Metrics Summary: ReadLatency (s) *******")
	fmt.Printf("Average Latency: %.2f\n", avgLatency)
	fmt.Printf("Count: %d\n", len(rows))
	fmt.Printf("Standard deviation: %.2f\n", standardDeviation)
	fmt.Printf("p0: %.2f\n", percentileFloat64(latencyValues, 0))
	fmt.Printf("p50: %.2f\n", percentileFloat64(latencyValues, 50))
	fmt.Printf("p90: %.2f\n", percentileFloat64(latencyValues, 90))
	fmt.Printf("p95: %.2f\n", percentileFloat64(latencyValues, 95))
	fmt.Printf("p99: %.2f\n", percentileFloat64(latencyValues, 99))
	fmt.Printf("p99.9: %.2f\n", percentileFloat64(latencyValues, 99.9))
	fmt.Printf("p99.99: %.2f\n", percentileFloat64(latencyValues, 99.99))
	fmt.Printf("p100: %.2f\n", percentileFloat64(latencyValues, 100))
}

func percentileFloat64(values []float64, p float32) float64 {
	if p < 0 || p > 100 {
		panic("Percentile must be between 1 and 100")
	}

	index := int((p / float32(100)) * float32(len(values)))
	if index == len(values) {
		index--
	}
	return values[index]
}

