package main

import (
	"encoding/csv"
	"fmt"
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
	folder := "/usr/local/google/home/lankita/projects/test-bucket/metrics"
	
	// Store all data rows from all files
	var allDataRows []DataRow

	// Iterate over all CSV files in the folder
	err := filepath.Walk(folder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == ".csv" {
			dataRows, err := readCSV(path)
			if err != nil {
				return err
			}
			allDataRows = append(allDataRows, dataRows...)
		}
		return nil
	})
	if err != nil {
		fmt.Printf("Error reading CSV files: %v\n", err)
		return
	}

	// Sort all data rows by Timestamp
	sort.Slice(allDataRows, func(i, j int) bool {
		return allDataRows[i].Timestamp < allDataRows[j].Timestamp
	})

	// Remove the first and last 50 rows
	if len(allDataRows) > 100 {
		allDataRows = allDataRows[50 : len(allDataRows)-50]
	} else {
		fmt.Println("Not enough data to filter first and last 5 rows")
		return
	}

	// Write the filtered data to a new CSV file
	outputFile := "/usr/local/google/home/lankita/projects/merged-and-filtered-data.csv"
	err = writeCSV(outputFile, allDataRows)
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


