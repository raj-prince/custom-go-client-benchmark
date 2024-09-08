package main

import (
	"encoding/csv"
	"fmt"
	"image/color"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/raj-prince/custom-go-client-benchmark/util"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

func applySamples(p *plot.Plot, numSamples int, expectedValue float64, rnd *rand.Rand, d *util.Delay) {
	var samplesOverThreshold int
	for i := 0; i < numSamples; i++ {
		randomDelay := time.Duration(-math.Log(rnd.Float64()) * expectedValue * float64(time.Second))
		if randomDelay > d.Value() {
			samplesOverThreshold++
			d.Increase()
		} else {
			d.Decrease()
		}
		AddPoints(p, randomDelay.Seconds(), d.Value().Seconds())
	}
	fmt.Println("Over threshold: ", samplesOverThreshold)
}

// ConvertToXYs takes separate x and y slices and converts them into the correct plotter.XYs format
func ConvertToXYs(xValues, yValues []float64) plotter.XYs {
	if len(xValues) != len(yValues) {
		panic("xValues and yValues must have the same length")
	}

	xys := make(plotter.XYs, len(xValues))
	for i := range xValues {
		xys[i].X = xValues[i]
		xys[i].Y = yValues[i]
	}
	return xys
}

func actualSample(p *plot.Plot, dataRows []DataRow, d *util.Delay) {
	totalCnt := 500
	xValues := make([]float64, totalCnt)
	yValues1 := make([]float64, totalCnt)
	yValues2 := make([]float64, totalCnt)
	var samplesOverThreshold int
	for i, row := range dataRows {
		if i >= totalCnt {
			break
		}

		actualDelay := time.Duration(row.ReadLatency * float64(time.Second))
		if actualDelay > d.Value() {
			samplesOverThreshold++
			d.Increase()
		} else {
			d.Decrease()
		}
		xValues[i] = float64(i)
		yValues1[i] = d.Value().Seconds()
		yValues2[i] = actualDelay.Seconds()

		//AddPoints(p, actualDelay.Seconds(), d.Value().Seconds())
	}

	// Add line series for the first curve (sine)
	line1, err := plotter.NewLine(ConvertToXYs(xValues, yValues1))
	if err != nil {
		panic(err)
	}

	line1.Color = color.RGBA{R: 255, A: 255} // Red color

	// Add line series for the second curve (cosine)
	line2, err := plotter.NewLine(ConvertToXYs(xValues, yValues2))
	if err != nil {
		panic(err)
	}
	line2.Color = color.RGBA{B: 255, A: 255} // Blue color
	fmt.Println(len(dataRows))

	// Add lines to the plot
	p.Add(line1, line2)

	fmt.Println("Over threshold: ", samplesOverThreshold)
}

func AddPoints(p *plot.Plot, x float64, y float64) {
	// Create a slice of points (we'll have just one point)
	pts := plotter.XYs{{X: x, Y: y}} // Adjust these coordinates as needed

	// Add the points to the plot
	scatter, err := plotter.NewScatter(pts)
	if err != nil {
		panic(err)
	}
	p.Add(scatter)
}

type DataRow struct {
	Timestamp   int64
	ReadLatency float64
	Throughput  float64
}

func GetDataRows(folder string) ([]DataRow, error) {
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
			if cmnEndTime > dataRows[n-1].Timestamp {
				cmnEndTime = dataRows[n-1].Timestamp
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
		return nil, fmt.Errorf("Error reading CSV files: %v\n", err)
	}

	return allDataRows, nil
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

func main() {
	// Create a new plot
	p := plot.New()

	dataRows, err := GetDataRows("/usr/local/google/home/princer/csv/metrics")
	if err != nil {
		fmt.Println("Error while fetching datarows")
		return
	}

	// Set the title and axis labels
	p.Title.Text = "Single Point Plot"
	p.X.Label.Text = "X"
	p.Y.Label.Text = "Y"

	delay, err := util.NewDelay(0.50, 15, 500*time.Millisecond, 500*time.Millisecond, 700*time.Millisecond)
	if err != nil {
		panic(err)
	}

	//applySamples(p, 1000, 0.05, rand.New(rand.NewSource(1)), delay)
	actualSample(p, dataRows, delay)

	// Save the plot as a PNG image
	if err := p.Save(50*vg.Inch, 20*vg.Inch, "point_plot_15.png"); err != nil {
		panic(err)
	}
}
