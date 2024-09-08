package util

import (
	"fmt"
	"math"
	"time"
)

// Package dynamicdelay calculates the delay at a fixed percentile, based on
// delay samples.
//
// Delay is not goroutine-safe.
type Delay struct {
	increaseFactor float64
	decreaseFactor float64
	minDelay       time.Duration
	maxDelay       time.Duration
	value          time.Duration
}

// NewDelay returns a Delay.
//
// targetPercentile is the desired percentile to be computed. For example, a
// targetPercentile of 0.99 computes the delay at the 99th percentile. Must be
// in the range [0, 1].
//
// increaseRate (must be > 0) determines how many Increase calls it takes for
// Value to double.
//
// Decrease can never lower the delay past minDelay, Increase can never raise
// the delay past maxDelay.
func NewDelay(targetPercentile float64, increaseRate float64, initialDelay, minDelay, maxDelay time.Duration) (*Delay, error) {
	if targetPercentile < 0 || targetPercentile > 1 {
		return nil, fmt.Errorf("invalid targetPercentile (%v): must be within [0, 1]", targetPercentile)
	}
	if increaseRate <= 0 {
		return nil, fmt.Errorf("invalid increaseRate (%v): must be > 0", increaseRate)
	}
	if minDelay >= maxDelay {
		return nil, fmt.Errorf("invalid minDelay (%v) and maxDelay (%v) combination: minDelay must be smaller than maxDelay", minDelay, maxDelay)
	}
	if initialDelay < minDelay {
		initialDelay = minDelay
	}
	if initialDelay > maxDelay {
		initialDelay = maxDelay
	}
	// See http://google3/net/rpc/contrib/hedged_call/dynamic_delay.h?l=35&rcl=194811090
	increaseFactor := math.Exp(math.Log(2) / increaseRate)
	if increaseFactor < 1.001 {
		increaseFactor = 1.001
	}
	decreaseFactor := math.Exp(-math.Log(increaseFactor) * (1 - targetPercentile) / targetPercentile)
	if decreaseFactor > 0.9999 {
		decreaseFactor = 0.9999
	}

	return &Delay{
		increaseFactor: increaseFactor,
		decreaseFactor: decreaseFactor,
		minDelay:       minDelay,
		maxDelay:       maxDelay,
		value:          initialDelay,
	}, nil
}

// increase notes that the RPC took longer than the delay returned by Value.
func (d *Delay) Increase() {
	v := time.Duration(float64(d.value) * d.increaseFactor)
	if v > d.maxDelay {
		d.value = d.maxDelay
	} else {
		d.value = v
	}
}

// decrease notes that the RPC completed before the delay returned by Value.
func (d *Delay) Decrease() {
	v := time.Duration(float64(d.value) * d.decreaseFactor)
	if v < d.minDelay {
		d.value = d.minDelay
	} else {
		d.value = v
	}
}

// Update notes that the RPC either took longer than the delay or completed
// before the delay, depending on the specified latency.
func (d *Delay) Update(latency time.Duration) {
	if latency > d.value {
		d.Increase()
	} else {
		d.Decrease()
	}
}

// Value returns the desired delay to wait before hedging the RPC call.
func (d *Delay) Value() time.Duration {
	return d.value
}

func (d *Delay) PrintDelay() {
	fmt.Println("IncreaseFactor: ", d.increaseFactor)
	fmt.Println("DecreaseFactor: ", d.decreaseFactor)
	fmt.Println("MinDelay: ", d.minDelay)
	fmt.Println("MaxDelay: ", d.maxDelay)
	fmt.Println("Value: ", d.value)
}
