#!/bin/bash

# MRD Pool Benchmark Runner
# Convenience script for running common benchmark scenarios

set -e

# Default values
BUCKET="princer-gcsfuse-test-zonal-us-west4a"
OBJECT="test_file.0.0"

# io_sizes=(4194304)
io_sizes=(131072)
pool_sizes=(1)
# threads=(1 4 8 16 32 64)
threads=(1)
cnt=3

# Run benchmark for each combination of parameters
for io_size in "${io_sizes[@]}"; do
  for pool_size in "${pool_sizes[@]}"; do
    for thread in "${threads[@]}"; do
      for i in $(seq 1 $cnt); do
        echo "----------------------------------------" 
        echo "Running benchmark with io_size=$io_size, pool_size=$pool_size, threads=$thread (iteration $i)"
        go run ./cmd --bucket="$BUCKET" --object="$OBJECT" --io-size="$io_size" --pool-size="$pool_size" --normal-workers="$thread" --discard-io
        echo "----------------------------------------"
      done
    done
  done
done