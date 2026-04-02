#!/bin/bash

# MRD Pool Benchmark Runner
# Convenience script for running common benchmark scenarios

set -e

# Default values
BUCKET="princer-gcsfuse-test-zonal-us-west4a"
OBJECT="100GB"

# io_sizes=(8388608)
io_sizes=(131072 262144 524288 1048576 2097152 4194304 8388608 16777216 33554432)
pool_sizes=(16)
# threads=(1 4 8 16 32 64)
threads=(32 64 128 256)
# threads=(128)

# Run benchmark for each combination of parameters
for pool_size in "${pool_sizes[@]}"; do
  for io_size in "${io_sizes[@]}"; do
    for thread in "${threads[@]}"; do
      echo "----------------------------------------" 
      echo "Running benchmark with io_size=$io_size, pool_size=$pool_size, threads=$thread (iteration $i)"
      cd /home/princer_google_com/dev/custom-go-client-benchmark/rapid && go run ./cmd --bucket="$BUCKET" --object="$OBJECT" --io-size="$io_size" --pool-size="$pool_size" --normal-workers="$thread" && cd -
      echo "----------------------------------------"
    done
  done
done

# cd /home/princer_google_com/dev/custom-go-client-benchmark/rapid && go run ./cmd --bucket="princer-gcsfuse-test-zonal-us-west4a" --object="100GB" --io-size="131072" --pool-size="8" --normal-workers="256" && cd -