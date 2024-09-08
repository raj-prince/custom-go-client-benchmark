#!/bin/bash

# p50 = 30ms
# Enable command tracing.
set -x

# p10
go run . --increase-rate 1 --target-percentile 0.1 --initial-delay 1ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_10_r_1_i_1ms
go run . --increase-rate 1 --target-percentile 0.1 --initial-delay 30ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_10_r_1_i_30ms
go run . --increase-rate 1 --target-percentile 0.1 --initial-delay 100ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_10_r_1_i_100ms


go run . --increase-rate 15 --target-percentile 0.1 --initial-delay 1ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_10_r_15_i_1ms
go run . --increase-rate 15 --target-percentile 0.1 --initial-delay 30ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_10_r_15_i_30ms
go run . --increase-rate 15 --target-percentile 0.1 --initial-delay 100ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_10_r_15_i_100ms

go run . --increase-rate 100 --target-percentile 0.1 --initial-delay 1ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_10_r_100_i_1ms
go run . --increase-rate 100 --target-percentile 0.1 --initial-delay 30ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_10_r_100_i_30ms
go run . --increase-rate 100 --target-percentile 0.1 --initial-delay 100ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_10_r_100_i_100ms

# p50
go run . --increase-rate 1 --target-percentile 0.5 --initial-delay 1ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_50_r_1_i_1ms
go run . --increase-rate 1 --target-percentile 0.5 --initial-delay 30ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_50_r_1_i_30ms
go run . --increase-rate 1 --target-percentile 0.5 --initial-delay 100ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_50_r_1_i_100ms

go run . --increase-rate 15 --target-percentile 0.5 --initial-delay 1ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_50_r_15_i_1ms
go run . --increase-rate 15 --target-percentile 0.5 --initial-delay 30ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_50_r_15_i_30ms
go run . --increase-rate 15 --target-percentile 0.5 --initial-delay 100ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_50_r_15_i_100ms

go run . --increase-rate 100 --target-percentile 0.5 --initial-delay 1ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_50_r_100_i_1ms
go run . --increase-rate 100 --target-percentile 0.5 --initial-delay 30ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_50_r_100_i_30ms
go run . --increase-rate 100 --target-percentile 0.5 --initial-delay 100ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_50_r_100_i_100ms

# p90
go run . --increase-rate 1 --target-percentile 0.9 --initial-delay 1ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_90_r_1_i_1ms
go run . --increase-rate 1 --target-percentile 0.9 --initial-delay 30ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_90_r_1_i_30ms
go run . --increase-rate 1 --target-percentile 0.9 --initial-delay 100ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_90_r_1_i_100ms

go run . --increase-rate 15 --target-percentile 0.9 --initial-delay 1ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_90_r_15_i_1ms
go run . --increase-rate 15 --target-percentile 0.9 --initial-delay 30ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_90_r_15_i_30ms
go run . --increase-rate 15 --target-percentile 0.9 --initial-delay 100ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_90_r_15_i_100ms

go run . --increase-rate 100 --target-percentile 0.9 --initial-delay 1ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_90_r_100_i_1ms
go run . --increase-rate 100 --target-percentile 0.9 --initial-delay 30ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_90_r_100_i_30ms
go run . --increase-rate 100 --target-percentile 0.9 --initial-delay 100ms --min-delay 1ms --max-delay 10m --sample-count 1000 --output-file p_90_r_100_i_100ms

# Disable command tracing.
set +x