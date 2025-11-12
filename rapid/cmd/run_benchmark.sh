#!/bin/bash

# MRD Pool Benchmark Runner
# Convenience script for running common benchmark scenarios

set -e

# Default values
BUCKET=""
OBJECT=""
SCENARIO="balanced"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --bucket)
      BUCKET="$2"
      shift 2
      ;;
    --object)
      OBJECT="$2"
      shift 2
      ;;
    --scenario)
      SCENARIO="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 --bucket BUCKET --object OBJECT [--scenario SCENARIO]"
      echo ""
      echo "Scenarios:"
      echo "  balanced       - Balanced configuration (default)"
      echo "  high-throughput - Optimized for maximum throughput"
      echo "  high-iops      - Optimized for maximum IOPS"
      echo "  low-latency    - Optimized for low latency"
      echo "  custom         - Use custom parameters (add your own flags)"
      echo ""
      echo "Example:"
      echo "  $0 --bucket my-bucket --object large-file.bin --scenario high-throughput"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Validate required parameters
if [ -z "$BUCKET" ] || [ -z "$OBJECT" ]; then
  echo "Error: --bucket and --object are required"
  echo "Use --help for usage information"
  exit 1
fi

# Build if needed
if [ ! -f "./mrd_benchmark" ]; then
  echo "Building mrd_benchmark..."
  go build -o mrd_benchmark
fi

# Run based on scenario
case $SCENARIO in
  balanced)
    echo "Running balanced configuration..."
    ./mrd_benchmark \
      --bucket="$BUCKET" \
      --object="$OBJECT" \
      --threads=20 \
      --io-size=4194304 \
      --queue-depth=10 \
      --pool-size=5 \
      --duration=60s
    ;;
    
  high-throughput)
    echo "Running high-throughput configuration..."
    ./mrd_benchmark \
      --bucket="$BUCKET" \
      --object="$OBJECT" \
      --threads=50 \
      --io-size=8388608 \
      --queue-depth=20 \
      --pool-size=10 \
      --duration=120s
    ;;
    
  high-iops)
    echo "Running high-IOPS configuration..."
    ./mrd_benchmark \
      --bucket="$BUCKET" \
      --object="$OBJECT" \
      --threads=100 \
      --io-size=65536 \
      --queue-depth=50 \
      --pool-size=20 \
      --duration=60s
    ;;
    
  low-latency)
    echo "Running low-latency configuration..."
    ./mrd_benchmark \
      --bucket="$BUCKET" \
      --object="$OBJECT" \
      --threads=5 \
      --io-size=1048576 \
      --queue-depth=5 \
      --pool-size=3 \
      --duration=60s
    ;;
    
  custom)
    echo "Running with custom parameters..."
    echo "Pass additional flags after --scenario custom"
    ./mrd_benchmark \
      --bucket="$BUCKET" \
      --object="$OBJECT" \
      "$@"
    ;;
    
  *)
    echo "Unknown scenario: $SCENARIO"
    echo "Valid scenarios: balanced, high-throughput, high-iops, low-latency, custom"
    exit 1
    ;;
esac
