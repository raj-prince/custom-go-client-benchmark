# StatObject Benchmark Results

This file contains results of the comparisons between HTTP/1.1, gRPC Cloud-Path (GFE), and gRPC Direct-Path conducted on `princer-grpc-read-test-uc1a` bucket.

---

## Run 1: 1 Worker (50 Calls Total)

```
=======================================================================
Starting StatObject Benchmark
Bucket: princer-grpc-read-test-uc1a, Workers: 1, Calls/Worker: 50
=======================================================================
Running HTTP/1.1 benchmark... DONE
Running gRPC Cloud-Path benchmark... DONE
Running gRPC Direct-Path benchmark... DONE

========================= BENCHMARK RESULTS =========================

Protocol           | Total Ops | Elapsed Time | QPS   | Avg Latency | P50 Latency | P90 Latency | P99 Latency | Status/Error
--------           | --------- | ------------ | ---   | ----------- | ----------- | ----------- | ----------- | ------------
HTTP/1.1           | 50        | 27.249s      | 1.83  | 544.98 ms   | 774.41 ms   | 870.74 ms   | 1083.19 ms  | Success
gRPC Cloud-Path    | 50        | 24.538s      | 2.04  | 490.77 ms   | 640.78 ms   | 711.85 ms   | 1958.83 ms  | Success
gRPC Direct-Path   | 50        | 23.482s      | 2.13  | 469.63 ms   | 615.60 ms   | 702.05 ms   | 1131.23 ms  | Success

=====================================================================
```

---

## Run 2: 16 Workers (800 Calls Total)

```
=======================================================================
Starting StatObject Benchmark
Bucket: princer-grpc-read-test-uc1a, Workers: 16, Calls/Worker: 50
=======================================================================
Running HTTP/1.1 benchmark... DONE
Running gRPC Cloud-Path benchmark... DONE
Running gRPC Direct-Path benchmark... DONE

========================= BENCHMARK RESULTS =========================

Protocol           | Total Ops | Elapsed Time | QPS    | Avg Latency | P50 Latency | P90 Latency | P99 Latency | Status/Error
--------           | --------- | ------------ | ---    | ----------- | ----------- | ----------- | ----------- | ------------
HTTP/1.1           | 800       | 27.997s      | 28.57  | 544.05 ms   | 742.63 ms   | 875.68 ms   | 1185.64 ms  | Success
gRPC Cloud-Path    | 800       | 16.098s      | 49.70  | 307.07 ms   | 199.07 ms   | 881.50 ms   | 1929.10 ms  | Success
gRPC Direct-Path   | 800       | 22.31s       | 35.86  | 426.50 ms   | 200.19 ms   | 908.32 ms   | 1177.24 ms  | Success

=====================================================================
```
