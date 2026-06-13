# StatObject Benchmark Results

This file contains results of the comparisons between HTTP/1.1, gRPC Cloud-Path (GFE), and gRPC Direct-Path conducted on `princer-grpc-read-test-uc1a` bucket.

---
## Run 1: 1 Worker (50 Calls Total) - Executed on Co-located VM (abhishek-central1a, us-central1-a) on June 13, 2026 (Third Attempt)

```
=======================================================================
Starting StatObject Benchmark
Bucket: princer-grpc-read-test-uc1a, Workers: 1, Calls/Worker: 50
=======================================================================
Running HTTP/1.1 benchmark... DONE
Running gRPC Cloud-Path benchmark... DONE
Running gRPC Direct-Path benchmark... DONE

========================= BENCHMARK RESULTS =========================

Protocol           | Total Ops | Elapsed Time | QPS    | Avg Latency | P50 Latency | P90 Latency | P99 Latency | Status/Error
--------           | --------- | ------------ | ---    | ----------- | ----------- | ----------- | ----------- | ------------
HTTP/1.1           | 50        | 1.104s       | 45.27  | 22.09 ms    | 20.77 ms    | 30.00 ms    | 47.78 ms    | Success
gRPC Cloud-Path    | 50        | 1.449s       | 34.50  | 28.98 ms    | 28.36 ms    | 36.12 ms    | 47.40 ms    | Success
gRPC Direct-Path   | 50        | 1.507s       | 33.17  | 30.15 ms    | 26.91 ms    | 32.26 ms    | 147.06 ms   | Success

=====================================================================
```

---

## Run 1: 16 Workers (800 Calls Total) - Executed on Co-located VM (abhishek-central1a, us-central1-a) on June 13, 2026 (Third Attempt)

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
HTTP/1.1           | 800       | 1.233s       | 648.57 | 21.01 ms    | 19.87 ms    | 24.83 ms    | 42.34 ms    | Success
gRPC Cloud-Path    | 800       | 1.103s       | 725.08 | 21.13 ms    | 19.45 ms    | 27.47 ms    | 52.10 ms    | Success
gRPC Direct-Path   | 800       | 1.196s       | 668.91 | 21.80 ms    | 18.08 ms    | 25.98 ms    | 145.58 ms   | Success

=====================================================================
```
