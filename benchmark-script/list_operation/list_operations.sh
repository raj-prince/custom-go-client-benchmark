#!/bin/bash

set -e
shopt -s expand_aliases


alias list_for_1="/home/princer_google_com/memory_work/custom-go-client-benchmark/benchmark-script/list_operation/list_operation --dir \"/home/princer_google_com/gcs/listing/1\""

alias list_for_1K="/home/princer_google_com/memory_work/custom-go-client-benchmark/benchmark-script/list_operation/list_operation --dir \"/home/princer_google_com/gcs/listing/1K\""

alias list_for_10K="/home/princer_google_com/memory_work/custom-go-client-benchmark/benchmark-script/list_operation/list_operation --dir \"/home/princer_google_com/gcs/listing/10K\""

alias list_for_50K="/home/princer_google_com/memory_work/custom-go-client-benchmark/benchmark-script/list_operation/list_operation --dir \"/home/princer_google_com/gcs/listing/50K\""

alias list_for_100K="/home/princer_google_com/memory_work/custom-go-client-benchmark/benchmark-script/list_operation/list_operation --dir \"/home/princer_google_com/gcs/listing/100K\""


# 1K files with cache
echo "LoggerPrince: listing test for 1K files with cache"
/home/princer_google_com/memory_work/gcsfuse/gcsfuse --type-cache-ttl 10m --stat-cache-ttl 10m --stat-cache-capacity 2000  gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
for i in {1..50}; do list_for_1K; done
umount ~/gcs

# 1K files with without cache
echo "LoggerPrince: listing test for 1K files without cache"
/home/princer_google_com/memory_work/gcsfuse/gcsfuse --type-cache-ttl 0s --stat-cache-ttl 0s gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
for i in {1..50}; do list_for_1K; done
umount ~/gcs

# 10K files with cache
echo "LoggerPrince: listing test for 10K files with cache"
/home/princer_google_com/memory_work/gcsfuse/gcsfuse --type-cache-ttl 10m --stat-cache-ttl 10m --stat-cache-capacity 20000  gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
for i in {1..5}; do list_for_10K; done
umount ~/gcs

# 10K files with without cache
echo "LoggerPrince: listing test for 10K files without cache"
/home/princer_google_com/memory_work/gcsfuse/gcsfuse --type-cache-ttl 0s --stat-cache-ttl 0s gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
for i in {1..5}; do list_for_10K; done
umount ~/gcs

# 50K files with cache
echo "LoggerPrince: listing test for 50K files with cache"
/home/princer_google_com/memory_work/gcsfuse/gcsfuse --type-cache-ttl 10m --stat-cache-ttl 10m --stat-cache-capacity 100000  gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
for i in {1..1}; do list_for_50K; done
umount ~/gcs

# 50K files with without cache
echo "LoggerPrince: listing test for 50K files without cache"
/home/princer_google_com/memory_work/gcsfuse/gcsfuse --type-cache-ttl 0s --stat-cache-ttl 0s gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
for i in {1..1}; do list_for_50K; done
umount ~/gcs

# 100K files with cache
echo "LoggerPrince: listing test for 100K files with cache"
/home/princer_google_com/memory_work/gcsfuse/gcsfuse --type-cache-ttl 10m --stat-cache-ttl 10m --stat-cache-capacity 200000  gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
for i in {1..1}; do list_for_100K; done
umount ~/gcs

# 100K files with without cache
echo "LoggerPrince: listing test for 100K files without cache"
/home/princer_google_com/memory_work/gcsfuse/gcsfuse --type-cache-ttl 0s --stat-cache-ttl 0s gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
for i in {1..1}; do list_for_100K; done
umount ~/gcs
