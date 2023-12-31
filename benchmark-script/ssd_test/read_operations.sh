#!/bin/bash

set -e
shopt -s expand_aliases

threads=$1

alias read_for_256KB="GODEBUG=asyncpreemptoff=1 /home/princer_google_com/memory_work/custom-go-client-benchmark/benchmark-script/read_operation/read_operation --read-count 1000 --threads ${threads} --block-size 256 --dir \"/home/princer_google_com/gcs/reading/256KB\""

alias read_for_1MB="GODEBUG=asyncpreemptoff=1 /home/princer_google_com/memory_work/custom-go-client-benchmark/benchmark-script/read_operation/read_operation --read-count 100 --threads ${threads} --block-size 1024 --dir \"/home/princer_google_com/gcs/reading/1MB\""

alias read_for_100MB="GODEBUG=asyncpreemptoff=1 /home/princer_google_com/memory_work/custom-go-client-benchmark/benchmark-script/read_operation/read_operation --read-count 10 --threads ${threads} --block-size 1024 --dir \"/home/princer_google_com/gcs/reading/100MB\""

alias read_for_1GB="GODEBUG=asyncpreemptoff=1 /home/princer_google_com/memory_work/custom-go-client-benchmark/benchmark-script/read_operation/read_operation --read-count 1 --threads ${threads} --block-size 1024 --dir \"/home/princer_google_com/gcs/reading/1GB\""

# read for 256KB file
echo "LoggerPrince: reading for 256KB with ${threads} threads"
/home/princer_google_com/memory_work/gcsfuse/gcsfuse --type-cache-ttl 10000m --stat-cache-ttl 10000m gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
read_for_256KB
umount ~/gcs

# read for 1MB file
echo "LoggerPrince: reading for 1MB with ${threads} threads"
/home/princer_google_com/memory_work/gcsfuse/gcsfuse --type-cache-ttl 10000m --stat-cache-ttl 10000m gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
read_for_1MB
umount ~/gcs

# read for 100MB file
echo "LoggerPrince: reading for 100MB with ${threads} threads"
/home/princer_google_com/memory_work/gcsfuse/gcsfuse --type-cache-ttl 10000m --stat-cache-ttl 10000m gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
read_for_100MB
umount ~/gcs

# read for 1GB file
echo "LoggerPrince: reading for 1GB with ${threads} threads"
/home/princer_google_com/memory_work/gcsfuse/gcsfuse --type-cache-ttl 10000m --stat-cache-ttl 10000m gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
read_for_1GB
umount ~/gcs
