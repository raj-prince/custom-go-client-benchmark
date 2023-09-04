#!/bin/bash

set -e
shopt -s expand_aliases

file_count=$1

alias open_file_cmd="GODEBUG=asyncpreemptoff=1 /home/princer_google_com/memory_work/custom-go-client-benchmark/benchmark-script/open_file/open_file --open-files ${file_count} --dir \"/home/princer_google_com/gcs/listing/100K\""

echo "With cache"
/home/princer_google_com/memory_work/gcsfuse/gcsfuse --type-cache-ttl 10000m --stat-cache-ttl 10000m --stat-cache-capacity 200000 gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
open_file_cmd
umount ~/gcs

echo "Without cache"
/home/princer_google_com/memory_work/gcsfuse/gcsfuse --type-cache-ttl 0s --stat-cache-ttl 0s --stat-cache-capacity 0 gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
open_file_cmd
umount ~/gcs