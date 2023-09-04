#!/bin/bash

set -e
shopt -s expand_aliases

dir=$1

alias list_cmd="GODEBUG=asyncpreemptoff=1 /home/princer_google_com/memory_work/custom-go-client-benchmark/benchmark-script/list_operation/list_operation --dir \"/home/princer_google_com/gcs/listing/${dir}\""


echo "With cache"
/home/princer_google_com/memory_work/gcsfuse/gcsfuse --type-cache-ttl 10000m --stat-cache-ttl 10000m --stat-cache-capacity 200000 gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
list_cmd
umount ~/gcs

echo "Without cache"
/home/princer_google_com/memory_work/gcsfuse/gcsfuse --type-cache-ttl 0s --stat-cache-ttl 0s --stat-cache-capacity 0 gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
list_cmd
umount ~/gcs