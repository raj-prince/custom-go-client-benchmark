#!/bin/bash

set -e
shopt -s expand_aliases

block_size=$1
file_size=$2

alias write_file_cmd="/home/princer_google_com/memory_work/custom-go-client-benchmark/benchmark-script/write_operations/write_operations --block-size ${block_size} --file-size ${file_size}  --dir \"/home/princer_google_com/gcs/writing/\""

/home/princer_google_com/memory_work/gcsfuse/gcsfuse gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
write_file_cmd
umount ~/gcs
