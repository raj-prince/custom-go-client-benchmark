#!/bin/bash

set -e
shopt -s expand_aliases

threads=$1
block_size=$2
file_size=$3
write_count=$4

alias write_file_cmd="GODEBUG=asyncpreemptoff=1 /home/princer_google_com/memory_work/custom-go-client-benchmark/benchmark-script/write_operations/write_operations --threads ${threads} --block-size ${block_size} --file-size ${file_size} --write-count ${write_count} --dir \"/home/princer_google_com/gcs/writing/\""

/home/princer_google_com/memory_work/gcsfuse/gcsfuse gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
write_file_cmd
umount ~/gcs
