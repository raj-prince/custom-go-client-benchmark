#!/bin/bash

set -e
shopt -s expand_aliases

file_count=$1

alias open_file_cmd="/home/princer_google_com/memory_work/custom-go-client-benchmark/benchmark-script/open_file/open_file --open-file ${file_count} --dir \"/home/princer_google_com/gcs/listing/128KB/100K\""

/home/princer_google_com/memory_work/gcsfuse/gcsfuse gcs-fuse-memory-profile-data ~/gcs
ps -aux | grep "gcsfuse"
open_file_cmd
umount ~/gcs