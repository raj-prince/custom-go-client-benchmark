#!/bin/bash

set -e
shopt -s expand_aliases

time go run . --threads 64 --read-count 1 --file-size-mb 64 --dir /home/princer_google_com/warp-test/gcs/64M/ --file-prefix "experiment."