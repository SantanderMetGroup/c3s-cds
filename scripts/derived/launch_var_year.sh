#!/usr/bin/env bash

set -euo pipefail

SCRIPT="reanalysis-era5-single-levels.py"
var="sfcwind"
# Process 2000 -> 2020
for year in $(seq 1990 2024); do
    echo "Processing ${var} for ${year}"
    python "$SCRIPT" --year "$year" --variable $var
done

# Process 1940 -> 1999
for year in $(seq 1940 1999); do
    echo "Processing ${var} for ${year}"
    python "$SCRIPT" --year "$year" --variable $var
done

echo "Done."
