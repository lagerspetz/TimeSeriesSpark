#!/bin/bash

dir=$( dirname "$0" )
scripts=${dir}/energy-data-processing
ptp="$scripts"/pt4processor.exe

m=$( which mono )
if [ -z "$m" ]; then echo "Please install mono."; exit 1; fi

for k in *.pt4; do
  echo "Processing $k ..."
  name="$dir"/$( basename "$k" ".pt4" ).csv
  mono "${ptp}" "$k" > "$name"
done

