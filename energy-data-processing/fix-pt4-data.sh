#!/bin/bash

scripts=$( dirname "$0" )
ptp="$scripts"/pt4processor.exe
cf="$scripts"/csv-fixer.awk
dir="$PWD/$1"

if [ ! -d "$dir" ]; then mkdir "$dir"; fi

if [ -n "$2" ]; then
	for k in $*; do
		if [ "$k" == "$1" ]; then
			continue
		fi
		echo "Processing $k ..."
                name="$dir"/$( basename "$k" ".pt4" ).csv
                mono "${ptp}" "$k" > "$name"
        done
else
	for k in *.pt4; do
		echo "Processing $k ..."
		name="$dir"/$( basename "$k" ".pt4" ).csv
		mono "${ptp}" "$k" > "$name"
	done
fi
#for k in *.csv; do
#	echo "Processing $k ..."
#	name="$dir"/$( basename "$k" ".csv" ).csv
#	$cf "$k" > "$name"
#done

