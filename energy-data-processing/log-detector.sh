#!/bin/bash
for k in *csv; do
	n=$( basename "$k" ".csv" )
	log=../dessy-energy-logs/"$n".txt
	if [ ! -f "$log" ]; then
		echo "no log for $k"
	fi
done

