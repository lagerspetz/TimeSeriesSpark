#!/bin/bash

scripts=$( dirname "$0" )

if [ -n "$1" ]; then 
	dir="$PWD/$1"
else
	dir=temp
fi

makeplot() {
    lines=$( wc -l "$1" | awk '{ print $1 }' )
    let lines=$lines/5000
    if [ "$lines" -gt 1000 ]; then
        "$scripts"/average.awk -v span=5000 "$1" > "$2"
    else
        "$scripts"/average.awk -v span=2500 "$1" > "$2"
    fi
    "$scripts"/plot-energy.sh "$2"
}

#for k in *-600MHz-4V.csv; do
 #   short=$( basename "$k" "-600MHz-4V.csv" ).txt;
  #  makeplot "$k" "$dir/$short"
#done

for k in *-600MHz-4V-2.csv; do
    short=$( basename "$k" "-600MHz-4V-2.csv" )-2.txt;
    makeplot "$k" "$dir/$short"
done

