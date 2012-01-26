#!/bin/bash

if [ -z "$1" ]; then exit 1; fi

dir=$( dirname "$1" )
bn=$( basename "$1" ".txt" )

echo "set term postscript eps enhanced color 'NimbusMono' 18
          set boxwidth 0.75 absolute
          set style fill solid 1.00 border -1
          #set key samplen 4 spacing 1 width 0 height 0
          #set xtics nomirror rotate by 90
          #set xtics out offset 0, -4.0
          #set datafile separator \",\"
          set ytics nomirror
          set xtics offset -1,0
          #set bmargin 4
          #set grid x y
          set xlabel \"Time (s)\"
          set ylabel \"Energy (mW)\"
          set output \"$dir/${bn}.eps\"" > plotfile

#echo "set xrange[0:1000000]" >> plotfile
#echo "set ytics (6, 7, 8, 9, 10, 19, 20, 21, 22)" >> plotfile
#echo "$xtics)" >> plotfile
#cat graphs.txt >> plotfile
echo "plot \"$1\" using 1:2 title \"${bn}\" with lines" >> plotfile
gnuplot plotfile;

