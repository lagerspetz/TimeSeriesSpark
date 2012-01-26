#!/bin/bash

num=1  
plotstr="plot "
name=""
for k in *-slopes.csv
do
  title=$(basename "$k" "-slopes.csv" )
  title=$(basename "$title" ".csv" )

  if [ -z "$name" ]; then name=$title; fi
  
  if [ "$num" -gt 1 ]
    then plotstr="${plotstr},"
  fi 
  if [ "$num" == 1 ]
  then
    color='lt rgb "#f3b14d"'
  elif [ "$num" == 2 ]
  then
    color='lt rgb "#007777"'
  else
    color="lt $num"
  fi

  plotstr+="\"$k\" using (-1*\$1):(\$2/10000) with linespoints $color title \"$title\""
  let num++
done

echo "set term postscript eps enhanced color 'Helvetica' 24
    #set key samplen 4 spacing 1 width 0 height 0 left bottom Left reverse
    #set xtics nomirror rotate by 90
    set logscale x
    #set logscale y
    set xtics out
    # offset 0, -2.0
    set ytics nomirror
    #set yrange [0:1]
    set xlabel \"Battery drain (% / min)\"
    set ylabel \"Frequency\"
    set output \"$name-frequency.eps\"" > plotfile.txt
    
echo "$plotstr" >> plotfile.txt

gnuplot plotfile.txt
