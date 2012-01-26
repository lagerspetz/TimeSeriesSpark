#!/bin/bash
# Plot battery level using pt4-processed csv files in current directory.

dir="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

plotstr="plot "

if [ -n "$1" ]
  then span="$1"
else
  span=5000
fi

if [ -n "$2" ]
  then battery="$2"
else
  battery=1320 # 1320 mAh = N900
  battery=1500 # 1500 mAh = LG Optimus X2
fi

if [ -n "$3" ]
  then starth="$3"
else
  starth=10
fi

if [ -n "$4" ]
  then startm="$4"
else
  startm=0
fi

num=1
name=""
for k in *csv
  # No csv's in directory?
  do if [ "$k" == '*.csv' ]
    then continue
  fi

  ending=$( basename "$k" "-temp.csv" )
  # Trying to process an existing tempfile?
  if [ "$ending" != "$k" ]
    then continue
  fi

  if [ ! -f "$k"-temp.csv ]
    then  "$dir"/battery-drain-average.awk -v span=$span -v battery=$battery \
      -v starth=$starth -v startm=$startm "$k" > "$k"-temp.csv
  fi
done

for k in *-temp.csv
  do if [ "$k" == '*-temp.csv' ]
    then continue
  fi

  if [ "$num" -gt 1 ]
    then plotstr="${plotstr},"
  fi
  
  n=$( echo "$k" | sed -e "s/-temp.csv//" )
  n=$( echo "$k" | sed -e "s/-5MB-dispoff-5runs-600MHz-4V.csv//" )
  n=$( echo "$n" | sed -e "s/-5MB.csv//" )
  n=$( echo "$n" | sed -e "s/.csv//" )
  
  if [ -z "$name" ]
    then name="$n"
  fi

  if [ "$num" == 6 -o "$num" == 15 ]
    then let num++
  fi
  temp=$( expr match "$k" '.*powermon.*' )
  if [ "$temp" -gt 0 ]
    then plotstr="$plotstr \"$k\" using 1:2 with lines lt $num lw 2 title \"$n\""
  else
    plotstr="$plotstr \"$k\" using 1:2 with linespoints lt $num lw 2 pt $num title \"$n\""
  fi
  let num++
done
#plotstr="$plotstr, (x*1.5/18) with lines lt 13 lw 2 title \"300 mA\""
#plotstr="$plotstr, (($battery-x*100/3600)/$battery*100) with lines lt 13 lw 2 title \"N900 idle (Nokia paper)\""
#plotstr="$plotstr, (($battery-x*0.0625)/$battery*100) with lines lt 16 lw 2 title \"N900 100% CPU (Nokia paper)\""
if [ -f events.txt ]
  then while read line; do plotstr="$plotstr, $line"; done < events.txt
fi


echo "set term postscript eps enhanced color 'NimbusMono' 18
    set parametric
    set trange [0:105]
    #set logscale x
#    set logscale y
    set boxwidth 0.75 absolute
    set xdata time
    set timefmt \"%H:%M:%S\"
    set style fill solid 1.00 border -1
    set key samplen 4 spacing 1 width 0 height 0 left bottom Left reverse
    set xtics nomirror rotate by 90
    set xtics out
    # offset 0, -2.0
    set format x \"%H:%M\"
    #set datafile separator \",\"
    set ytics nomirror
    set yrange [0:105]
    #set bmargin 4
    #set grid x y
    set xlabel \"Time (HH:MM)\"
    set ylabel \"Battery level (% of $battery mAh)\"
    set output \"$name.eps\"" > plotfile.txt

#echo "set xrange[0:1000000]" >> plotfile
#echo "set ytics (6, 7, 8, 9, 10, 19, 20, 21, 22)" >> plotfile
#echo "$xtics)" >> plotfile
#cat graphs.txt >> plotfile

echo "$plotstr" >> plotfile.txt
gnuplot plotfile.txt

