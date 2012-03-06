#!/bin/bash
dir="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Number of samples
if [ -n "$1" ]
then
  n="$1"
else
  n=10000
fi

RANGE2=14320
#calculate slopes per minute.
period=60 

# delete old slope files
for file in *-temp.csv
do
  slopef=$( basename "$file" "-temp.csv" )-slopes.csv
  rm $slopef
done


for k in $( seq 1 $n )
do
  br=$( echo "scale=10;$RANDOM / 32767 * ${RANGE2}" | bc )
  dr=$( echo "scale=10;$RANDOM / 32767 * ( ${RANGE2} - $br )" | bc )
  begin=$( date -d "0:00" +%s )
  begin=$( echo "$begin + $br" | bc )
  end=$( echo "$begin + $dr" | bc )
  begin=$( date -d "@$begin" "+%H:%M" )
  end=$( date -d "@$end" "+%H:%M" )
  #echo "$br $dr $begin $end"
  for file in *-temp.csv
  do
    slopef=$( basename "$file" "-temp.csv" )-slopes.csv
    
    slope=$( "${dir}"/specific-data-slope.awk -v start=$begin -v end=$end $file )
    if  [ "$slope" != "same" ]
    then
      echo "$slope" >> $slopef
    fi
  done
done

for file in *-temp.csv
do
  slopef=$( basename "$file" "-temp.csv" )-slopes.csv
  cat $slopef | sort -g | awk '
BEGIN{count=0; prev=0}

prev == 0 {prev=$1;count=1;next}

$1 == prev { count+=1;next }

{print prev,count;prev=$1;count=1}

END{print prev,count}' > ${slopef}2
  mv ${slopef}2 $slopef
done

