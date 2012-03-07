#!/bin/bash
d1="Fri Mar  2"
d2="Mon Mar  5"
d3="Tue Mar  6"
st="00:00:00 PST 2012"
i=0
for k in "$d1" "$d2" "$d3"
do
  let i++
  sec=$( date -d "$k $st" +%s )
  # 24h + 10 mins
  let end=$sec+86400
  echo $sec $end
  s[$i]=$sec
  e[$i]=$end
done

