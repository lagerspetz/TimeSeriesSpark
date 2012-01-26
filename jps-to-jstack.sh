#!/bin/bash

ps=$( jps | awk '$2 != "Jps" { print $1 }' )
for k in $ps; do
  n=$( jps | awk -v ps=$k '$1 == ps { print $2 }' )
  #echo "n=$n k=$k"
  jstack "$k" > "$n-$k".txt
done
