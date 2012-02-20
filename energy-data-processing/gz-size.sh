#!/bin/bash
if [ ! -f "$1" ]
then
  echo "Usage: gz-size.sh file-to-gzip-lines-of.txt"
  exit 1
fi

while read line
do
  gzsize=$( echo "$line" | gzip -cf | wc -c )
  echo "$line $gzsize"
done < "$1"

