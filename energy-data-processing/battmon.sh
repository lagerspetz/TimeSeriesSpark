#!/bin/sh
if [ -n "$1" ]; then output="$1";
else
output=battmon-log.txt;
fi

while true; do
sleep 60;
dumpsys battery >> "$output";
date >> "$output";
done
