#!/bin/bash
cd $( dirname "$0" )
cp=""
first="1"
for k in jar/* jfreechart-1.0.13/lib/*.jar; do
  if [ first == "1" ]; then first=0; else cp="${cp}:"; fi
  cp="${cp}${k}"
done

if [ -f src/spark/timeseries/j/PlotData.java ]; then
  javadoc -d doc -classpath "${cp}:bin" src/spark/timeseries/j/PlotData.java
fi

scaladoc -d doc -cp "${cp}:bin" $( find src -type f -name "*.scala" )

