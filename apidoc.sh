#!/bin/bash
cd $( dirname "$0" )
./compile.sh
cp=""
first="1"
for k in jar/* jfreechart-1.0.13/lib/*.jar; do
  if [ first == "1" ]; then first=0; else cp="${cp}:"; fi
  cp="${cp}${k}"
done

scaladoc -d api -cp "${cp}:./bin" \
src/spark/timeseries/BucketDetector.scala \
src/spark/timeseries/BucketLogsByHour.scala \
src/spark/timeseries/BucketRDD.scala \
src/spark/timeseries/EnergyOps.scala \
src/spark/timeseries/HdfsRuns.scala \
src/spark/timeseries/HourBucketDetector.scala \
src/spark/timeseries/IdleEnergyArrayDetector.scala \
src/spark/timeseries/MeasurementTest.scala \
src/spark/timeseries/package.scala \
src/spark/timeseries/RunDetector.scala \
src/spark/timeseries/RunRDD.scala \
src/spark/timeseries/Run.scala \
src/spark/timeseries/TimeSeriesSpark.scala \
src/spark/timeseries/examples/SuperComputerLogs.scala
#src/spark/timeseries/Plotter.scala \
#src/spark/timeseries/XScalaWTEnergyPlot.scala \
#src/spark/timeseries/ChartData.scala \
#src/spark/timeseries/EnergyPlot.scala \
#src/spark/timeseries/examples \
#src/spark/timeseries/j \

