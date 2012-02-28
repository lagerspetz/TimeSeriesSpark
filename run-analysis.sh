#!/bin/bash
# Main script for running the Carat data analysis.
while true
do
  d=$( date +%Y-%m-%d-%H-%M-%S )
  echo "Analysis started at $d"
  ./run-laptop.sh edu.berkeley.cs.amplab.carat.CaratDynamoDataAnalysis DEBUG \
  &> "carat-dynamo-data-analysis-log-$d.txt"
  d=$( date +%Y-%m-%d-%H-%M-%S )
  echo "Analysis finished at $d"
  rm -rf spark-temp/spark-local-*
  sleep 3600
done
