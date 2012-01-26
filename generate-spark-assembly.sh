#!/bin/bash
dir=$( dirname  "$0" )
cd "$dir"
cd ..
if [ ! -d spark ]; then
  git=$( which git )
  if [ -z "$git" ]; then echo "Please install git."; exit 1; fi
  git clone git://github.com/mesos/spark.git
fi
cd spark
sbt/sbt update compile assembly
cp core/target/spark-core-assembly-0.4-SNAPSHOT.jar ..

