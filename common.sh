#!/bin/bash
dir="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#dir=$HOME/work/energy-spark
#echo dir=$dir
# Set Spark environment variables for your site in this file. Some useful
# variables to set are:
# - MESOS_HOME, to point to your Mesos installation
# - SCALA_HOME, to point to your Scala installation
# - SPARK_CLASSPATH, to add elements to Spark's classpath
# - SPARK_JAVA_OPTS, to add JVM options
# - SPARK_MEM, to change the amount of memory used per node (this should
#   be in the same format as the JVM's -Xmx option, e.g. 300m or 1g).
# - SPARK_LIBRARY_PATH, to add extra search paths for native libraries.

export MESOS_HOME=$HOME/mesos
#export SCALA_HOME=
cp=""
for k in $dir/jar/* $dir/jfreechart-1.0.13/lib/*.jar; do
  if [ first == "1" ]; then first=0; else cp="${cp}:"; fi
  cp="${cp}${k}"
done
export SPARK_CLASSPATH="$HOME/mesos/lib/java/mesos.jar:$dir/jar/spark-core-assembly-0.4-SNAPSHOT.jar:$cp:$dir/bin"
#SPARK_JAVA_OPTS=""
mem="27g"
export SPARK_MEM="${mem}"
#Workaround an issue with xerces? Where did I get xerces?
export JAVA_OPTS="-Djavax.xml.parsers.DocumentBuilderFactory=com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl -XX:+UseCompressedOops"
# "-Xmx${mem} -XX:+UseCompressedOops -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
export SPARK_JAVA_OPTS="$JAVA_OPTS"
export SPARK_HOME="$HOME/spark"
export SPARK_LIBRARY_PATH=$HOME/mesos/lib/java:$HOME/mesos/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:\
$HOME/mesos/lib/java:$HOME/mesos/lib

#slaves="cloudtech07 cloudtech08 cloudtech12 cloudtech13 cloudtech14 cloudtech15"
slaves="localhost"

master_mesos='mesos://master@localhost:5050'
master_local='local[8]'

datafile=data/d-2g-elisa-tktl-71MB-dispoff-5runs-600MHz-4V.csv
class=spark.timeseries.EnergyOps

#echo $SPARK_CLASSPATH
