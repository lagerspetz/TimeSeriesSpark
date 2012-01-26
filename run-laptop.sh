#!/bin/bash
dir="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$dir"
source $dir/common.sh

SPARK_MEM="2g"
export JAVA_OPTS="-Djavax.xml.parsers.DocumentBuilderFactory=com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl -XX:+UseCompressedOops"
export SPARK_JAVA_OPTS="$JAVA_OPTS"
export MASTER='local[1]'

if [ -n "$1" ]; then
    class="$1"
fi

if [ -n "$2" ]; then
    datafile="$2"
fi

if [ -n "$3" ]; then
    idle="$3"
else
    idle=250
fi

scala -cp "$SPARK_CLASSPATH" $class $MASTER $datafile "$idle" $4 $5

