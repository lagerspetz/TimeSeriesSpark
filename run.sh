#!/bin/bash
dir=$( dirname "$0" )
dir=$(readlink -f "$dir" )
source $dir/common.sh

SPARK_MEM="20g"
JAVA_OPTS+=" -Xmx20g"
master=${master_local}

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

scala -cp "$SPARK_CLASSPATH" $class $master $datafile "$idle" $4 $5

