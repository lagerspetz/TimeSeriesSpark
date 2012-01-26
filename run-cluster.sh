#!/bin/bash
dir="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$dir"
source $dir/common.sh

./prepare-cluster.sh

#killall java

MASTER=${master_mesos}

if [ -n "$1" ]; then
    class="$1"
fi

if [ -n "$2" ]; then
    datafile="$2"
fi

if [ -n "$3" ]; then
    idle="$3"
else
    idle=500
fi

scala -cp "$SPARK_CLASSPATH" $class $MASTER "$datafile" "$idle"

