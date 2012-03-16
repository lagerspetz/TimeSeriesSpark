#!/bin/bash
dir="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$dir"
source $dir/common.sh

SPARK_MEM="2g"
export SPARK_JAVA_OPTS="$JAVA_OPTS"
export MASTER='local[1]'


scala -cp "$SPARK_CLASSPATH" $1 $MASTER $2 $3 $4 $5 $6

