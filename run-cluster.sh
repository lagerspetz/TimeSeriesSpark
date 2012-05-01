#!/bin/bash
dir="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$dir"
source $dir/common.sh

./prepare-cluster.sh

export MASTER=${master_mesos}

scala -cp "$SPARK_CLASSPATH" $1 $MASTER $2 $3 $4 $5 $6
