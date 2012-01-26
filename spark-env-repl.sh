#!/bin/bash
dir=$( dirname "$0" )
dir=$(readlink -f "$dir" )
source ../common.sh

SPARK_MEM="2g"
JAVA_OPTS="-Xms100m -Xmx2400m"
master='local[2]'

