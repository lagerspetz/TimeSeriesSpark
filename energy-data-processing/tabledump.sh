#!/bin/bash
source ./common.sh
export MASTER=$master_local
scala -cp "$SPARK_CLASSPATH" edu.berkeley.cs.amplab.carat.dynamodb.DumpTables \
&>alltables-$( date +%Y-%m-%d ).txt
