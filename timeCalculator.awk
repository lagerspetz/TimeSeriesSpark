#!/usr/bin/awk -f

$1 ~ "^Time edu.berkeley.cs." { a[$1] += $2 }
$1 ~ "^Time spark.timeseries." { a[$1] += $2 }

END {
        for (k in a){
                name=k
                gsub("Time edu.berkeley.cs.amplab.carat", "", name)
                gsub("Time spark.timeseries.", "", name)
                print a[k], name
        }
}

