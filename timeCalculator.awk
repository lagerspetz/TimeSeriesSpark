#![$2] += $NF }
$0 ~ "^Time edu.berkeley.cs.amplab.carat." { a[$2] += $NF }
$0 ~ "^Time spark.timeseries." { a[$2] += $NF }

END {
        for (k in a){
                name=k
                gsub("edu.berkeley.cs.amplab.carat.", "", name)
                gsub("spark.timeseries.", "", name)
                print a[k], name
        }
}

