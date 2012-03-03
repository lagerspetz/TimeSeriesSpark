#![$2] += $NF }
$0 ~ "^Time edu.berkeley.cs.amplab.carat." {
  sums[$2] += $NF
  count[$2] += 1
}
$0 ~ "^Time spark.timeseries." {
  sums[$2] += $NF
  count[$2] += 1
}

END {
  for (k in sums){
    name=k
    gsub("edu.berkeley.cs.amplab.carat.", "", name)
    gsub("spark.timeseries.", "", name)
    print sums[k], sums[k]/count[k], name
  }
}

