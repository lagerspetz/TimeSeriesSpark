#![$2] += $NF }
$0 ~ "^Time edu.berkeley.cs.amplab.carat." {
  k = $2
  if ($0 ~ "^Time edu.berkeley.cs.amplab.carat.* from ")
    k = $2" "$4
  sums[k] += $NF
  count[k] += 1
}
$0 ~ "^Time spark.timeseries." {
  k = $2
  if ($0 ~ "^Time spark.timeseries.* from ")
    k = $2" "$4
  sums[k] += $NF
  count[k] += 1
}

END {
  for (k in sums){
    name=k
    gsub("edu.berkeley.cs.amplab.carat.", "", name)
    gsub("spark.timeseries.", "", name)
    print sums[k], sums[k]/count[k], name
  }
}

