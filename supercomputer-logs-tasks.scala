import spark.timeseries._

/* tbird datafile: */
val hdfsDir = "hdfs://server.com:54310/user/username/"
val fileName = "tbird-tagged"

/* bucket by hour */
val buckets = TimeSeriesSpark.bucketLogsByHour(sc, hdfsDir+fileName)

/*
 * 1. looking for time buckets with abnormally large numbers of messages
 */

// how large is abnormally large?
val threshold = 100000

val abnormal = buckets.filter(_.length >= threshold)
val sizes = abnormal.map(_.length).collect()

var sum = 0
var i = 0
var largest = 0
var largestIndex = 0
while ( i < sizes.length){
  println((i+1) + " " + sizes(i))
  sum+=sizes(i)
  if (sizes(i) > largest){
    largestIndex = i
    largest = sizes(i)
  }
  i+=1
}
println("Total " + sum + " messages in " + i + " buckets, largest bucket " + largest)

println("Saving largest bucket(s) of size " + largest +" as text")
val largestbuckets = abnormal.filter(_.length >= largest)
val strbuckets = largestbuckets.map(x => {var str=""; for (k <- x){str+= k.mkString(" ");str+="\n" }; str+="\n";str }).collect()

/*
 * searching for messages within a particular time window
 */

val month = "Dec"
val dayrange = Array(10, 20)
val hourrange = Array(8, 17)

val dec10to20workinghours = buckets.filter(x => {
  val xmonth = x(0)(5)
  val xdate = x(0)(6) toInt
  val xhour = x(0)(7).substring(0,2) toInt
  dayrange(0) < xdate && dayrange(1) >= xdate &&
    hourrange(0) < xhour && hourrange(1) >= xhour &&
    xmonth == month
})

/*
 * or from a particular source
 */

val src = "en263"

val all_en263 = buckets.filter(_(0)(3) == src)

/*
 * Not done yet:
 * alert detection
 * alert prediction (are there patterns of non-alert messages that tend to precede alerts?)
 *
 * this is probably based on some sort of filter on the message text, for example:
 */

val panic = buckets.filter(_(0)(7).contains("something here ...")

// or a complicated regexp

import scala.util.matching.Regex

val kernelacpi = """.*kernel: ACPI.*""".r

val icasePanic = """(?i).*panic.*""".r

val matched = buckets.filter(x => x match {
  case kernelacpi() => {
    // do more processing here
    true
  }
  case icasePanic() => {
    println("DEBUG: \""+x+"\" matches regex.")
    // do more processing here
    true
  }
  case _ => false
})

// print only the first one that matches from each hour bucket:

val all = matched.collect()

for (k <- all){
  println(k(0).mkString(" "))
}


