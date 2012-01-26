package spark.timeseries

import spark._
import spark.SparkContext._
import scala.collection.mutable.ArrayBuffer

/**
 * Bucket supercomputer logs (such as [[http://www.cs.sandia.gov/~jrstear/logs/]])
 * into (hour,node)(message) buckets.
 * 
 */
object BucketLogsByHour {

  /**
   * Runs Spark with the master `args(0)`. Assigns log messages stored in the file `args(1)`
   * into buckets by hour and source node. Save the bucketed messages into HDFS, into the same directory as the input file. 
   *  
   * @param args Command line arguments.
   */
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: AlertRuns master filename bucketMinutes\n" +
        "Example: AlertRuns local[2] tbird-tagged")
      return
    }
    val hdfsDir = ""
    val sc = TimeSeriesSpark.init(args(0), "default")
    val buckets = TimeSeriesSpark.bucketLogsByNodeHour(sc, hdfsDir+args(1))
    val sizes = buckets.map(_._2.length).collect()
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
    println(sum)
    println("Saving largest buckets of size " + largest)
    val largestbuckets = buckets.filter(_._2.length >= largest)
    largestbuckets.map(_._2.mkString).saveAsTextFile(hdfsDir+"largest-buckets-of-"+args(1)+"-"+args(2))
    
    //buckets.saveAsTextFile(hdfsDir+"buckets-of-"+args(1)+"-"+args(2))

    System.exit(0)
  }
}



