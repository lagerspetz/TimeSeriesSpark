package spark.timeseries.examples


import spark._
import spark.SparkContext._
import spark.timeseries._

/**
 * This example program shows some operations that can be performed
 * with the TimeSeriesSpark API on supercomputer log data.
 */
object SuperComputerLogs {
  
    /**
   * General grep function. Matches log messages against the given regexp.
   * 
   * Entire code:
   * {{{
   * buckets.filter(_.mkString(" ") matches regex)
   * }}}
   * 
   * Usage:
   * {{{
   * import spark._
   * import spark.SparkContext._
   * import spark.timeseries._
   *
   * /** tbird datafile: */
   * val hdfsDir = "hdfs://server.name.org:54310/user/hadoop/"
   * val fileName = "tbird-tagged"
   *
   * /** The Mesos master server to use: */
   * val mesosMaster = "mesos://master@host.name.org:5050"
   *
   * /** Initialize Spark and set caching class: */
   * val sc = TimeSeriesSpark.init("mesosMaster")
   *
   * /** bucket by hour */
   * val buckets = TimeSeriesSpark.bucketLogsByHour(sc, hdfsDir+fileName)
   * 
   * val icasePanic = "(?i).*panic.*"
   *
   * val alerts = grep(buckets, icasePanic)
   * 
   * for (k <- alerts) {
   *   println(k(0).mkString(" "))
   * }
   * }}}
   * 
   * 
   * @param buckets The BucketRDD whose messages are examined.
   * @param regex The regular expression that log messages are matched against.
   */
  
  def grep(buckets: RDD[(java.lang.String, Seq[String])], regex: String) = {
    buckets.filter(_._2.mkString(" ") matches regex)
  }

  /**
   * Returns the `howmany` largest buckets of the data.
   * 
   * Entire code:
   * {{{
   * val sizes = buckets.map(_.length).collect()

    import scala.collection.mutable.ArrayBuffer
    var largest = new ArrayBuffer[Int]

    for (i <- sizes) {
      if (largest.isEmpty || i >= (largest sorted).head)
        largest += i
      if (largest.length > howmany)
        largest = (largest sorted) drop 1
    }

    val threshold = (largest sorted).head

    // Take the large buckets only
    buckets.filter(_.length >= threshold)
   * }}}
   * 
   * Usage:
   * {{{
    import spark._
    import spark.SparkContext._
    import spark.timeseries._
    import spark.timeseries.examples._
   
    /** tbird datafile: */
    val hdfsDir = "hdfs://server.name.org:54310/user/hadoop/"
    val fileName = "tbird-tagged"
   
    /** The Mesos master server to use: */
    val mesosMaster = "mesos://master@host.name.org:5050"
   
    /** Initialize Spark and set caching class: */
    val sc = TimeSeriesSpark.init("mesosMaster")
   
    /** bucket by hour */
    val buckets = TimeSeriesSpark.bucketLogsByHour(sc, hdfsDir+fileName)
    
    val howmany = 10
    
    val largest = SuperComputerLogs.largestBuckets(buckets, howmany)
    
    val largestBuckets = largest.collect()
    
    println("Largest buckets:")
    for (k <- largestBuckets) {
      println(k.length)
    }
   * }}}
   * 
   * @param buckets The BucketRDD whose messages are examined.
   * @param howmany The number of buckets to return.
   * @return the largest buckets of the data. May return more than `howmany` buckets
   * if there are multiple buckets with the smallest included size.
   */
  
  def largestBuckets(buckets: RDD[(java.lang.String, Seq[String])], howmany: Int) = {
    val sizes = buckets.map(_._2.length).collect()

    import scala.collection.mutable.ArrayBuffer
    var largest = new ArrayBuffer[Int]

    for (i <- sizes) {
      if (largest.isEmpty || i >= (largest sorted).head)
        largest += i
      if (largest.length > howmany)
        largest = (largest sorted) drop 1
    }

    val threshold = (largest sorted).head

    // Take the large buckets only
    buckets.filter(_._2.length >= threshold)
  }

  /**
   * Look for time buckets with abnormally large numbers of messages.
   * Finds and saves to a HDFS file abnormally large time buckets.
   * 
   * Entire code:
   * {{{
    // Take the large buckets only
    val abnormal = buckets.filter(_.length >= threshold)
    
    // Examine their sizes
    val sizes = abnormal.map(_.length).collect()

    var sum = 0
    var i = 0
    var largest = 0
    var largestIndex = 0
    while (i < sizes.length) {
      println((i + 1) + " " + sizes(i))
      sum += sizes(i)
      if (sizes(i) > largest) {
        largestIndex = i
        largest = sizes(i)
      }
      i += 1
    }
    println("Total " + sum + " messages in " + i + " buckets, largest bucket " + largest)

    println("Saving largest bucket(s) of size " + largest + " as text")
    val largestbuckets = abnormal.filter(_.length >= largest)
    val strbuckets = largestbuckets.map(x => { var str = ""; for (k <- x) { str += k.mkString(" "); str += "\n" }; str += "\n"; str })
    strbuckets.saveAsTextFile(hdfsDir+"largest-buckets")
   * }}}
   * 
   * Usage:
   * {{{
   * import spark._
   * import spark.SparkContext._
   * import spark.timeseries._
   *
   * /** tbird datafile: */
   * val hdfsDir = "hdfs://server.name.org:54310/user/hadoop/"
   * val fileName = "tbird-tagged"
   *
   * /** The Mesos master server to use: */
   * val mesosMaster = "mesos://master@host.name.org:5050"
   *
   * /** Initialize Spark and set caching class: */
   * val sc = TimeSeriesSpark.init("mesosMaster")
   *
   * /** bucket by hour */
   * val buckets = TimeSeriesSpark.bucketLogsByHour(sc, hdfsDir+fileName)
   * 
   * val threshold = 100000
   * 
   * //Save abnormally large buckets:
   * SuperComputerLogs.abnormallyLargeBuckets(hdfsDir, buckets, threshold)
   * }}}
   * 
   * 
   * @param hdfsDir The HDFS directory to save the abnormally large time buckets files to.
   * @param buckets The BucketRDD whose messages are examined.
   * @param threshold the number of messages considered abnormally large.
   */

  def abnormallyLargeBuckets(hdfsDir:String, buckets: RDD[(java.lang.String, Seq[String])], threshold: Int = 100000) {
    /**
     * Take the large buckets only
     */
    val abnormal = buckets.filter(_._2.length >= threshold)
    
    /**
     * Examine their sizes
     */
    val sizes = abnormal.map(_._2.length).collect()

    var sum = 0
    var i = 0
    var largest = 0
    var largestIndex = 0
    while (i < sizes.length) {
      println((i + 1) + " " + sizes(i))
      sum += sizes(i)
      if (sizes(i) > largest) {
        largestIndex = i
        largest = sizes(i)
      }
      i += 1
    }
    println("Total " + sum + " messages in " + i + " buckets, largest bucket " + largest)

    println("Saving largest bucket(s) of size " + largest + " as text")
    val largestbuckets = abnormal.filter(_._2.length >= largest)
    val strbuckets = largestbuckets.map(x => { var str = ""; for (k <- x._2) { str += k.mkString(" "); str += "\n" }; str += "\n"; str })
    strbuckets.saveAsTextFile(hdfsDir+"largest-buckets")
  }

  /**
   * Find messages within a particular time window.
   *
   * Entire code:
   * {{{
   * buckets.filter(x => {
   * val xmonth = x(0)(5)
   * val xdate = x(0)(6) toInt
   * val xhour = x(0)(7).substring(0, 2) toInt
   *
   * dayrange(0) < xdate && dayrange(1) >= xdate &&
   * hourrange(0) < xhour && hourrange(1) >= xhour &&
   * xmonth == month
   * })
   * }}}
   *
   *
   * Usage:
   * {{{
   * import spark._
   * import spark.SparkContext._
   * import spark.timeseries._
   *
   * /** tbird datafile: */
   * val hdfsDir = "hdfs://server.name.org:54310/user/hadoop/"
   * val fileName = "tbird-tagged"
   *
   * /** The Mesos master server to use: */
   * val mesosMaster = "mesos://master@host.name.org:5050"
   *
   * /** Initialize Spark and set caching class: */
   * val sc = TimeSeriesSpark.init("mesosMaster")
   *
   * /** bucket by hour */
   * val buckets = TimeSeriesSpark.bucketLogsByHour(sc, hdfsDir+fileName)
   *
   * // get the messages of the time period
   * val dec10to20workinghours = SuperComputerLogs.messagesWithin(buckets, "Dec", Array(10,20), Array(8,17)).cache()
   *
   * // detect interesting properties in them
   * val interesting = dec10to20workinghours.filter(...)
   *
   * for (k <- interesting){
   * println(k.mkString(" ")
   * }
   * }}}
   * 
   * @param buckets The BucketRDD whose messages are examined.
   * @param month the month to find messages in. Use three letter abbreviation, such as `Dec`.
   * @param dayrange the range of days for log messages. Use `Array(10, 20)` for example.
   * @param hourrange the range of hours of log messages. Use `Array(8, 17)` for example.
   * @return an RDD of Arrays of log messages within the time window.
   */

  def messagesWithin(buckets: RDD[(java.lang.String, Seq[String])], month: String = "Dec", dayrange: Array[Int] = Array(10, 20), hourrange: Array[Int] = Array(8, 17)) = {
    buckets.filter(x => {
      val fields = x._2(0).split(" ", 8)
      val xmonth = fields(5)
      val xdate = fields(6) toInt
      val xhour = fields(7).substring(0, 2) toInt

      dayrange(0) < xdate && dayrange(1) >= xdate &&
        hourrange(0) < xhour && hourrange(1) >= xhour &&
        xmonth == month
    })
  }

 /**
   * Find messages from a particular source.
   *
   * Entire code:
   * {{{
   *   buckets.filter(_(0)(3) == src)
   * })
   * }}}
   *
   *
   * Usage:
   * {{{
   * import spark._
   * import spark.SparkContext._
   * import spark.timeseries._
   *
   * /** tbird datafile: */
   * val hdfsDir = "hdfs://server.name.org:54310/user/hadoop/"
   * val fileName = "tbird-tagged"
   *
   * /** The Mesos master server to use: */
   * val mesosMaster = "mesos://master@host.name.org:5050"
   *
   * /** Initialize Spark and set caching class: */
   * val sc = TimeSeriesSpark.init("mesosMaster")
   *
   * /** bucket by hour */
   * val buckets = TimeSeriesSpark.bucketLogsByHour(sc, hdfsDir+fileName)
   *
   * // source:
   * val src = "en263"
   *
   * // get the messages from the source
   * val all_from_en263= SuperComputerLogs.messagesWithin(buckets, src).cache()
   *
   * // detect interesting properties in them
   * val interesting = all_from_en263.filter(...)
   *
   * for (k <- interesting){
   * println(k.mkString(" ")
   * }
   * }}}
   * 
   * @param buckets The BucketRDD whose messages are examined.
   * @param sorce for messages, such as `en263`.
   * @return an RDD of Arrays of log messages within the time window.
   */

  def messagesFrom(buckets: RDD[(java.lang.String, Seq[String])], src: String = "en263") = {
    buckets.filter(_._1.split(" ").last == src)
  }

  /**
   * Filters buckets based on two regexps, `.*kernel: ACPI.*` and `(?i).*panic.*`,
   * resulting in messages from the ACPI subsystem and those containing the
   * word PANIC in lower, upper, or mixed case.
   * 
   * Entire code:
   * {{{
    import scala.util.matching.Regex

    val kernelacpi = """.*kernel: ACPI.*""".r

    val icasePanic = """(?i).*panic.*""".r

    val matched = buckets.filter(x => x match {
      case kernelacpi() => {
        // do more processing here
        true
      }
      case icasePanic() => {
        println("DEBUG: \"" + x + "\" matches regex.")
        // do more processing here
        true
      }
      case _ => false
    })
    matched
   * }}}
   * 
   * Usage:
   * {{{
   * import spark._
   * import spark.SparkContext._
   * import spark.timeseries._
   *
   * /** tbird datafile: */
   * val hdfsDir = "hdfs://server.name.org:54310/user/hadoop/"
   * val fileName = "tbird-tagged"
   *
   * /** The Mesos master server to use: */
   * val mesosMaster = "mesos://master@host.name.org:5050"
   *
   * /** Initialize Spark and set caching class: */
   * val sc = TimeSeriesSpark.init("mesosMaster")
   *
   * /** bucket by hour */
   * val buckets = TimeSeriesSpark.bucketLogsByHour(sc, hdfsDir+fileName)
   *
   * // Detect alerts:
   * val alerts = SuperComputerLogs.detectAlerts(buckets)
   * // print only the first one that matches from each hour bucket:
   * 
   * for (k <- alerts) {
   *   println(k(0).mkString(" "))
   * }
   * }}}
   * 
   * Not done yet:
   * sophisticated alert detection
   * alert prediction (are there patterns of non-alert messages that tend to precede alerts?)
   * 
   * @param buckets The BucketRDD whose messages are examined.
   */

  def detectAlerts(buckets: RDD[(java.lang.String, Seq[String])]) {
    import scala.util.matching.Regex

    val kernelacpi = """.*kernel: ACPI.*""".r

    val icasePanic = """(?i).*panic.*""".r

    val matched = buckets.filter(x => x._2.mkString(" ") match {
      case kernelacpi() => {
        // do more processing here
        true
      }
      case icasePanic() => {
        println("DEBUG: \"" + x + "\" matches regex.")
        // do more processing here
        true
      }
      case _ => false
    })
    matched
  }

   /**
   * Main program entry point. Runs Spark with the master `args(0)`. Assigns log messages stored in the file `args(1)`
   * into buckets by source node and wall clock hour. 
   * Saves the 10 largest buckets to a file
   * in the same directory as the input file.  
   * 
   * `args(2)` needs to be specified, but is currently unused. In the future,
   * it may be used to set the size of a bucket in minutes.
   * 
   * @param args Command line arguments.
   */
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: SuperComputerLogs master filename\n" +
        "Example: AlertRuns local[2] tbird-tagged")
      return
    }
    var slash = args(1).lastIndexOf("/")+1
    if (slash == -1)
      slash = 0
    val filename = args(1).substring(slash)
    val hdfsDir = args(1).substring(0, slash)
    val sc = TimeSeriesSpark.init(args(0), "default")
    
    val buckets = TimeSeriesSpark.bucketLogsByNodeHour(sc, hdfsDir+filename) 
    
    val howmany = 10
    
    //Save abnormally large buckets:
    val largest = SuperComputerLogs.largestBuckets(buckets, howmany)
    largest.map(_._2.mkString).saveAsTextFile(hdfsDir+howmany +"largest-buckets-of-"+args(1))
    
    System.exit(0)
  }
}
