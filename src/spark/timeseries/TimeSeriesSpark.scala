package spark.timeseries

import spark._
import spark.SparkContext._
import scala.collection.mutable.Queue

/**
 * Time Series Spark main class. Implements a generic time series data analysis API using [[spark.timeseries.RunRDD]] and [[scala.Array]]s of values.
 * The class includes functions for setting the caching method, creating a Spark Context, splitting up data into [[scala.Array]]s of numbers, and splitting data
 * files into buckets based on their common properties.
 * 
 * For log data examples, see [[spark.timeseries.BucketLogsByHour]].
 * 
 * Energy measurement specific example functions, such as the IdleEnergyArrayDetector class are deprecated. For current implementations, see [[spark.timeseries.IdleEnergyArrayDetector]]
 * and the [[spark.timeseries.EnergyOps]] example class.
 * 
 * A secondary API using [[spark.timeseries.MeasurementRunRDD]] restricts to the use of [[scala.Tuple3]]. This is deprecated.
 * 
 * @author Eemil Lagerspetz
 *
*/

object TimeSeriesSpark {

  /**
   * Create the SparkContext and set caching class.
   * 
   * @param master the Mesos Master, or local[number of threads] for running Spark locally.
   * @param cache The name of the caching system to use. One of "kryo" or "bounded".
   * Other values are treated as leaving the caching scheme unchanged. The null value is allowed.
   * Default: null.
   * @param sparkName The name of the application for Spark. Default: "TimeSeriesSpark". 
   * @return A new SparkContext
   */
  def init(master: String, cache: String = null, sparkName: String = "TimeSeriesSpark") = {
    setCache(cache)
    new SparkContext(master, sparkName)
  }
  
   /**
   * Set the caching class.
   * Called by init(). Use this method if you do not need a new SparkContext
   * (such as from the spark scala interpreter).
   * 
   * @param cache The name of the caching system to use. One of "kryo" or "bounded".
   * Other values are treated as leaving the caching scheme unchanged. The null value is allowed.
   * Default: null.
   */
  def setCache(cache: String = null){
   if (cache != null) {
      if (cache == "kryo") {
        println("kryo")
        System.setProperty("spark.cache.class", "spark.SerializingCache")
        System.setProperty("spark.serializer", "spark.KryoSerializer")
        System.setProperty("spark.kryoserializer.buffer.mb", "200")
        //System.setProperty("spark.kryo.registrator", classOf[MyKryoRegistrator].getName)
      }
      if (cache == "bounded") {
        println("bounded")
        System.setProperty("spark.cache.class", "spark.BoundedMemoryCache")
      }
    }
  }
  
  /**
   * Map a line of text of comma-separated numbers into an [[scala.Array]] of [[scala.Double]]s.
   * 
   * @param separator The separator (regexp). Default ","
   * @param s the line of text to split.
   */
  def genericMapper(s: String, separator:String = ",") = {
    val x = s.split(",")
    var ret = Array(0.0)
    try {
      ret = x map (_ toDouble)
    } catch {
      case e: NumberFormatException =>
      case e => { throw e }
    }
    ret
  }
  
   /**
   * Maps a supercomputer log `line` into a `date`" "`time`" "`source`,`line` - pair.
   */
  def nodeHourMapper(line:String) = {
    //- 1131524107 2005.11.09 tbird-admin1 Nov 10 00:15:07 local@tbird-admin1 
    val fmt = "yyyy.MM.dd HH" // mm:ss
    val arr = line.split(" ", 8)
    val date = arr(2)
    val time = arr(6) take 2
    //(new SimpleDateFormat(fmt).parse(date+" "+time), line)
    // date, time, node as key
    (date+" "+time+" "+arr(3), line)
  }
  
  /**
   * Split text file lines into 8 fields, separated by spaces, and bucket the lines by hour,
   * assuming that messages are sorted by date and time,
   * the 6th field is a numeric date in the month and
   * the 7th field is a time in the form `HH:MM:SS`.
   *
   * @param sc The SparkContext
   * @param fileName The path to the file with all the log messages.
   */
  def bucketLogsByNodeHour(sc: SparkContext, fileName: String) = {
    val file = sc.textFile(fileName)
    /* Assign messages to hour, node - uckets with nodeHourMapper. */
    file.map(nodeHourMapper).groupByKey()
  }

   /**
   * Map a line of text of comma-separated numbers into a (Double,Double,Double) ignoring the first on each line (measurement number).
   * 
   * @param separator The separator (regexp). Default ","
   * @param s the line of text to split.
   * 
   * 
   */
  @deprecated("Use Array functions instead.", "TimeSeriesSpark 0.3")
  def tuple3Mapper(s: String): (/*Int,*/ Double, Double, Double) = {
    val x = s.split(",")
    try{
      /* val num = x(0).toInt*/
      val time = x(1).toDouble
      val ma = x(2).toDouble
      val volt = x(3).toDouble
      return (/*num,*/ time, ma, volt)
    } catch {
      case e:NumberFormatException => return (/*0,*/0,0,0)
      case e => { throw e }
    }
    return (/*0,*/0,0,0)
  }

  /**
   * Map a current and voltage pair into their product.
   * 
   * @param time the time, in s
   * @param amp the current, in mA
   * @param volt the voltage, in volts
   * @return amp*volt
   */
  @deprecated("Use Array functions instead.", "TimeSeriesSpark 0.3")
  def energymapper(time:Double, amp:Double, volt:Double): Double = {
    return amp*volt
  }

  /**
   * Detect periods of activity from a data file of [[scala.Tuple3]] of [[scala.Double]]s where each Tuple consists of time, current, and voltage values.
   * 
   * Detection of activity is based on the average power (in mW) being above
   * a given threshold <code>idle_thresh</code>. The average is calculated over
   * a period of <code>windowLength</code> tuples.
   *   @param idle_thresh The power threshold above which values are considered activity.
   *   @param windowLength The number of tuples that averages are calculated over, and compared to <code>idle_thresh</code>.
   */
  @deprecated("Use IdleEnergyArrayDetector class instead.", "TimeSeriesSpark 0.3")
  class IdleEnergyTupleDetector(idle_thresh: Double, windowLength: Int) extends MeasurementRunDetector[(Double, Double, Double)] {
    var q = new Queue[(Double, Double, Double)]()
    var sum = 0.0
    var readyDelay = 0

    def reset() {
      q = new Queue[(Double, Double, Double)]()
      sum = 0.0
    }

    def inRun(): Boolean = {
      if (q.size < windowLength)
        false
      else
        sum / q.size >= idle_thresh
    }

    def ready() = { readyDelay == 0 && q.size >= windowLength }

    def update(sample: (Double, Double, Double)) {
      q += sample
      if (q.size > windowLength) {
        var prev = q.dequeue()
        sum -= prev._2 * prev._3
      }
      sum += sample._2 * sample._3
      if (readyDelay > 0)
        readyDelay -= 1
    }

    def prepend() = q

    def splitChanged() {
      readyDelay = windowLength
    }
  }
}