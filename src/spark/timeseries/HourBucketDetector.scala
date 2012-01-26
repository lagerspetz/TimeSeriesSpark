package spark.timeseries

/**
 * Simple class to be used with [[spark.timeseries.BucketRDD]] for detection of buckets
 * from data such as [[http://www.cs.sandia.gov/~jrstear/logs/]] by date and hour.
 * 
 * 
 * Usage:
 {{{
import spark._
import spark.SparkContext._
import spark.timeseries._

val sc = TimeSeriesSpark.init("local[1]", "default")
    
val file = sc.textFile("fileName")
val mapped = file.map(_.split(" ", 8))
    
var det = new HourBucketDetector()
val buckets = new BucketRDD(mapped, det)

buckets.cache()

val sizes = buckets.map(_.length).collect()

for(k <- sizes)
  println(k)
}}}

See [[spark.timeseries.BucketLogsByHour]] for a more real example.
 */
class HourBucketDetector extends BucketDetector[Array[String]] {
  var hour = 0
  var date = 0
  var debug = false

   /**
   * Updates `hour` and `date` and returns true if this sample belongs to a new bucket.
   * For the very first sample, returns false. Returns false in other cases.
   * @return For the very first sample, returns false. Returns true if this sample belongs to a new bucket.
   * Returns false in other cases.
   */
  def update(sample: Array[String]) = {
    val sdate = sample(5) toInt
    val shour = sample(6).substring(0, 2) toInt
    val retval = date != 0 && (sdate != date || shour != hour)
    
    if (debug && retval)
      println("date=" + date + " sdate=" + sdate + " hour=" + hour +
          " shour=" + shour + "msg=" + sample.mkString("", " ", ""))
    hour = shour
    date = sdate
    retval
  }

    /**
   * Resets the internal variables. Called in the beginning of ´compute()´ of the RunRDD.
   */
  def reset() {
    hour = 0
    date = 0
  }
}