package spark.timeseries

import scala.collection.mutable.Queue

/** Put Linux log messages like the ones available at [[http://www.cs.sandia.gov/~jrstear/logs/]]
 * into buckets based on the hour they occurred in. Each message is considered a
 * [[scala.Array]] of [[scala.String]], of length 8, where field arr(6) is the time.
 
Usage:
 {{{
import spark._
import spark.SparkContext._
import spark.timeseries._


val sc = TimeSeriesSpark.init("local[1]", "default")
    
val file = sc.textFile("fileName")
val mapped = file.map(_.split(" ", 8))
    
var det = new TimeBucketDetector()
val buckets = new RunRDD(mapped, det)

buckets.cache()

val sizes = buckets.map(_.length).collect()

for(k <- sizes)
  println(k)
 
}}}

Single message buckets are currently dropped. This is because of the way that RunRDD works.
Fix pending.
{{{
//(number of all messages)
sum: Int = 211194343
//(expected number)
scala> val real = 211212192
real: Int = 211212192

scala> sum - real
res1: Int = 17849
}}}

Single message bucket call sequence:
{{{

detector.update(k) // single item run
detector.ready()
detector.inRun() // false
detector.update(k) // start of a longer run
detector.ready()
detector.inRun() // false <- should be set true to dequeue single item run?
detector.update(k) // longer run continued
detector.inRun()
detector.inRun() // true
detector.prepend()
detector.update(k)
}}}

*/

@deprecated("Use HourBucketDetector instead.", "TimeSeriesSpark 0.3")
class TimeBucketDetector() extends RunDetector[String] {
  var qs = new Queue[Queue[Array[String]]]()
  var q = new Queue[Array[String]]()
  //qs+=q
  var hour = 0
  var date = 0
  var isinRun = true
  var skipRun = false
  var singleBucketDequeue = false
  var singleBucketDequeued = false
  /**
   * Resets the internal queue and other variables. Called in the beginning of ´compute()´ of the RunRDD.
   */
  def reset() {
    q = new Queue[Array[String]]()
    hour = 0
    date = 0
    isinRun = true
    skipRun = false
    singleBucketDequeue = false
    singleBucketDequeued = false
    //println("reset called")
  }

  /**
   * Returns whether we are in a run.
   * 
   * For supercomputer logs, this is false when the hour changes, and true otherwise.
   */
  def inRun() ={
    //println("inRun(): " + isinRun)
    isinRun
  }

  /**
   * Appends `sample` to the internal queue and updates `hour`.
   *
   * Also discards the queue if the hour changes, to not append messages to multiple buckets.
   */
  def update(sample: Array[String]) {
    val time = sample(6)
    val sdate = sample(5) toInt
    val shour = time.substring(0,2) toInt
    val minute = time.substring(3,5) toInt

    if (hour != shour || date != sdate){
      // new hour, first one has been skipped
      skipRun = false
      isinRun = false
      q = new Queue[Array[String]]()
      q += sample
      qs += q
    } else if (skipRun){
      isinRun = false
    } else {
      /* First after not in run is skipped by compute(), so need to store 2. */
      if (q.length == 1 || singleBucketDequeued)
        q+= sample
      isinRun = true
    }
    
    /* Fix skip of 1 item runs when detecting a start of a new run: */
    if ((hour != shour || date != sdate) && qs.length > 0 && qs(0).length == 1 && qs(0) != q && !singleBucketDequeue){
        isinRun = true // indicate 1 item run
        singleBucketDequeue = true
    }else if (singleBucketDequeue){
        isinRun = false
        singleBucketDequeue = false
        singleBucketDequeued = true
    } else if (singleBucketDequeued){
        isinRun = true // resume
        singleBucketDequeued = false
    }
    
    
    /* How about dequeue of longer runs? This should happen the next time that a 
    run finishes. */
    
    hour = shour
    date = sdate
    /*if (qs.length > 0){
        print("Queue contents: ")
        for (k <- qs){
            for (j <- k)
                println(j.mkString("[", ", ", "]"))
            println()
        }
        println()
    }*/
    
    /*println("update() date=" + date + " hour=" + hour + " isinRun="+isinRun
    + " skipRun=" + skipRun + " singleBucketDequeue=" + singleBucketDequeue
    +" singleBucketDequeued=" + singleBucketDequeued)*/
  }

  /**
   * Returns the internal queue. Used to prepend items to runs that were detected later than their actual start time.
   */
  def prepend() = {
      /*print("prepend returning: ")
      for(j <- qs(0))
        println(j.mkString("[", ", ", "]"))*/
      qs.dequeue()
  }


  override def splitChanged(){
    skipRun = true
  }
}
