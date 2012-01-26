package spark

/**
 * The main package for TimeSeriesSpark.
 * 
 * See the [[spark.timeseries.TimeSeriesSpark]] object for the TimeSeriesSpark API.
 * The API depends on the [[spark.timeseries.RunRDD]] that splits a data file into
 * interesting partitions using a [[spark.timeseries.RunDetector]].
 * The partitions are represented by [[spark.timeseries.Run]]s contained in the [[spark.timeseries.RunRDD]].
 * 
 *  Usage example that needs no data files:
 *  {{{
 *  scala> import spark._
 *  import spark._
 *  
 *  scala> import spark.SparkContext._
 *  import spark.SparkContext._
 *  
 *  scala> import spark.timeseries._
 *  import spark.timeseries.TimeSeriesSpark
 *  
 *  scala> import scala.collection.mutable.Queue
 *  import scala.collection.mutable.Queue
 *  
 *  val sc = new SparkContext("local", "TimeSeriesSparkTest")
 *  
 *  scala> val data = sc.parallelize(
     |       List(0, 0, 0, 0, 0, 2, 2, 2, 2, 2,
     |         0, 0, 0, 0, 0, 2, 2, 2, 2, 2,
     |         2, 2, 2, 2, 2, 0, 2, 0, 0, 0),
     |       4)
data: spark.RDD[Int] = spark.ParallelCollection@119db21f

scala> val dataPartitions = data.glom.collect
11/11/28 16:53:41 INFO spark.SparkContext: Starting job...
11/11/28 16:53:41 INFO spark.CacheTracker: Registering RDD ID 1 with cache
11/11/28 16:53:41 INFO spark.CacheTrackerActor: Registering RDD 1 with 4 partitions
11/11/28 16:53:41 INFO spark.CacheTracker: Registering RDD ID 0 with cache
11/11/28 16:53:41 INFO spark.CacheTrackerActor: Registering RDD 0 with 4 partitions
11/11/28 16:53:41 INFO spark.CacheTrackerActor: Asked for current cache locations
11/11/28 16:53:41 INFO spark.LocalScheduler: Final stage: Stage 0
11/11/28 16:53:41 INFO spark.LocalScheduler: Parents of final stage: List()
11/11/28 16:53:41 INFO spark.LocalScheduler: Missing parents: List()
11/11/28 16:53:41 INFO spark.LocalScheduler: Submitting Stage 0, which has no missing parents
11/11/28 16:53:42 INFO spark.LocalScheduler: Running task 0
11/11/28 16:53:42 INFO spark.LocalScheduler: Size of task 0 is 1619 bytes
11/11/28 16:53:42 INFO spark.LocalScheduler: Finished task 0
11/11/28 16:53:42 INFO spark.LocalScheduler: Running task 1
11/11/28 16:53:42 INFO spark.LocalScheduler: Size of task 1 is 1623 bytes
11/11/28 16:53:42 INFO spark.LocalScheduler: Finished task 1
11/11/28 16:53:42 INFO spark.LocalScheduler: Running task 2
11/11/28 16:53:42 INFO spark.LocalScheduler: Size of task 2 is 1619 bytes
11/11/28 16:53:42 INFO spark.LocalScheduler: Finished task 2
11/11/28 16:53:42 INFO spark.LocalScheduler: Running task 3
11/11/28 16:53:42 INFO spark.LocalScheduler: Size of task 3 is 1623 bytes
11/11/28 16:53:42 INFO spark.LocalScheduler: Finished task 3
11/11/28 16:53:42 INFO spark.LocalScheduler: Completed ResultTask(0, 0)
11/11/28 16:53:42 INFO spark.LocalScheduler: Completed ResultTask(0, 1)
11/11/28 16:53:42 INFO spark.LocalScheduler: Completed ResultTask(0, 2)
11/11/28 16:53:42 INFO spark.LocalScheduler: Completed ResultTask(0, 3)
11/11/28 16:53:42 INFO spark.SparkContext: Job finished in 0.389642796 s
dataPartitions: Array[Array[Int]] = Array(Array(0, 0, 0, 0, 0, 2, 2), Array(2, 2, 2, 0, 0, 0, 0, 0), Array(2, 2, 2, 2, 2, 2, 2), Array(2, 2, 2, 0, 2, 0, 0, 0))

class Det(windowLength: Int) extends RunDetector[Int] {
  var q: Queue[Array[Int]] = new Queue[Array[Int]]()
  var sum = 0.0
  var readyDelay = 0
  
  def reset() {
    q = new Queue[Array[Int]]()
    sum = 0.0
  }

  def inRun(): Boolean = {
    sum / q.size >= 1
  }

  def update(k: Array[Int]){
    q += k
    if (q.size > windowLength)
      sum -= q.dequeue()(0)
    sum += k(0)
    if (readyDelay > 0)
      readyDelay-=1
  }

  def ready () = { readyDelay == 0 && q.size >= windowLength }

  def prepend() = q
  
  def splitChanged(){
    readyDelay = windowLength
  }
}
    
scala> val detector = new Det(2)
detector: this.Det = Det@57830ec7

scala> def mapper(i: Int) = {
     |       Array(i)
     |     }
mapper: (i: Int)Array[Int]
    
scala> val mapped = data.map(mapper)
mapped: spark.RDD[Array[Int]] = spark.MappedRDD@1239752
   
scala> val runs = new RunRDD[Int](mapped, detector)
runs: spark.timeseries.RunRDD[Int] = spark.timeseries.RunRDD@220860ba

scala> val runsPartitions = runs.glom.collect
11/11/28 16:58:21 INFO spark.SparkContext: Starting job...
11/11/28 16:58:21 INFO spark.CacheTracker: Registering RDD ID 4 with cache
11/11/28 16:58:21 INFO spark.CacheTrackerActor: Registering RDD 4 with 4 partitions
11/11/28 16:58:21 INFO spark.CacheTracker: Registering RDD ID 3 with cache
11/11/28 16:58:21 INFO spark.CacheTrackerActor: Registering RDD 3 with 4 partitions
11/11/28 16:58:21 INFO spark.CacheTracker: Registering RDD ID 2 with cache
11/11/28 16:58:21 INFO spark.CacheTrackerActor: Registering RDD 2 with 4 partitions
11/11/28 16:58:21 INFO spark.CacheTrackerActor: Asked for current cache locations
11/11/28 16:58:21 INFO spark.LocalScheduler: Final stage: Stage 1
11/11/28 16:58:21 INFO spark.LocalScheduler: Parents of final stage: List()
11/11/28 16:58:21 INFO spark.LocalScheduler: Missing parents: List()
11/11/28 16:58:21 INFO spark.LocalScheduler: Submitting Stage 1, which has no missing parents
11/11/28 16:58:21 INFO spark.LocalScheduler: Running task 0
11/11/28 16:58:21 INFO spark.LocalScheduler: Size of task 0 is 5069 bytes
11/11/28 16:58:21 INFO spark.LocalScheduler: Finished task 0
11/11/28 16:58:21 INFO spark.LocalScheduler: Running task 1
11/11/28 16:58:21 INFO spark.LocalScheduler: Completed ResultTask(1, 0)
11/11/28 16:58:21 INFO spark.LocalScheduler: Size of task 1 is 5069 bytes
11/11/28 16:58:21 INFO spark.LocalScheduler: Finished task 1
11/11/28 16:58:21 INFO spark.LocalScheduler: Running task 2
11/11/28 16:58:21 INFO spark.LocalScheduler: Size of task 2 is 5069 bytes
11/11/28 16:58:21 INFO spark.LocalScheduler: Completed ResultTask(1, 1)
11/11/28 16:58:21 INFO spark.LocalScheduler: Finished task 2
11/11/28 16:58:21 INFO spark.LocalScheduler: Completed ResultTask(1, 2)
11/11/28 16:58:21 INFO spark.LocalScheduler: Running task 3
11/11/28 16:58:21 INFO spark.LocalScheduler: Size of task 3 is 5069 bytes
11/11/28 16:58:21 INFO spark.LocalScheduler: Finished task 3
11/11/28 16:58:21 INFO spark.LocalScheduler: Completed ResultTask(1, 3)
11/11/28 16:58:21 INFO spark.SparkContext: Job finished in 0.075984605 s
runsPartitions: Array[Array[spark.timeseries.Run[Int]]] = Array(Array(0, 2, 2, 2, 2, 0), Array(2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 2, 0), Array(), Array())

scala> println("Partitions of data:")
Partitions of data:

scala> dataPartitions.foreach(x => println(x.mkString("[", ",", "]")))
[0,0,0,0,0,2,2]
[2,2,2,0,0,0,0,0]
[2,2,2,2,2,2,2]
[2,2,2,0,2,0,0,0]

scala> println("Runs:")
Runs:

scala> runsPartitions.foreach(x => println(x.mkString))
0, 2, 2, 2, 2, 0
2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 2, 0



 *  
 *  }}}
 *  
 *  
 * A more "real" usage example that needs a csv data file can be found below.
 * The data file should contain lines like these:
{{{
num,time,current,voltage
1,0.0002,12.374,3.9925
2,0.0004,13.278,3.9925
3,0.0006,12.69,3.993
4,0.0008,12.84,3.993
5,0.001,12.686,3.993
}}}

The example is as follows:
 *  {{{
scala> import spark._
import spark._

scala> import spark.SparkContext._
import spark.SparkContext._

scala> import spark.timeseries._
import spark.timeseries.TimeSeriesSpark

scala> val master = "local[1]"
master: java.lang.String = local[1]

scala> val sc = TimeSeriesSpark.init(master)
11/11/28 16:27:13 INFO spark.CacheTrackerActor: Registered actor on port 50501
11/11/28 16:27:13 INFO spark.MapOutputTrackerActor: Registered actor on port 50501
sc: spark.SparkContext = spark.SparkContext@56d115db

scala> val fileName = "../d-wlan-71MB-dispoff-5runs-600MHz-4V.csv"
fileName: java.lang.String = ../d-wlan-71MB-dispoff-5runs-600MHz-4V.csv

scala> val file = sc.textFile(fileName)
11/11/28 16:31:10 INFO mapred.FileInputFormat: Total input paths to process : 1
file: spark.RDD[String] = spark.MappedRDD@3ce3cd45

scala> val mapped = file.map(TimeSeriesSpark.genericMapper(_, ","))
mapped: spark.RDD[Array[Double]] = spark.MappedRDD@575b2f17

scala> val idlethresh=500
idlethresh: Int = 500

scala> var det = new IdleEnergyArrayDetector(idlethresh, 2500)
det: spark.timeseries.IdleEnergyArrayDetector = spark.timeseries.IdleEnergyArrayDetector@761b2f32

scala> val runs = new RunRDD(mapped, det)
runs: spark.timeseries.RunRDD[Double] = spark.timeseries.RunRDD@191d7bd5

scala>   def ArrayRunMapper(run: Run[Double]) = {
     |     var energy = 0.0
     |     var last = 0.0
     |     var duration = 0.0
     |     var start = 0.0
     |     for (k <- run.list) {
     |       if (last == 0) {
     |         last = k(1)
     |         start = last
     |       } else {
     |         var step = k(1) - last
     |         if (step > 0.000201 || step < 0.000199) {
     |           println("weird step from " + last + " to " + k(1) + ": " + step)
     |         } else {
     |           duration += 0.0002
     |           energy += 0.0002 * k(2) * k(3)
     |         }
     |         last = k(1)
     |       }
     |     }
     |     (start, last, duration, energy / 1000.0 / 3600.0)
     |   }
ArrayRunMapper: (run: spark.timeseries.Run[Double])(Double, Double, Double, Double)

scala> val reduced = runs.map(ArrayRunMapper)
reduced: spark.RDD[(Double, Double, Double, Double)] = spark.MappedRDD@638c6b76

scala> reduced.cache()
res1: spark.RDD[(Double, Double, Double, Double)] = spark.MappedRDD@638c6b76

scala> val str = reduced.map(k => { k._1 + ", " + k._2 + ", " + k._3 + " s, " + k._4 + " Wh" })
str: spark.RDD[java.lang.String] = spark.MappedRDD@75775dde

str.saveAsTextFile("runs-of-csv.csv-"+idlethresh)
}}}
 *  
 */

package object timeseries {

}
