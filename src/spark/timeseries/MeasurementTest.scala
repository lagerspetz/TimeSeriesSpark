package spark.timeseries

import spark._
import spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue

object MeasurementTest {
  
  @deprecated("Use Array functions instead", "TimeSeriesSpark 0.3")
  def old(sc: SparkContext){
    println("old method")
    val data = sc.parallelize(
        List(0, 0, 0, 0, 0, 2, 2, 2, 2, 2,
             0, 0, 0, 0, 0, 2, 2, 2, 2, 2,
             2, 2, 2, 2, 2, 0, 2, 0, 0, 0),
        4)
    val dataPartitions = data.glom.collect
    

    class Det(windowLength: Int) extends MeasurementRunDetector[Int]{
      var q: Queue[Int] = new Queue[Int]()
      var sum = 0.0
      var readyDelay = 0
      
      def reset() {
        println("Detector reset")
        q = new Queue[Int]()
        sum = 0.0
      }

      def inRun(): Boolean = {
        //println("inRun(): " + sum + " / " +q.size + " >= 1 = " + (sum / q.size >= 1))
        sum / q.size >= 1
      }

      def update(k: Int){
        q += k
        if (q.size > windowLength)
          sum -= q.dequeue()
        sum += k
        if (readyDelay > 0)
          readyDelay-=1
      }

      def ready () = { readyDelay == 0 && q.size >= windowLength }

      def prepend() = q
      
      def splitChanged(){
        //println("splitChanged")
        readyDelay = windowLength   
      }
    }
    
    var detector = new Det(2)
    
    val runs = new MeasurementRunRDD(data, detector)
    val runsPartitions = runs.glom.collect

    println("Partitions of data:")
    dataPartitions.foreach(x => println(x.mkString("[", ",", "]")))

    println("Partitions of runs:")
    runsPartitions.foreach(x => println(x.map(_.mkString).mkString("[", ",", "]")))
  }

  def runs(sc: SparkContext) {
    println("new method")

    val data = sc.parallelize(
      List(0, 0, 0, 0, 0, 2, 2, 2, 2, 2,
        0, 0, 0, 0, 0, 2, 2, 2, 2, 2,
        2, 2, 2, 2, 2, 0, 2, 0, 0, 0),
      4)
    val dataPartitions = data.glom.collect

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

      override def ready () = { readyDelay == 0 && q.size >= windowLength }

      def prepend() = q
      
      override def splitChanged(){
        readyDelay = windowLength
      }
    }
    
    val detector = new Det(2)
    
    def mapper(i: Int) = {
      Array(i)
    }
    
    val mapped = data.map(mapper)
    val runs = new RunRDD[Int](mapped, detector)
    val runsPartitions = runs.glom.collect

    println("Partitions of data:")
    dataPartitions.foreach(x => println(x.mkString("[", ",", "]")))

    println("Runs:")
    runsPartitions.foreach(x => println(x.mkString))
  }
  
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "MeasurementTest")
    //old(sc)
    runs(sc)
  }
}

