package spark.timeseries.examples
import scala.collection.mutable.HashSet

class CaratRate(var uuid:String, val os:String, val model:String,
    val timeDiff:Long, val batteryDiff:Double,
    val events1:Seq[String], val events2:Seq[String],
    val apps1:Seq[String], val apps2:Seq[String]) extends Ordered[CaratRate]{
  
  def this(uuid: String, os:String, model:String, time1: Long, time2:Long, battery1:Double, battery2:Double, events1:String, events2: String, apps1:String, apps2:String){
    this(uuid, os, model, time2-time1, battery2-battery1, events1.split(" "), events2.split(" "), apps1.split(" "), apps2.split(" "))
  }
  
  def this(uuid: String, os:String, model:String, time1: Long, time2:Long, battery1:Double, battery2:Double, events1:Seq[String], events2: Seq[String], apps1:Seq[String], apps2:Seq[String]){
    this(uuid, os, model, time2-time1, battery2-battery1, events1, events2, apps1, apps2)
  }
  
  def rate() = batteryDiff * -1.0 / (timeDiff / 1000.0)
  
  def getAllApps() = {
    val k = new HashSet[String]
    k ++= apps1
    k ++= apps2
    k
  }
  
  def getAllEvents() = {
    val k = new HashSet[String]
    k ++= events1
    k ++= events2
    k
  }
  
  def compare(that:CaratRate) = {
    if (this.rate() < that.rate()) 
      -1 
    else if (this.rate == that.rate())
      0
    else 1
  }

}