package edu.berkeley.cs.amplab.carat
import scala.collection.mutable.HashSet

/**
 * Represents a rate measurement of Carat.
 */
class CaratRate(var uuid:String, val os:String, val model:String,
    val timeDiff:Double, val batteryDiff:Double,
    val events1:Seq[String], val events2:Seq[String],
    val apps1:Seq[String], val apps2:Seq[String]) extends Ordered[CaratRate] with Serializable{
  
  def this(uuid: String, os:String, model:String, time1: Long, time2:Long, battery1:Double, battery2:Double, events1:String, events2: String, apps1:String, apps2:String){
    this(uuid, os, model, time2-time1, battery2-battery1, events1.split(" "), events2.split(" "), apps1.split(" "), apps2.split(" "))
  }
  
  def this(uuid: String, os:String, model:String, time1: Long, time2:Long, battery1:Double, battery2:Double, events1:Seq[String], events2: Seq[String], apps1:Seq[String], apps2:Seq[String]){
    this(uuid, os, model, time2-time1, battery2-battery1, events1, events2, apps1, apps2)
  }
  
  def this(uuid: String, os:String, model:String, time1: Double, time2:Double, battery1:Double, battery2:Double, events1:Seq[String], events2: Seq[String], apps1:Seq[String], apps2:Seq[String]){
    this(uuid, os, model, time2-time1, battery2-battery1, events1, events2, apps1, apps2)
  }
  
  def rate() = {
    // batteryDiff is between 0 and 1, negative. Multiply by -100.0 to get 0 to 100, positive.
    // The unit for rate is percent per second.
      batteryDiff * -100.0 / timeDiff
  }
  
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
  
  override def toString() = "CaratRate rate="+rate+" uuid="+uuid+" os="+os+" model="+model+" events="+getAllEvents()+ " apps="+getAllApps()
}