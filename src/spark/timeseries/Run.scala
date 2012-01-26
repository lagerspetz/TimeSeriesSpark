package spark.timeseries
import spark._
import spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue


/**
 * A Class that represents a measurement run, containing the measurements inside.
Usage:
 *{{{
import scala.collection.mutable.ArrayBuffer
import spark.timeseries.Run

var data = new ArrayBuffer[Array[Double]]
data += Array(0.0,0.1,0.2,0.3,0.4)
data += Array(1.0,1.1,1.2,1.3,1.4)
val run = new Run[Double](data.toArray: _*)
for (k <- run.list) {
  println(k.mkString("[", ", ", "]"))
}
}}}
 * @constructor Creates a new Run with the specified lines of data.
 */
class Run[T: ClassManifest](var list: Array[T]*) {
  override def toString() = {
    var str = ""
    var first = true
    for (k <- list){
      if (first){ first = false} else{
        str += ", "
      }
      str += k.mkString("", " ", "")
    }
    if (str == "") {
      str = "[]"
    }
    str
  }
  
  def length = list.length

}

