package edu.berkeley.cs.amplab.carat

import spark.timeseries.UniformDist
import scala.collection.immutable.HashSet

/**
 * Represents a rate measurement of Carat.
 */
class CaratRate(var uuid: String, val os: String, val model: String,
  val time1: Double, val time2: Double,
  val batt1: Double, val batt2: Double,
  val events1: Seq[String], val events2: Seq[String],
  val allApps: Set[String],
  // Allow multiple values per feature using Seq.
  val features: Map[String, Seq[(String, Object)]],
  val rateRange: UniformDist = null) extends Ordered[CaratRate] with Serializable {

  def this(uuid: String, os: String, model: String, time1: Double, time2: Double, battery1: Double, battery2: Double, events1: String, events2: String, apps1: Seq[String], apps2: Seq[String],
    features: Map[String, Seq[(String, Object)]]) {
    this(uuid, os, model, time1, time2, battery1, battery2, events1.split(" "), events2.split(" "),
      {
        var k: Set[String] = new HashSet[String]
        k ++= apps1
        k ++= apps2
        k
      }, features)
  }

  def this(uuid: String, os: String, model: String, time1: Double, time2: Double, battery1: Double, battery2: Double,
    rateRng: UniformDist,
    events1: String, events2: String, apps1: Seq[String], apps2: Seq[String], features: Map[String, Seq[(String, Object)]]) {
    this(uuid, os, model, time1, time2, battery1, battery2, events1.split(" "), events2.split(" "), {
      var k: Set[String] = new HashSet[String]
      k ++= apps1
      k ++= apps2
      k
    }, features, rateRng)
  }

  def isRateRange() = rateRange != null

  /* Should not be used to get exact values. */
  def rate() = {
    // batteryDiff is between 0 and 1, negative. Multiply by -100.0 to get 0 to 100, positive.
    // The unit for rate is percent per second.
    if (isRateRange) {
      rateRange.getEv()
    } else
      (batt1 - batt2) * 100.0 / (time2 - time1)
  }

  def getTriggers() = {
    var k = new HashSet[String]
    k ++= events1
    k ++= events2
    k
  }

  def compare(that: CaratRate) = {
    if (this.rate() < that.rate())
      -1
    else if (this.rate == that.rate())
      0
    else 1
  }

  def toString(verbose: Boolean = false): String = {
    "CaratRate " + {
      if (isRateRange)
        "from %4f to %4f".format(rateRange.from, rateRange.to)
      else
        "%4f".format(rate)
    } + " time1=%4f time2=%4f batt1=%4.2f batt2=%4.2f uuid=%s os=%s model=%s events=%s %s".format(time1, time2, batt1, batt2,
      uuid, os, model, getTriggers(), {
        if (verbose)
          "apps=%s extra features: %s".format(allApps, features)
        else
          "%d apps and %d extra features".format(allApps.size, features.size)
      })
  }

  override def toString() = toString(true)
}
