package spark.timeseries.examples

import spark._
import spark.SparkContext._
import spark.timeseries._
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import java.util.Date
import java.io.FileWriter
import scala.collection.Seq
import scala.collection.mutable.HashSet
import scala.collection.immutable.TreeMap
import scala.collection.immutable.SortedMap
import java.io.ObjectOutputStream

/**
 * A program for loading values off S3 into distributions produced by S3DataAnalysis.
 * 
 * Usage:
 * {{{
 *  CaratFakeDataAnalysis.main(Array(""))
 * }}}
 */

object S3DataLoader {

  /**
   * Main program entry point.
   */
  def main(args: Array[String]) {
    loadData()
    exit(0)
  }

  def loadData() {
    val uuid = "85" // this is our device's uuid
    val key = uuid + "-current.txt"
    // installed apps. Does not include KTDict from the powermon data, or Yle Areena.
    val allApps = Array("Angry Birds", "Skype", "Carat", "Bejeweled Blitz", "Safari", "Mail")

    loadPrint(key)

    for (app <- allApps) {
      val key = app + "-current.txt"
      loadPrintApp(key)
    }
  }

  def loadPrintApp(key: String) {
    S3Decoder.get(key)
    for (i <- 0 until 4) {
      /* p1, p2, pc1, pc2, distance.*/
      val (name, dist) = getDistribution()
      println(name)
      for (k <- dist)
        println(k._1 + ", " + k._2 + ", " + k._3)
    }
    val distance:Double = S3Decoder.read().asInstanceOf[Double]
    val distName = S3Decoder.read().asInstanceOf[String]
    
    println(distName + ": " + distance)
  }

  def loadPrint(key: String) {
    S3Decoder.get(key)
    for (i <- 0 until 4) {
      /* p1, p2, pc1, pc2, distance, and then bugs for each app...*/
      val (name, dist) = getDistribution()
      println(name)
      for (k <- dist)
        println(k._1 + ", " + k._2 + ", " + k._3)
    }
    val distance:Double = S3Decoder.read().asInstanceOf[Double]
    val name = S3Decoder.read().asInstanceOf[String]
    
    println(name + ": " + distance)

    while (S3Decoder.open) {
      /* p1, p2, pc1, pc2, distance, and then bugs for each app...*/
      for (i <- 0 until 4) {
        /* p1, p2, pc1, pc2, distance, and then bugs for each app...*/
        val (name, dist) = getDistribution()
        println(name)
        for (k <- dist)
          println(k._1 + ", " + k._2 + ", " + k._3)
      }
      val distance: Double = S3Decoder.read().asInstanceOf[Double]
      val distName = S3Decoder.read().asInstanceOf[String]
      println(distName + ": " + distance)
    }

    println("finished")
  }

  def getDistribution() = {
    var finished = false
    var uuid = ""
    var arr = new ArrayBuffer[(String, Double, Double)]
    while (!finished) {
      val obj = S3Decoder.read()
      if (!S3Decoder.open || obj == null)
        finished = true
      else {
        uuid = obj.toString()
        if (uuid.startsWith("distance")
          || uuid.startsWith("-uuid=")
          || uuid.startsWith("-not-uuid=")
          || uuid.startsWith("cumulative")
          || uuid.startsWith("-bug")
          || uuid.startsWith("-app=")
          || uuid.startsWith("-not-app="))
          finished = true
      }
      if (!finished) {
        val rate = S3Decoder.read().asInstanceOf[Double]
        val prob = S3Decoder.read().asInstanceOf[Double]
        arr += ((uuid, rate, prob))
      }
    }
    (uuid, arr)
  }

  def writeDistance(distance: Double, app: String) {
    S3Encoder.write(distance)
    S3Encoder.write(app)
  }
}
