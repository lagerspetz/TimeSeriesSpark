package edu.berkeley.cs.amplab.carat

import spark._
import spark.SparkContext._
import spark.timeseries._
import java.util.concurrent.Semaphore
import scala.collection.mutable.ArrayBuffer
import scala.collection.Seq
import scala.collection.immutable.Set
import scala.collection.immutable.HashSet
import scala.collection.immutable.TreeMap
import collection.JavaConversions._
import com.amazonaws.services.dynamodb.model.AttributeValue
import java.io.File
import java.text.SimpleDateFormat
import java.io.ByteArrayOutputStream
import com.amazonaws.services.dynamodb.model.Key
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileInputStream
import java.io.FileWriter
import java.io.FileOutputStream
import edu.berkeley.cs.amplab.carat.dynamodb.DynamoAnalysisUtil
import scala.collection.immutable.TreeSet
import edu.berkeley.cs.amplab.carat.dynamodb.DynamoDbDecoder
import scala.actors.scheduler.ResizableThreadPoolScheduler
import scala.collection.mutable.HashMap
import com.esotericsoftware.kryo.Kryo

/**
 * Analyzes data in the Carat Amazon DynamoDb to obtain probability distributions
 * of battery rates for each of the following case pairs:
 * 1) App X is running & App X is not running
 * 2) App X is running on uuId U & App X is running on uuId != U
 * 3) OS Version == V & OS Version != V
 * 4) Device Model == M & Device Model != M
 * 5) uuId == U & uuId != U
 * 6) Similar apps to uuId U are running vs dissimilar apps are running.
 *    This is calculated by taking the set A of all apps ever reported running on uuId U
 *    and taking the data from samples where (A intersection sample.getAllApps()).size >= ln(A)
 *    and comparing it with (A intersection sample.getAllApps()).size < ln(A).
 *
 * Where uuId is a unique device identifier.
 *
 * NOTE: We do not store hogs or bugs with negative distance values.
 *
 * @author Eemil Lagerspetz
 */

object AprioriSliceOfUuidTimePeriod {

  // Bucketing and decimal constants
  val buckets = 100
  val smallestBucket = 0.0001
  val DECIMALS = 3
  var DEBUG = false
  val LIMIT_SPEED = false
  val ABNORMAL_RATE = 9

  val tmpdir = "/mnt/TimeSeriesSpark-unstable/spark-temp-plots/"
  val RATES_CACHED_NEW = tmpdir + "cached-rates-new.dat"
  val RATES_CACHED = tmpdir + "cached-rates.dat"
  val LAST_SAMPLE = tmpdir + "last-sample.txt"
  val LAST_REG = tmpdir + "last-reg.txt"

  val last_sample = DynamoAnalysisUtil.readDoubleFromFile(LAST_SAMPLE)

  var last_sample_write = 0.0

  val last_reg = DynamoAnalysisUtil.readDoubleFromFile(LAST_REG)

  var last_reg_write = 0.0

  val dfs = "yyyy-MM-dd"
  val df = new SimpleDateFormat(dfs)
  val dateString = "plots-" + df.format(System.currentTimeMillis())

  val DATA_DIR = "data"
  val PLOTS = "plots"
  val PLOTFILES = "plotfiles"

  val Bug = "Bug"
  val Hog = "Hog"
  val Sim = "Sim"
  val Pro = "Pro"

  val BUGS = "bugs"
  val HOGS = "hogs"
  val SIM = "similarApps"
  val UUIDS = "uuIds"

  /**
   * Main program entry point.
   */
  def main(args: Array[String]) {
    val start = DynamoAnalysisUtil.start()
    var master = "local[1]"
    if (args != null && args.length >= 1) {
      master = args(0)
      val debug = args.length > 1 && args(1) == "DEBUG"

      var givenUuid = ""
      if (args.length > 2)
        givenUuid = args(2)
      var time1 = 0L
      var time2 = 0L

      if (args.length > 3)
        time1 = args(3).toLong
      if (args.length > 4)
        time1 = args(4).toLong

      if (debug) {
        DEBUG = true
      } else {
        // turn off INFO logging for spark:
        System.setProperty("hadoop.root.logger", "WARN,console")
        // This is misspelled in the spark jar log4j.properties:
        System.setProperty("log4j.threshhold", "WARN")
        // Include correct spelling to make sure
        System.setProperty("log4j.threshold", "WARN")
      }
      // turn on ProbUtil debug logging
      System.setProperty("log4j.category.spark.timeseries.ProbUtil.threshold", "DEBUG")
      System.setProperty("log4j.appender.spark.timeseries.ProbUtil.threshold", "DEBUG")

      // Fix Spark running out of space on AWS.
      System.setProperty("spark.local.dir", "/mnt/TimeSeriesSpark-unstable/spark-temp-plots")

      //System.setProperty("spark.kryo.registrator", classOf[CaratRateRegistrator].getName)

      if (givenUuid != "" && (time1 != 0 || time2 != 0)) {
        val sc = TimeSeriesSpark.init(master, "default", "CaratDynamoDataToPlots")
        analyzeData(sc, givenUuid, time1, time2)
      }else{
        println("Usage: AprioriSliceOfUuidTimePeriod master debug uuid time1 time2\n"+
            "Example: AprioriSliceOfUuidTimePeriod local[8] false 11-22-33-44-55 139547 1294574")
      }
    }
    DynamoAnalysisUtil.finish(start)
  }

  /**
   * Main function. Called from main() after sc initialization.
   */

  def analyzeData(sc: SparkContext, givenUuid: String, time1: Long, time2: Long) = {
    // Master RDD for all data.

    val oldRates: spark.RDD[CaratRate] = {
      val f = new File(RATES_CACHED)
      if (f.exists()) {
        sc.objectFile(RATES_CACHED)
      } else
        null
    }

    var allRates: spark.RDD[CaratRate] = oldRates

    // closure to forget uuids, models and oses after assigning them to rates
    {
      // Unique uuIds, Oses, and Models from registrations.
      val uuidToOsAndModel = new scala.collection.mutable.HashMap[String, (String, String)]
      val allModels = new scala.collection.mutable.HashSet[String]
      val allOses = new scala.collection.mutable.HashSet[String]

      if (oldRates != null) {
        val devices = oldRates.map(x => {
          (x.uuid, (x.os, x.model))
        }).collect()
        for (k <- devices) {
          uuidToOsAndModel += ((k._1, (k._2._1, k._2._2)))
          allOses += k._2._1
          allModels += k._2._2
        }
      }

      if (last_reg > 0) {
        DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.filterItemsAfter(registrationTable, regsTimestamp, last_reg + ""),
          DynamoDbDecoder.filterItemsAfter(registrationTable, regsTimestamp, last_reg + "", _),
          CaratDynamoDataToPlots.handleRegs(_, _, uuidToOsAndModel, allOses, allModels))
      } else {
        DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.getAllItems(registrationTable),
          DynamoDbDecoder.getAllItems(registrationTable, _),
          CaratDynamoDataToPlots.handleRegs(_, _, uuidToOsAndModel, allOses, allModels))
      }

      /* Limit attributesToGet here so that bandwidth is not used for nothing. Right now the memory attributes of samples are not considered. */
      if (last_sample > 0) {
        allRates = DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.filterItemsAfter(samplesTable, sampleTime, last_sample + ""),
          DynamoDbDecoder.filterItemsAfter(samplesTable, sampleTime, last_sample + "", _),
          CaratDynamoDataToPlots.handleSamples(sc, _, uuidToOsAndModel, _),
          true,
          allRates)
      } else {
        allRates = DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.getAllItems(samplesTable),
          DynamoDbDecoder.getAllItems(samplesTable, _),
          CaratDynamoDataToPlots.handleSamples(sc, _, uuidToOsAndModel, _),
          true,
          allRates)
      }

      // we may not be interesed in these actually.
      println("All uuIds: " + uuidToOsAndModel.keySet.mkString(", "))
      println("All oses: " + allOses.mkString(", "))
      println("All models: " + allModels.mkString(", "))
    }

    if (allRates != null) {
      allRates.saveAsObjectFile(RATES_CACHED_NEW)
      DynamoAnalysisUtil.saveDoubleToFile(last_sample_write, LAST_SAMPLE)
      DynamoAnalysisUtil.saveDoubleToFile(last_reg_write, LAST_REG)
      analyzeRateData(sc, allRates, givenUuid, time1, time2)
    } else
      null
  }
  
  /**
   * Main analysis function. Called on the entire collected set of CaratRates.
   */
  def analyzeRateData(sc: SparkContext, inputRates: RDD[CaratRate], givenUuid: String, time1: Long, time2: Long) = {
    // cache first
    val allRates = inputRates.cache()

    // determine oses and models that appear in accepted data and use those
    val uuidToOsAndModel = new scala.collection.mutable.HashMap[String, (String, String)]
    uuidToOsAndModel ++= allRates.map(x => { (x.uuid, (x.os, x.model)) }).collect()

    val oses = uuidToOsAndModel.map(_._2._1).toSet
    val models = uuidToOsAndModel.map(_._2._2).toSet

    println("uuIds with data: " + uuidToOsAndModel.keySet.mkString(", "))
    println("oses with data: " + oses.mkString(", "))
    println("models with data: " + models.mkString(", "))

    println("Calculating aPriori.")
    val aPrioriDistribution = DynamoAnalysisUtil.getApriori(allRates)
    println("Calculated aPriori.")
    if (aPrioriDistribution.size == 0)
      println("WARN: a priori dist is empty!")
    else
      println("a priori dist:\n" + aPrioriDistribution.mkString("\n"))

    val fromUuid = allRates.filter(x =>
      { x.uuid == givenUuid && x.time1 > time1 && x.time2 < time2 }) //.cache()
    // no distance check, not bug or hog
    val freqWith = DynamoAnalysisUtil.getFrequencies(aPrioriDistribution, fromUuid).collect()

    println("%s freqs for %s to %s".format(givenUuid, time1, time2))
    for (k <- freqWith)
      println(k._1 + " " + k._2)
    dateString + "/" + PLOTS
  }
}
