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

object ModelAndOsNumbers {

  // How many concurrent plotting operations are allowed to run at once.
  val CONCURRENT_PLOTS = 100
  // How many Rates must there be in a dist for it to be plotted?
  val DIST_THRESHOLD = 10

  lazy val scheduler = {
    scala.util.Properties.setProp("actors.corePoolSize", CONCURRENT_PLOTS + "")
    val s = new ResizableThreadPoolScheduler(false)
    s.start()
    s
  }

  // Bucketing and decimal constants
  val buckets = 100
  val smallestBucket = 0.0001
  val DECIMALS = 3
  var DEBUG = false
  val LIMIT_SPEED = false
  val ABNORMAL_RATE = 9

  val tmpdir = "/mnt/TimeSeriesSpark-unstable/spark-temp-plots/"

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
    var master = "local[1]"
    if (args != null && args.length >= 1) {
      master = args(0)
    }
    plotEverything(master, args != null && args.length > 1 && args(1) == "DEBUG", null)
    //sys.exit(0)
  }

  def plotEverything(master: String, debug: Boolean, plotDirectory: String) = {
    val start = DynamoAnalysisUtil.start()
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
    val sc = TimeSeriesSpark.init(master, "default", "CaratDynamoDataToPlots")
    analyzeData(sc, plotDirectory)
    DynamoAnalysisUtil.finish(start)
  }
  /*
  class CaratRateRegistrator extends KryoRegistrator{
    def registerClasses(kryo: Kryo){
      kryo.register(classOf[Array[edu.berkeley.cs.amplab.carat.CaratRate]])
      kryo.register(classOf[edu.berkeley.cs.amplab.carat.CaratRate])
    }
  }*/

  /**
   * Main function. Called from main() after sc initialization.
   */

  def analyzeData(sc: SparkContext, plotDirectory: String) = {
    // Master RDD for all data.
    var allRates: spark.RDD[CaratRate] = null

    // closure to forget uuids, models and oses after assigning them to rates
    {
      // Unique uuIds, Oses, and Models from registrations.
      val uuidToOsAndModel = new scala.collection.mutable.HashMap[String, (String, String)]
      val allModels = new scala.collection.mutable.HashSet[String]
      val allOses = new scala.collection.mutable.HashSet[String]

        DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.getAllItems(registrationTable),
          DynamoDbDecoder.getAllItems(registrationTable, _),
          CaratDynamoDataToPlots.handleRegs(_, _, uuidToOsAndModel, allOses, allModels))

      /* Limit attributesToGet here so that bandwidth is not used for nothing. Right now the memory attributes of samples are not considered. */
        allRates = DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.getAllItems(samplesTable),
          DynamoDbDecoder.getAllItems(samplesTable, _),
          CaratDynamoDataToPlots.handleSamples(sc, _, uuidToOsAndModel, _),
          true,
          allRates)

      // we may not be interesed in these actually.
      println("All uuIds: " + uuidToOsAndModel.keySet.mkString(", "))
      println("All oses: " + allOses.mkString(", "))
      println("All models: " + allModels.mkString(", "))
    }

    if (allRates != null) {
      analyzeRateData(sc, allRates, plotDirectory)
    } else
      null
  }

  /**
   * Main analysis function. Called on the entire collected set of CaratRates.
   */
  def analyzeRateData(sc: SparkContext, inputRates: RDD[CaratRate], plotDirectory: String) = {
    // cache first
    val allRates = inputRates.cache()

    // determine oses and models that appear in accepted data and use those
    val uuidToOsAndModel = new scala.collection.mutable.HashMap[String, (String, String)]
    uuidToOsAndModel ++= allRates.map(x => { (x.uuid, (x.os, x.model)) }).collect()

    var evByUuid = new TreeMap[String, Double]

    val sem = new Semaphore(CONCURRENT_PLOTS)

    println("Calculating aPriori.")
    val aPrioriDistribution = DynamoAnalysisUtil.getApriori(allRates)
    println("Calculated aPriori.")
    if (aPrioriDistribution.size == 0)
      println("WARN: a priori dist is empty!")
    else
      println("a priori dist:\n" + aPrioriDistribution.mkString("\n"))

    var allApps = allRates.flatMap(_.allApps).collect().toSet
    println("AllApps (with daemons): " + allApps)
    val DAEMONS_LIST_GLOBBED = DynamoAnalysisUtil.daemons_globbed(allApps)
    allApps --= DAEMONS_LIST_GLOBBED
    println("AllApps (no daemons): " + allApps)

    val uuidArray = uuidToOsAndModel.keySet.toArray.sortWith((s, t) => {
      s < t
    })

    /* uuid stuff */
    val uuidSem = new Semaphore(CONCURRENT_PLOTS)

    for (i <- 0 until uuidArray.length) {
      // these are independent until JScores.
      val uuid = uuidArray(i)
      val fromUuid = allRates.filter(_.uuid == uuid) //.cache()

      var uuidApps = fromUuid.flatMap(_.allApps).collect().toSet
      uuidApps --= DAEMONS_LIST_GLOBBED

      val notFromUuid = allRates.filter(_.uuid != uuid) //.cache()
      // no distance check, not bug or hog
      val (dist, ev) = DynamoAnalysisUtil.getEvAndDistribution(fromUuid, aPrioriDistribution)
      evByUuid += ((uuid, ev))
    }

    // need to collect uuid stuff here:
    uuidSem.acquireUninterruptibly(CONCURRENT_PLOTS)
    uuidSem.release(CONCURRENT_PLOTS)
    plotJScores(sc, sem, allRates, aPrioriDistribution, evByUuid, uuidToOsAndModel)

    // not allowed to return before everything is done
    sem.acquireUninterruptibly(CONCURRENT_PLOTS)
    sem.release(CONCURRENT_PLOTS)
    // return plot directory for caller
    dateString + "/" + PLOTS
  }

  /**
   * The J-Score is the % of people with worse = higher energy use.
   * therefore, it is the size of the set of evDistances that are higher than mine,
   * compared to the size of the user base.
   * Note that the server side multiplies the JScore by 100, and we store it here
   * as a fraction.
   */
  def plotJScores(sc: SparkContext, sem: Semaphore, allRates: RDD[CaratRate], aPrioriDistribution: Array[(Double, Double)],
    evByUuid: TreeMap[String, Double],
    uuidToOsAndModel: scala.collection.mutable.HashMap[String, (String, String)]) {

    val oses = uuidToOsAndModel.map(_._2._1).toSet
    val models = uuidToOsAndModel.map(_._2._2).toSet

    for (os <- oses) {
      // can be done in parallel, independent of anything else
      val fromOs = allRates.filter(_.os == os)
      val notFromOs = allRates.filter(_.os != os)
      // no distance check, not bug or hog
      plotDistsStdDevAndSampleCount(sem, sc, os, fromOs, notFromOs, aPrioriDistribution, false, evByUuid, uuidToOsAndModel)
    }

    for (model <- models) {
      // can be done in parallel, independent of anything else
      val fromModel = allRates.filter(_.model == model)
      val notFromModel = allRates.filter(_.model != model)
      // no distance check, not bug or hog
      plotDistsStdDevAndSampleCount(sem, sc, model, fromModel, notFromModel, aPrioriDistribution, false, evByUuid, uuidToOsAndModel)
    }
  }

  def plotDistsStdDevAndSampleCount(sem: Semaphore, sc: SparkContext, title: String,
    one: RDD[CaratRate], two: RDD[CaratRate], aPrioriDistribution: Array[(Double, Double)], isBugOrHog: Boolean,
    allEvs: scala.collection.immutable.TreeMap[String,Double], uuidToOsAndModel: scala.collection.mutable.HashMap[String, (String, String)], enoughWith: Boolean = false, enoughWithout: Boolean = false) = {
    val usersWith = one.map(_.uuid).collect().toSet.size
    // the ev is over all the points in the distribution
    val (probOne, ev) = DynamoAnalysisUtil.getEvAndDistribution(one, aPrioriDistribution)
    // convert to prob dist
    val evOne = probOne.map(x => { (x._1 * x._2) })
    val mean = ProbUtil.mean(evOne)
    val variance = ProbUtil.variance(evOne, mean)
    val sampleCount = one.count()
    
    val userEvs = allEvs.filter(x => {
      val p = uuidToOsAndModel.get(x._1).getOrElse("", "")
      p._1 == title || p._2 == title
    }).map(_._2).toSeq
    val meanU = ProbUtil.mean(userEvs)
    val varianceU = ProbUtil.variance(userEvs, meanU)
    
    var imprMin = (100.0 / (ev) - 100.0 / (ev + variance)) / 60.0
    var imprHr = (imprMin / 60.0).toInt
    imprMin -= imprHr * 60.0
    var imprD = (imprHr / 24.0).toInt
    imprHr -= imprD * 24
    
    println("%s ev=%s mean=%s variance=%s (%s d %s h %s min), clients=%s samples=%s".format(title, ev, mean, variance, imprD, imprHr, imprMin, usersWith, sampleCount))
        
    var imprMinU = (100.0 / (ev) - 100.0 / (ev + varianceU)) / 60.0
    var imprHrU = (imprMin / 60.0).toInt
    imprMin -= imprHr * 60.0
    var imprDU = (imprHr / 24.0).toInt
    imprHr -= imprD * 24
    
    println("%s ev=%s meanU=%s varianceU=%s (%s d %s h %s min), clients=%s samples=%s".format(title, ev, meanU, varianceU, imprDU, imprHrU, imprMinU, usersWith, sampleCount))
  }
}
