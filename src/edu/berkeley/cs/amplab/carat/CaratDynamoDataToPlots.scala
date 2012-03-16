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
import scala.collection.mutable.Map
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
import com.esotericsoftware.kryo.Kryo
import edu.berkeley.cs.amplab.carat.plot.PlotUtil

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

object CaratDynamoDataToPlots {

  // How many concurrent plotting operations are allowed to run at once.
  val CONCURRENT_PLOTS = 100
  // How many clients do we need to consider data reliable?
  val ENOUGH_USERS = 5
  
  val tmpdir = "/mnt/TimeSeriesSpark-unstable/spark-temp-plots/"

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

  def plotSampleTimes() {
    // turn off INFO logging for spark:
    System.setProperty("hadoop.root.logger", "WARN,console")
    // This is misspelled in the spark jar log4j.properties:
    System.setProperty("log4j.threshhold", "WARN")
    // Include correct spelling to make sure
    System.setProperty("log4j.threshold", "WARN")
    // turn on ProbUtil debug logging
    System.setProperty("log4j.category.spark.timeseries.ProbUtil.threshold", "DEBUG")

    // Fix Spark running out of space on AWS.
    System.setProperty("spark.local.dir", tmpdir)
    val plotDirectory = "/mnt/www/plots"
    val tm = {
      val allSamples = new scala.collection.mutable.HashMap[String, TreeSet[Double]]
      DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.getAllItems(samplesTable),
        DynamoDbDecoder.getAllItems(samplesTable, _),
        addToSet(_, _, allSamples))
      var tm = new TreeMap[String, TreeSet[Double]]()
      tm ++= allSamples
      tm
    }
    PlotUtil.plotSamples("Samples in time", tm)
  }

  def sampleCountsPerOsAndModel() {
    // turn off INFO logging for spark:
    System.setProperty("hadoop.root.logger", "WARN,console")
    // This is misspelled in the spark jar log4j.properties:
    System.setProperty("log4j.threshhold", "WARN")
    // Include correct spelling to make sure
    System.setProperty("log4j.threshold", "WARN")
    // turn on ProbUtil debug logging
    System.setProperty("log4j.category.spark.timeseries.ProbUtil.threshold", "DEBUG")

    // Fix Spark running out of space on AWS.
    System.setProperty("spark.local.dir", tmpdir)
    val allSamples = new scala.collection.mutable.HashMap[String, Long]
    DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.getAllItems(samplesTable),
      DynamoDbDecoder.getAllItems(samplesTable, _),
      addToStats(_, _, allSamples))

    val uuidToOsAndModel = new scala.collection.mutable.HashMap[String, (String, String)]
    val allModels = new scala.collection.mutable.HashSet[String]
    val allOses = new scala.collection.mutable.HashSet[String]

    val modelSampleCounts = new scala.collection.mutable.HashMap[String, Long]
    val osSampleCounts = new scala.collection.mutable.HashMap[String, Long]

    DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.getAllItems(registrationTable),
      DynamoDbDecoder.getAllItems(registrationTable, _),
      DynamoAnalysisUtil.handleRegs(_, _, uuidToOsAndModel, allOses, allModels))

    for (k <- allSamples) {
      val (os, model) = uuidToOsAndModel.get(k._1).getOrElse("", "")
      val c = osSampleCounts.getOrElse(os, 0L) + k._2
      osSampleCounts += ((os, c))
      val m = modelSampleCounts.getOrElse(model, 0L) + k._2
      modelSampleCounts += ((model, m))
    }
    for (k <- osSampleCounts)
      println(k._1 + " " + k._2)
    for (k <- modelSampleCounts)
      println(k._1 + " " + k._2)
  }

  def addToStats(key: Key, samples: java.util.List[java.util.Map[String, AttributeValue]],
    allSamples: scala.collection.mutable.HashMap[String, Long]) {
    val mapped = samples.map(x => {
      /* See properties in package.scala for data keys. */
      val uuid = x.get(sampleKey).getS()
      (uuid, 1)
    })

    for (k <- mapped) {
      var oldVal = allSamples.get(k._1).getOrElse(0L)
      oldVal += k._2
      allSamples.put(k._1, oldVal)
    }
  }

  def addToSet(key: Key, samples: java.util.List[java.util.Map[String, AttributeValue]],
    allSamples: scala.collection.mutable.HashMap[String, TreeSet[Double]]) {
    val mapped = samples.map(x => {
      /* See properties in package.scala for data keys. */
      val uuid = x.get(sampleKey).getS()
      val time = { val attr = x.get(sampleTime); if (attr != null) attr.getN().toDouble else 0.0 }
      (uuid, time)
    })
    for (k <- mapped) {
      var oldVal = allSamples.get(k._1).getOrElse(new TreeSet[Double])
      oldVal += k._2
      allSamples.put(k._1, oldVal)
    }
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
    System.setProperty("spark.local.dir", tmpdir)

    //System.setProperty("spark.kryo.registrator", classOf[CaratRateRegistrator].getName)
    val sc = TimeSeriesSpark.init(master, "default", "CaratDynamoDataToPlots")
    // getRates
    val allRates = DynamoAnalysisUtil.getRates(sc, tmpdir)
    if (allRates != null) {
      // analyze data
      analyzeRateData(sc, allRates, plotDirectory)
      // save rates for next time
    }
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
   * Sample or any other record size calculator function. Takes multiple records as input and produces a
   * Map of (key, size, compressedSize) pairs where the sizes are in Bytes. the first size is the pessimistic
   * String representation bytes of the objects, while the second one is the size of the string representation
   * when gzipped.
   */
  def getSizeMap(key: String, samples: java.util.List[java.util.Map[java.lang.String, AttributeValue]]) = {
    var dist = new scala.collection.immutable.TreeMap[String, (Int, Int)]
    for (k <- samples) {
      var keyValue = {
        val av = k.get(key)
        if (av != null) {
          if (av.getN() != null)
            av.getN()
          else
            av.getS()
        } else
          ""
      }
      dist += ((keyValue, getSizes(DynamoDbDecoder.getVals(k))))
    }
    dist
  }

  /**
   * Calculates the size of a DynamoDb Map.
   * This is a pessimistic estimate where the size of each element is its String representation's size in Bytes.
   * Key lengths are ignored, since in a custom communication protocol object order can be used to determine keys,
   * or very short key identifiers can be used.
   */
  def getSizes(sample: java.util.Map[String, Any]) = {
    var b = 0
    var gz = 0
    val bos = new ByteArrayOutputStream()
    val g = new java.util.zip.GZIPOutputStream(bos)
    val values = sample.values()
    for (k <- values) {
      b += k.toString().getBytes().length
      g.write(k.toString().getBytes())
    }
    g.flush()
    g.finish()
    (b, bos.toByteArray().length)
  }

  /**
   * TODO: This function should calculate the stddev of all the distributions that it calculates, and return those in some sort of data structure.
   * The stddevs would then be added to by a future iteration of this function, etc., until we have a time series of stddevs for all the distributions
   * that are calculated from the data. Those would then be plotted as their own distributions.
   */
  def analyzeRateDataStdDevsOverTime() {}

  /**
   * Main analysis function. Called on the entire collected set of CaratRates.
   */
  def analyzeRateData(sc: SparkContext, inputRates: RDD[CaratRate], plotDirectory: String) = {
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

    val sem = new Semaphore(CONCURRENT_PLOTS)
    /**
     * uuid distributions, xmax, ev and evNeg
     * FIXME: With many users, this is a lot of data to keep in memory.
     * Consider changing the algorithm and using RDDs.
     */
    var distsWithUuid = new TreeMap[String, Array[(Double, Double)]]
    var distsWithoutUuid = new TreeMap[String, Array[(Double, Double)]]
    /* xmax, ev, evNeg */
    var parametersByUuid = new TreeMap[String, (Double, Double, Double)]
    /* evDistances*/
    var evDistanceByUuid = new TreeMap[String, Double]
    /* total samples and apps */
    var totalsByUuid = new TreeMap[String, (Double, Double)]

    var appsByUuid = new TreeMap[String, Set[String]]

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

    for (os <- oses) {
      // can be done in parallel, independent of anything else
      scheduler.execute({
        val fromOs = allRates.filter(_.os == os)
        val notFromOs = allRates.filter(_.os != os)
        // no distance check, not bug or hog
        val ret = plotDists(sem, sc, "iOS " + os, "Other versions", fromOs, notFromOs, aPrioriDistribution, false, plotDirectory, null, null, null, null, 0, 0, null)
      })
    }

    for (model <- models) {
      // can be done in parallel, independent of anything else
      scheduler.execute({
        val fromModel = allRates.filter(_.model == model)
        val notFromModel = allRates.filter(_.model != model)
        // no distance check, not bug or hog
        val ret = plotDists(sem, sc, model, "Other models", fromModel, notFromModel, aPrioriDistribution, false, plotDirectory, null, null, null, null, 0, 0, null)
      })
    }

    val uuidArray = uuidToOsAndModel.keySet.toArray.sortWith((s, t) => {
      s < t
    })

    //scheduler.execute({
    var allHogs = new HashSet[String]
    var allBugs = new HashSet[String]

    /* Hogs: Consider all apps except daemons. */
    for (app <- allApps) {
      val filtered = allRates.filter(_.allApps.contains(app)).cache()
      val filteredNeg = allRates.filter(!_.allApps.contains(app)).cache()

      // skip if counts are too low:
      val fCountStart = DynamoAnalysisUtil.start
      val usersWith = filtered.map(_.uuid).collect().toSet.size

      if (usersWith >= ENOUGH_USERS) {
        val usersWithout = filteredNeg.map(_.uuid).collect().toSet.size
        DynamoAnalysisUtil.finish(fCountStart, "clientCount")
        if (usersWithout >= ENOUGH_USERS) {
          if (plotDists(sem, sc, "Hog " + app + " running", app + " not running", filtered, filteredNeg, aPrioriDistribution, true, plotDirectory, filtered, oses, models, null, usersWith, usersWithout, null)) {
            // this is a hog

            allHogs += app
          } else {
            // not a hog. is it a bug for anyone?
            for (i <- 0 until uuidArray.length) {
              val uuid = uuidArray(i)
              /* Bugs: Only consider apps reported from this uuId. Only consider apps not known to be hogs. */
              val appFromUuid = filtered.filter(_.uuid == uuid) //.cache()
              val appNotFromUuid = filtered.filter(_.uuid != uuid) //.cache()
              if (plotDists(sem, sc, "Bug " + app + " running on client " + i, app + " running on other clients", appFromUuid, appNotFromUuid, aPrioriDistribution, true, plotDirectory,
                filtered, oses, models, null, 0, 0, uuid)) {
                allBugs += app
              }
            }
          }
        } else {
          println("Skipped app " + app + " for too few points in: without: %s < thresh=%s".format(usersWithout, ENOUGH_USERS))
        }
      } else {
        println("Skipped app " + app + " for too few points in: with: %s < thresh=%s".format(usersWith, ENOUGH_USERS))
      }
    }

    val globalNonHogs = allApps -- allHogs
    println("Non-daemon non-hogs: " + globalNonHogs)
    println("All hogs: " + allHogs)
    println("All bugs: " + allBugs)
    //})

    /* uuid stuff */
    val uuidSem = new Semaphore(CONCURRENT_PLOTS)
    val bottleNeck = new Semaphore(1)

    for (i <- 0 until uuidArray.length) {
      // these are independent until JScores.
      scheduler.execute({
        uuidSem.acquireUninterruptibly()
        val uuid = uuidArray(i)
        val fromUuid = allRates.filter(_.uuid == uuid) //.cache()

        var uuidApps = fromUuid.flatMap(_.allApps).collect().toSet
        uuidApps --= DAEMONS_LIST_GLOBBED

        if (uuidApps.size > 0)
          similarApps(sem, sc, allRates, aPrioriDistribution, i, uuidApps, uuid, plotDirectory)
        /* cache these because they will be used numberOfApps times */
        val notFromUuid = allRates.filter(_.uuid != uuid) //.cache()
        // no distance check, not bug or hog
        val (xmax, probDist, probDistNeg, ev, evNeg, evDistance) = DynamoAnalysisUtil.getDistanceAndDistributionsUnBucketed(sc, fromUuid, notFromUuid, aPrioriDistribution)
        bottleNeck.acquireUninterruptibly()
        if (probDist != null && probDistNeg != null) {
          distsWithUuid += ((uuid, probDist.collect()))
          distsWithoutUuid += ((uuid, probDistNeg.collect()))
          parametersByUuid += ((uuid, (xmax, ev, evNeg)))
          evDistanceByUuid += ((uuid, evDistance))
        }
        val totalSamples = fromUuid.count() * 1.0
        totalsByUuid += ((uuid, (totalSamples, uuidApps.size)))
        appsByUuid += ((uuid, uuidApps))
        bottleNeck.release()
        uuidSem.release()
      })
    }

    // need to collect uuid stuff here:
    uuidSem.acquireUninterruptibly(CONCURRENT_PLOTS)
    uuidSem.release(CONCURRENT_PLOTS)

    /** Calculate correlation for each model and os version with all rates */
    val (osCorrelations, modelCorrelations, userCorrelations) = DynamoAnalysisUtil.correlation("All", allRates, aPrioriDistribution, models, oses, totalsByUuid)
    PlotUtil.plotJScores(plotDirectory, allRates, aPrioriDistribution, distsWithUuid, distsWithoutUuid, parametersByUuid, evDistanceByUuid, appsByUuid, uuidToOsAndModel, DECIMALS)

    PlotUtil.writeCorrelationFile(plotDirectory, "All", osCorrelations, modelCorrelations, userCorrelations, 0, 0, null)
    // not allowed to return before everything is done
    sem.acquireUninterruptibly(CONCURRENT_PLOTS)
    sem.release(CONCURRENT_PLOTS)
    // return plot directory for caller
    dateString + "/" + PLOTS
  }

  /**
   * Calculate similar apps for device `uuid` based on all rate measurements and apps reported on the device.
   * Write them to DynamoDb.
   */
  def similarApps(sem: Semaphore, sc: SparkContext, all: RDD[CaratRate], aPrioriDistribution: Map[Double, Double], i: Int, uuidApps: Set[String], uuid: String, plotDirectory: String) {
    val sCount = similarityCount(uuidApps.size)
    printf("SimilarApps client=%s sCount=%s uuidApps.size=%s\n", i, sCount, uuidApps.size)
    val similar = all.filter(_.allApps.intersect(uuidApps).size >= sCount)
    val dissimilar = all.filter(_.allApps.intersect(uuidApps).size < sCount)
    //printf("SimilarApps similar.count=%s dissimilar.count=%s\n",similar.count(), dissimilar.count())
    // no distance check, not bug or hog
    plotDists(sem, sc, "Similar to client " + i, "Not similar to client " + i, similar, dissimilar, aPrioriDistribution, false, plotDirectory, null, null, null, null, 0, 0, uuid)
  }

  /* Generate a gnuplot-readable plot file of the bucketed distribution.
   * Create folders plots/data plots/plotfiles
   * Save it as "plots/data/titleWith-titleWithout".txt.
   * Also generate a plotfile called plots/plotfiles/titleWith-titleWithout.gnuplot
   */

  def plotDists(sem: Semaphore, sc: SparkContext, title: String, titleNeg: String,
    one: RDD[CaratRate], two: RDD[CaratRate], aPrioriDistribution: Map[Double, Double], isBugOrHog: Boolean, plotDirectory: String,
    filtered: RDD[CaratRate], oses: Set[String], models: Set[String], totalsByUuid: TreeMap[String, (Double, Double)], usersWith: Int, usersWithout: Int, uuid: String) = {
    var hasSamples = true
    if (usersWith == 0 && usersWithout == 0) {
      hasSamples = one.take(1) match {
        case Array(t) => true
        case _ => false
      }
      hasSamples = two.take(1) match {
        case Array(t) => hasSamples && true
        case _ => false
      }
    }
    if (hasSamples) {
      val (xmax, probDist, probDistNeg, ev, evNeg, evDistance /*, usersWith, usersWithout*/ ) = DynamoAnalysisUtil.getDistanceAndDistributionsUnBucketed(sc, one, two, aPrioriDistribution)
      if (probDist != null && probDistNeg != null && (!isBugOrHog || evDistance > 0)) {
        if (evDistance > 0) {
          var imprHr = (100.0 / evNeg - 100.0 / ev) / 3600.0
          val imprD = (imprHr / 24.0).toInt
          imprHr -= imprD * 24.0
          printf("%s evWith=%s evWithout=%s evDistance=%s improvement=%s days %s hours (%s vs %s users)\n", title, ev, evNeg, evDistance, imprD, imprHr, usersWith, usersWithout)
        } else {
          printf("%s evWith=%s evWithout=%s evDistance=%s (%s vs %s users)\n", title, ev, evNeg, evDistance, usersWith, usersWithout)
        }
        scheduler.execute(
          if (isBugOrHog && filtered != null) {
            val (osCorrelations, modelCorrelations, userCorrelations) = DynamoAnalysisUtil.correlation(title, filtered, aPrioriDistribution, models, oses, totalsByUuid)
            PlotUtil.plot(plotDirectory, title, titleNeg, xmax, probDist.collect(), probDistNeg.collect(), ev, evNeg, evDistance, osCorrelations, modelCorrelations, userCorrelations, usersWith, usersWithout, uuid, DECIMALS)
          } else
            PlotUtil.plot(plotDirectory, title, titleNeg, xmax, probDist.collect(), probDistNeg.collect(), ev, evNeg, evDistance, null, null, null, usersWith, usersWithout, uuid, DECIMALS))
      }
      isBugOrHog && evDistance > 0
    } else
      false
  }
}
