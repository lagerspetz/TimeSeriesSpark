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
  /*
  lazy val scheduler = {
    scala.util.Properties.setProp("actors.corePoolSize", CONCURRENT_PLOTS+"")
    val s = new ResizableThreadPoolScheduler(false)
    s.start()
    s
  }*/
  
  // Bucketing and decimal constants
  val buckets = 100
  val smallestBucket = 0.0001
  val DECIMALS = 3
  var DEBUG = false
  val LIMIT_SPEED = false
  val ABNORMAL_RATE = 9
  
  val tmpdir = "/mnt/TimeSeriesSpark-unstable/spark-temp-plots/"
  val RATES_CACHED_NEW = tmpdir+"cached-rates-new.dat"
  val RATES_CACHED = tmpdir+"cached-rates.dat"
  val LAST_SAMPLE = tmpdir+"last-sample.txt"
  val LAST_REG = tmpdir+"last-reg.txt"
  
  val last_sample = DynamoAnalysisUtil.readDoubleFromFile(LAST_SAMPLE)
  
  var last_sample_write = 0.0
  
  val last_reg = DynamoAnalysisUtil.readDoubleFromFile(LAST_REG)
  
  var last_reg_write = 0.0
  
  val dfs = "yyyy-MM-dd"
  val df = new SimpleDateFormat(dfs)
  val dateString = "plots-"+df.format(System.currentTimeMillis())

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
    sys.exit(0)
  }
  
  def plotSampleTimes(){
    // turn off INFO logging for spark:
      System.setProperty("hadoop.root.logger", "WARN,console")
      // This is misspelled in the spark jar log4j.properties:
      System.setProperty("log4j.threshhold", "WARN")
      // Include correct spelling to make sure
      System.setProperty("log4j.threshold", "WARN")
    // turn on ProbUtil debug logging
    System.setProperty("log4j.category.spark.timeseries.ProbUtil.threshold", "DEBUG")

    // Fix Spark running out of space on AWS.
    System.setProperty("spark.local.dir", "/mnt/TimeSeriesSpark-unstable/spark-temp-plots")
    val plotDirectory =  "/mnt/www/plots"
    val tm = {
    val allSamples = new scala.collection.mutable.HashMap[String, TreeSet[Double]]
      DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.getAllItems(samplesTable),
        DynamoDbDecoder.getAllItems(samplesTable, _),
        addToSet(_, _, allSamples))
        var tm = new TreeMap[String, TreeSet[Double]]()
      tm ++= allSamples
      tm
    }
    plotSamples("Samples in time", plotDirectory, tm)
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
    val sc = new SparkContext(master, "CaratDynamoDataToPlots")
    analyzeData(sc, plotDirectory)
    DynamoAnalysisUtil.replaceOldRateFile(RATES_CACHED, RATES_CACHED_NEW)
  }
  
  /**
   * Main function. Called from main() after sc initialization.
   */

  def analyzeData(sc: SparkContext, plotDirectory:String) = {
    // Unique uuIds, Oses, and Models from registrations.
    val uuidToOsAndModel = new scala.collection.mutable.HashMap[String, (String, String)]
    val allModels = new scala.collection.mutable.HashSet[String]
    val allOses = new scala.collection.mutable.HashSet[String]

    // Master RDD for all data.

    val oldRates: spark.RDD[CaratRate] = {
      val f = new File(RATES_CACHED)
      if (f.exists()) {
        sc.objectFile(RATES_CACHED)
      } else
        null
    }
    
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

    var allRates: spark.RDD[CaratRate] = oldRates

    if (last_reg > 0) {
      DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.filterItemsAfter(registrationTable, regsTimestamp, last_reg + ""),
        DynamoDbDecoder.filterItemsAfter(registrationTable, regsTimestamp, last_reg + "", _),
        handleRegs(_, _, uuidToOsAndModel, allOses, allModels))
    } else {
      DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.getAllItems(registrationTable),
        DynamoDbDecoder.getAllItems(registrationTable, _),
        handleRegs(_, _, uuidToOsAndModel, allOses, allModels))
    }

    /* Limit attributesToGet here so that bandwidth is not used for nothing. Right now the memory attributes of samples are not considered. */
    if (last_sample > 0) {
      allRates = DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.filterItemsAfter(samplesTable, sampleTime, last_sample + ""),
        DynamoDbDecoder.filterItemsAfter(samplesTable, sampleTime, last_sample + "", _),
        handleSamples(sc, _, uuidToOsAndModel, _),
        true,
        allRates)
    } else {
      allRates = DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.getAllItems(samplesTable),
        DynamoDbDecoder.getAllItems(samplesTable, _),
        handleSamples(sc, _, uuidToOsAndModel, _),
        true,
        allRates)
    }

    println("All uuIds: " + uuidToOsAndModel.keySet.mkString(", "))
    println("All oses: " + allOses.mkString(", "))
    println("All models: " + allModels.mkString(", "))

    if (allRates != null) {
      allRates.saveAsObjectFile(RATES_CACHED_NEW)
      DynamoAnalysisUtil.saveDoubleToFile(last_sample_write, LAST_SAMPLE)
      DynamoAnalysisUtil.saveDoubleToFile(last_reg_write, LAST_REG)
      // cache allRates here?
      analyzeRateData(sc, allRates.cache(), uuidToOsAndModel, allOses, allModels, plotDirectory)
    }else
      null
  }
  
  /**
   * Handles a set of registration messages from the Carat DynamoDb.
   * uuids, oses and models are filled in.
   */
  def handleRegs(key:Key, regs: java.util.List[java.util.Map[String, AttributeValue]],
      uuidToOsAndModel: scala.collection.mutable.HashMap[String, (String, String)],
      oses: scala.collection.mutable.Set[String],
      models: scala.collection.mutable.Set[String]) {
    
    // Get last reg timestamp for set saving
    if (regs.size > 0){
      last_reg_write = regs.last.get(regsTimestamp).getN().toDouble
    }

    for (x <- regs) {
      val uuid = { val attr = x.get(regsUuid); if (attr != null) attr.getS() else "" }
      val model = { val attr = x.get(regsModel); if (attr != null) attr.getS() else "" }
      val os = { val attr = x.get(regsOs); if (attr != null) attr.getS() else "" }
      uuidToOsAndModel += ((uuid, (os, model)))
      models += model
      oses += os
    }
        
    /*
     * TODO: Stddev of samples per user over time,
     * stddev of distributions (hog, etc) per all users over increasing number of users,
     * change of distance of distributions (hog, etc) over increasing number of users.
     */
    //analyzeRateDataStdDevsOverTime(sc, distRet, uuid, os, model, plotDirectory)
  }
  /**
   * Process a bunch of samples, assumed to be in order by uuid and timestamp.
   * will return an RDD of CaratRates. Samples need not be from the same uuid.
   */
  def handleSamples(sc: SparkContext, samples: java.util.List[java.util.Map[java.lang.String, AttributeValue]],
    uuidToOsAndModel: scala.collection.mutable.HashMap[String, (String, String)],
    rates: RDD[CaratRate]) = {

    if (samples.size > 0) {
      val lastSample = samples.last
      last_sample_write = lastSample.get(sampleTime).getN().toDouble
    }

    var rateRdd = sc.parallelize[CaratRate]({
      val mapped = samples.map(x => {
        /* See properties in package.scala for data keys. */
        val uuid = x.get(sampleKey).getS()
        val apps = x.get(sampleProcesses).getSS().map(w => {
          if (w == null)
            ""
          else {
            val s = w.split(";")
            if (s.size > 1)
              s(1).trim
            else
              ""
          }
        })

        val time = { val attr = x.get(sampleTime); if (attr != null) attr.getN() else "" }
        val batteryState = { val attr = x.get(sampleBatteryState); if (attr != null) attr.getS() else "" }
        val batteryLevel = { val attr = x.get(sampleBatteryLevel); if (attr != null) attr.getN() else "" }
        val event = { val attr = x.get(sampleEvent); if (attr != null) attr.getS() else "" }
        (uuid, time, batteryLevel, event, batteryState, apps)
      })
      DynamoAnalysisUtil.rateMapperPairwise(uuidToOsAndModel, mapped)
    })
    if (rates != null)
      rateRdd = rateRdd.union(rates)
    rateRdd
  }
  
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
        }else
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
    for (k <- values){
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
  def analyzeRateDataStdDevsOverTime(sc:SparkContext, allRates: RDD[CaratRate],
    uuids: scala.collection.mutable.Set[String], oses: scala.collection.mutable.Set[String], models: scala.collection.mutable.Set[String], plotDirectory:String) = {
    val sem = new Semaphore(CONCURRENT_PLOTS)
     /**
     * uuid distributions, xmax, ev and evNeg
     * FIXME: With many users, this is a lot of data to keep in memory.
     * Consider changing the algorithm and using RDDs. 
     */
    var distsWithUuid = new TreeMap[String, RDD[(Int, Double)]]
    var distsWithoutUuid = new TreeMap[String, RDD[(Int, Double)]]
    /* xmax, ev, evNeg */
    var parametersByUuid = new TreeMap[String, (Double, Double, Double)]
    /* evDistances*/
    var evDistanceByUuid = new TreeMap[String, Double]
    
    var appsByUuid = new TreeMap[String, Set[String]]
    
    val aPrioriDistribution = DynamoAnalysisUtil.getApriori(allRates)
    if (aPrioriDistribution.size == 0)
      println("WARN: a priori dist is empty!")
    else
      println("a priori dist:\n" + aPrioriDistribution.mkString("\n"))

    val apps = allRates.map(x => {
      var sampleApps = x.allApps
      sampleApps --= DynamoAnalysisUtil.DAEMONS_LIST
      sampleApps
    }).collect()

    var allApps = new HashSet[String]
    for (k <- apps)
      allApps ++= k
      
    // mediaremoted does not get removed here, why?
    println("AllApps (no daemons): " + allApps)

    for (os <- oses) {
      val fromOs = allRates.filter(_.os == os)
      val notFromOs = allRates.filter(_.os != os)
      // no distance check, not bug or hog
      plotDists(sem, sc, "iOS " + os, "Other versions", fromOs, notFromOs, aPrioriDistribution, false, plotDirectory)
    }

    for (model <- models) {
      val fromModel = allRates.filter(_.model == model)
      val notFromModel = allRates.filter(_.model != model)
      // no distance check, not bug or hog
      plotDists(sem, sc, model, "Other models", fromModel, notFromModel, aPrioriDistribution, false, plotDirectory)
    }

    var allHogs = new HashSet[String]
    /* Hogs: Consider all apps except daemons. */
    for (app <- allApps) {
      val filtered = allRates.filter(_.allApps.contains(app))
      val filteredNeg = allRates.filter(!_.allApps.contains(app))
      if (plotDists(sem, sc, "Hog " + app, "Other apps", filtered, filteredNeg, aPrioriDistribution, true, plotDirectory)) {
        // this is a hog
        allHogs += app
      }
    }
    
    val uuidArray = uuids.toArray.sortWith((s, t) => {
      s < t
    })
    
    for (i <- 0 until uuidArray.length) {
      val uuid = uuidArray(i)
      val fromUuid = allRates.filter(_.uuid == uuid)

      var uuidApps = fromUuid.flatMap(_.allApps).collect().toSet
      uuidApps --= DynamoAnalysisUtil.DAEMONS_LIST
      val nonHogApps = uuidApps -- allHogs

      if (uuidApps.size > 0)
        similarApps(sem, sc, allRates, aPrioriDistribution, i, uuidApps, plotDirectory)
      //else
      // Remove similar apps entry?

      val notFromUuid = allRates.filter(_.uuid != uuid)
      // no distance check, not bug or hog
      val (xmax, bucketed, bucketedNeg, ev, evNeg, evDistance) = DynamoAnalysisUtil.getDistanceAndDistributions(sc, fromUuid, notFromUuid, aPrioriDistribution, buckets, smallestBucket, DECIMALS, DEBUG)
      if (bucketed != null && bucketedNeg != null) {
        distsWithUuid += ((uuid, bucketed))
        distsWithoutUuid += ((uuid, bucketedNeg))
        parametersByUuid += ((uuid, (xmax, ev, evNeg)))
        evDistanceByUuid += ((uuid, evDistance))
      }
      appsByUuid += ((uuid, uuidApps))

      /* Bugs: Only consider apps reported from this uuId. Only consider apps not known to be hogs. */
      for (app <- nonHogApps) {
          val appFromUuid = fromUuid.filter(_.allApps.contains(app))
          val appNotFromUuid = notFromUuid.filter(_.allApps.contains(app))
          plotDists(sem, sc, "Bug "+app + " on " + uuid, app + " elsewhere", appFromUuid, appNotFromUuid, aPrioriDistribution, true, plotDirectory)
        }
    }
    
    plotJScores(sem, distsWithUuid, distsWithoutUuid, parametersByUuid, evDistanceByUuid, appsByUuid, plotDirectory)
    // return plot directory for caller
    dateString + "/" + PLOTS
  }
  
  /**
   * Main analysis function. Called on the entire collected set of CaratRates.
   */
  def analyzeRateData(sc:SparkContext, allRates: RDD[CaratRate],
    uuidToOsAndModel: scala.collection.mutable.HashMap[String, (String, String)], oses: scala.collection.mutable.Set[String], models: scala.collection.mutable.Set[String], plotDirectory:String) = {
    val sem = new Semaphore(CONCURRENT_PLOTS)
    /**
     * uuid distributions, xmax, ev and evNeg
     * FIXME: With many users, this is a lot of data to keep in memory.
     * Consider changing the algorithm and using RDDs. 
     */
    var distsWithUuid = new TreeMap[String, RDD[(Int, Double)]]
    var distsWithoutUuid = new TreeMap[String, RDD[(Int, Double)]]
    /* xmax, ev, evNeg */
    var parametersByUuid = new TreeMap[String, (Double, Double, Double)]
    /* evDistances*/
    var evDistanceByUuid = new TreeMap[String, Double]
    
    var appsByUuid = new TreeMap[String, Set[String]]

    println("Calculating aPriori.")
    val aPrioriDistribution = DynamoAnalysisUtil.getApriori(allRates)
    println("Calculated aPriori.")
    if (aPrioriDistribution.size == 0)
      println("WARN: a priori dist is empty!")
    else
      println("a priori dist:\n" + aPrioriDistribution.mkString("\n"))

    var allApps = allRates.flatMap(_.allApps).collect().toSet
    val DAEMONS_LIST_GLOBBED = DynamoAnalysisUtil.daemons_globbed(allApps)
    allApps --= DAEMONS_LIST_GLOBBED
    println("AllApps (no daemons): " + allApps)

    for (os <- oses) {
      val fromOs = allRates.filter(_.os == os)
      val notFromOs = allRates.filter(_.os != os)
      // no distance check, not bug or hog
      plotDists(sem, sc, "iOS " + os, "Other versions", fromOs, notFromOs, aPrioriDistribution, false, plotDirectory)
    }

    for (model <- models) {
      val fromModel = allRates.filter(_.model == model)
      val notFromModel = allRates.filter(_.model != model)
      // no distance check, not bug or hog
      plotDists(sem, sc, model, "Other models", fromModel, notFromModel, aPrioriDistribution, false, plotDirectory)
    }

    var allHogs = new HashSet[String]
    /* Hogs: Consider all apps except daemons. */
    for (app <- allApps) {
      val filtered = allRates.filter(_.allApps.contains(app))
      val filteredNeg = allRates.filter(!_.allApps.contains(app))
      if (plotDists(sem, sc, "Hog " + app + " running", app + " not running", filtered, filteredNeg, aPrioriDistribution, true, plotDirectory)) {
        // this is a hog
        allHogs += app
      }
    }
    
    val uuidArray = uuidToOsAndModel.keySet.toArray.sortWith((s, t) => {
      s < t
    })
    
    for (i <- 0 until uuidArray.length) {
      val uuid = uuidArray(i)
      /* cache these because they will be used numberOfApps times */ 
      val fromUuid = allRates.filter(_.uuid == uuid).cache()

      var uuidApps = fromUuid.flatMap(_.allApps).collect().toSet
      uuidApps --= DAEMONS_LIST_GLOBBED
      val nonHogApps = uuidApps -- allHogs
    
      if (uuidApps.size > 0)
        similarApps(sem, sc, allRates, aPrioriDistribution, i, uuidApps, plotDirectory)
        /* cache these because they will be used numberOfApps times */
      val notFromUuid = allRates.filter(_.uuid != uuid).cache()
      // no distance check, not bug or hog
      val (xmax, bucketed, bucketedNeg, ev, evNeg, evDistance) = DynamoAnalysisUtil.getDistanceAndDistributions(sc, fromUuid, notFromUuid, aPrioriDistribution, buckets, smallestBucket, DECIMALS, DEBUG)
      if (bucketed != null && bucketedNeg != null) {
        distsWithUuid += ((uuid, bucketed))
        distsWithoutUuid += ((uuid, bucketedNeg))
        parametersByUuid += ((uuid, (xmax, ev, evNeg)))
        evDistanceByUuid += ((uuid, evDistance))
      }
      appsByUuid += ((uuid, uuidApps))

      /* Bugs: Only consider apps reported from this uuId. Only consider apps not known to be hogs. */
      for (app <- nonHogApps) {
        val appFromUuid = fromUuid.filter(_.allApps.contains(app))
        val appNotFromUuid = notFromUuid.filter(_.allApps.contains(app))
        plotDists(sem, sc, "Bug " + app + " running on client " + i, app + " running on other clients", appFromUuid, appNotFromUuid, aPrioriDistribution, true, plotDirectory)
      }
    }
    plotJScores(sem, distsWithUuid, distsWithoutUuid, parametersByUuid, evDistanceByUuid, appsByUuid, plotDirectory)
    
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
  def similarApps(sem:Semaphore, sc:SparkContext, all: RDD[CaratRate], aPrioriDistribution: Array[(Double, Double)], i: Int, uuidApps: Set[String], plotDirectory:String) {
    val sCount = similarityCount(uuidApps.size)
    printf("SimilarApps client=%s sCount=%s uuidApps.size=%s\n", i, sCount, uuidApps.size)
    val similar = all.filter(_.allApps.intersect(uuidApps).size >= sCount)
    val dissimilar = all.filter(_.allApps.intersect(uuidApps).size < sCount)
    //printf("SimilarApps similar.count=%s dissimilar.count=%s\n",similar.count(), dissimilar.count())
    // no distance check, not bug or hog
    plotDists(sem, sc, "Similar to client " + i, "Not similar to client "+i, similar, dissimilar, aPrioriDistribution, false, plotDirectory)
  }

  /* TODO: Generate a gnuplot-readable plot file of the bucketed distribution.
   * Create folders plots/data plots/plotfiles
   * Save it as "plots/data/titleWith-titleWithout".txt.
   * Also generate a plotfile called plots/plotfiles/titleWith-titleWithout.gnuplot
   */
  def plotDists(sem: Semaphore, sc:SparkContext, title: String, titleNeg: String, one: RDD[CaratRate], two: RDD[CaratRate], aPrioriDistribution: Array[(Double, Double)], isBugOrHog: Boolean, plotDirectory:String) = {
    val (xmax, bucketed, bucketedNeg, ev, evNeg, evDistance) = DynamoAnalysisUtil.getDistanceAndDistributions(sc, one, two, aPrioriDistribution, buckets, smallestBucket, DECIMALS, DEBUG)
    if (bucketed != null && bucketedNeg != null && (!isBugOrHog || evDistance > 0)) {
      //scheduler.execute(
          plot(sem, title, titleNeg, xmax, bucketed, bucketedNeg, ev, evNeg, evDistance, plotDirectory)
      //)
    }
    isBugOrHog && evDistance > 0
  }

  def plot(sem: Semaphore, title: String, titleNeg: String, xmax: Double, distWith: RDD[(Int, Double)],
    distWithout: RDD[(Int, Double)],
    ev: Double, evNeg: Double, evDistance: Double, plotDirectory: String, apps: Seq[String] = null) {
    sem.acquireUninterruptibly()
    plotSerial(title, titleNeg, xmax, distWith, distWithout, ev, evNeg, evDistance, plotDirectory)
    sem.release()
  }

   /**
     * The J-Score is the % of people with worse = higher energy use.
     * therefore, it is the size of the set of evDistances that are higher than mine,
     * compared to the size of the user base.
     * Note that the server side multiplies the JScore by 100, and we store it here
     * as a fraction.
     */
  def plotJScores(sem:Semaphore, distsWithUuid: TreeMap[String, RDD[(Int, Double)]],
    distsWithoutUuid: TreeMap[String, RDD[(Int, Double)]],
    parametersByUuid: TreeMap[String, (Double, Double, Double)],
    evDistanceByUuid: TreeMap[String, Double],
    appsByUuid: TreeMap[String, Set[String]], plotDirectory:String) {
    val dists = evDistanceByUuid.map(_._2).toSeq.sorted

    for (k <- distsWithUuid.keys) {
      val (xmax, ev, evNeg) = parametersByUuid.get(k).getOrElse((0.0, 0.0, 0.0))
      
      /**
       * jscore is the % of people with worse = higher energy use.
       * therefore, it is the size of the set of evDistances that are higher than mine,
       * compared to the size of the user base.
       */
      val jscore = {
        val temp = evDistanceByUuid.get(k).getOrElse(0.0)
        if (temp == 0)
          0
        else
          ProbUtil.nDecimal(dists.filter(_ > temp).size*1.0 / dists.size, DECIMALS)
      }
      val distWith = distsWithUuid.get(k).getOrElse(null)
      val distWithout = distsWithoutUuid.get(k).getOrElse(null)
      val apps = appsByUuid.get(k).getOrElse(null)
      if (distWith != null && distWithout != null && apps != null)
        //scheduler.execute(
        plot(sem, "Profile for " + k, "Other users", xmax, distWith, distWithout, ev, evNeg, jscore, plotDirectory, apps.toSeq)
        //)
      else
        printf("Error: Could not plot jscore, because: distWith=%s distWithout=%s apps=%s\n", distWith, distWithout, apps)
    }
  }
  
  def plotSerial(title: String, titleNeg: String, xmax:Double,distWith: RDD[(Int, Double)],
    distWithout: RDD[(Int, Double)],
      ev:Double, evNeg:Double, evDistance:Double, plotDirectory:String, apps: Seq[String] = null) {
    
    var fixedTitle = title
    if (title.startsWith("Hog "))
      fixedTitle = title.substring(4)
    else if (title.startsWith("Bug "))
      fixedTitle = title.substring(4)      
    // bump up accuracy here so that not everything gets blurred
    val evTitle = fixedTitle + " (EV="+ProbUtil.nDecimal(ev, DECIMALS+1) +")"
    val evTitleNeg = titleNeg + " (EV=" + ProbUtil.nDecimal(evNeg, DECIMALS+1)+ ")"
    printf("Plotting %s vs %s, distance=%s\n", evTitle, evTitleNeg, evDistance)
    plotFile(dateString, title, evTitle, evTitleNeg, xmax, plotDirectory)
    writeData(dateString, evTitle, distWith, xmax)
    writeData(dateString, evTitleNeg, distWithout, xmax)
    plotData(dateString, title)
  }

  def plotFile(dir: String, name: String, t1: String, t2: String, xmax:Double, plotDirectory:String) = {
    val pdir = dir + "/" + PLOTS + "/"
    val gdir = dir + "/" + PLOTFILES + "/"
    val ddir = dir + "/" + DATA_DIR + "/"
    var f = new File(pdir)
    if (!f.isDirectory() && !f.mkdirs())
      println("Failed to create " + f + " for plots!")
    else {
      f = new File(gdir)
      if (!f.isDirectory() && !f.mkdirs())
        println("Failed to create " + f + " for plots!")
      else {
        f = new File(ddir)
        if (!f.isDirectory() && !f.mkdirs())
          println("Failed to create " + f + " for plots!")
        else {
          val plotfile = new java.io.FileWriter(gdir + name + ".gnuplot")
          plotfile.write("set term postscript eps enhanced color 'Helvetica' 32\nset xtics out\n" +
            "set size 1.93,1.1\n" +
            "set logscale x\n" +
            "set xrange [0.0005:"+(xmax+0.5)+"]\n" +
            "set xlabel \"Battery drain % / s\"\n" +
            "set ylabel \"Probability\"\n")
          if (plotDirectory != null)
            plotfile.write("set output \"" + plotDirectory + "/" + assignSubDir(plotDirectory, name) + name + ".eps\"\n")
          else
            plotfile.write("set output \"" + pdir + name + ".eps\"\n")
          plotfile.write("plot \"" + ddir + t1 + ".txt\" using 1:2 with linespoints lt rgb \"#f3b14d\" ps 3 lw 5 title \"" + t1.replace("~", "\\\\~").replace("_", "\\\\_") +
              "\", " +
            "\"" + ddir + t2 + ".txt\" using 1:2 with linespoints lt rgb \"#007777\" ps 3 lw 5 title \"" + t2.replace("~", "\\\\~").replace("_", "\\\\_")
            + "\"\n")
          plotfile.close
          true
        }
      }
    }
  }

  def assignSubDir(plotDirectory: String, name: String) = {
    val p = new File(plotDirectory)
    if (!p.isDirectory() && !p.mkdirs()) {
      ""
    } else {
      val dir = name.substring(0, 3) match {
        case Bug => { BUGS }
        case Hog => { HOGS }
        case Pro => { UUIDS }
        case Sim => { SIM }
        case _ => ""
      }
      if (dir.length() > 0) {
        val d = new File(p, dir)
        if (!d.isDirectory() && !d.mkdirs())
          ""
        else
          dir + "/"
      }else
        ""
    }
  }
  
  def writeData(dir:String, name:String, dist: RDD[(Int, Double)], xmax:Double){
    val logbase = ProbUtil.getLogBase(buckets, smallestBucket, xmax)
    val ddir = dir + "/" + DATA_DIR + "/"
    var f = new File(ddir)
    if (!f.isDirectory() && !f.mkdirs())
      println("Failed to create " + f + " for plots!")
    else {
      val datafile = new java.io.FileWriter(ddir + name + ".txt")

      val dataPairs = dist.map(x => {
        val bucketStart = {
          if (x._1 == 0)
            0.0
          else
            xmax / (math.pow(logbase, buckets - x._1))
        }
        val bucketEnd = xmax / (math.pow(logbase, buckets - x._1 - 1))
        
       ((bucketStart+bucketEnd)/2, x._2)
      }).collect()
      var dataMap = new TreeMap[Double, Double]
      dataMap ++= dataPairs
      
      for (k <- dataMap)
        datafile.write(k._1 +" "+k._2 +"\n")
      datafile.close
    }
  }

  def plotData(dir: String, title: String) {
    val gdir = dir + "/" + PLOTFILES + "/"
    val f = new File(gdir)
    if (!f.isDirectory() && !f.mkdirs())
      println("Failed to create " + f + " for plots!")
    else {
      val temp = Runtime.getRuntime().exec(Array("gnuplot",  gdir + title + ".gnuplot"))
      val err_read = new java.io.BufferedReader(new java.io.InputStreamReader(temp.getErrorStream()))
      var line = err_read.readLine()
      while (line != null) {
        println(line)
        line = err_read.readLine()
      }
      temp.waitFor()
    }
  }
  
  def plotSamples(title: String, plotDirectory:String, data: TreeMap[String, TreeSet[Double]]) {
    println("Plotting samples.")
    writeSampleData(dateString, title, data)
  }
  
  def writeSampleData(dir:String, name:String, data: TreeMap[String, TreeSet[Double]]){
    val ddir = dir + "/" + DATA_DIR + "/"
    var f = new File(ddir)
    if (!f.isDirectory() && !f.mkdirs())
      println("Failed to create " + f + " for plots!")
    else {
      val datafile = new java.io.FileWriter(ddir + name + ".txt")
      val arr = data.toArray[(String, TreeSet[Double])]
      val ret = arr.sortWith((x, y) => {
        x._2.size > y._2.size
      })
      for (k <- ret)
        for (j <- k._2)
          datafile.write(k._1 +" "+j +"\n")
      datafile.close
    }
  }
}
