package edu.berkeley.cs.amplab.carat

import spark._
import spark.SparkContext._
import spark.timeseries._
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
 * @author Eemil Lagerspetz
 */

object CaratDynamoDataToPlots {
  /**
   * We do not store hogs or bugs with negative distance values.
   */

  // Bucketing and decimal constants
  val buckets = 100
  val smallestBucket = 0.0001
  val DECIMALS = 3
  var DEBUG = false
  val LIMIT_SPEED = false
  val ABNORMAL_RATE = 9
  
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

  def plotEverything(master:String, debug:Boolean, plotDirectory:String) = {
    if (debug)
      DEBUG = true
    // turn on ProbUtil debug logging
    System.setProperty("log4j.category.spark.timeseries.ProbUtil.threshold", "DEBUG")
    // turn off INFO logging for spark:
    System.setProperty("hadoop.root.logger", "WARN,console")
    // This is misspelled in the spark jar log4j.properties:
    System.setProperty("log4j.threshhold", "WARN")
    // Include correct spelling to make sure
    System.setProperty("log4j.threshold", "WARN")

    val sc = new SparkContext(master, "CaratDynamoDataToPlots")
    analyzeData(sc, plotDirectory)
  }
  
  /**
   * Main function. Called from main() after sc initialization.
   */

  def analyzeData(sc: SparkContext, plotDirectory:String) = {
    // Unique uuIds, Oses, and Models from registrations.
    val allUuids = new scala.collection.mutable.HashSet[String]
    val allModels = new scala.collection.mutable.HashSet[String]
    val allOses = new scala.collection.mutable.HashSet[String]

    // Master RDD for all data.
    var allRates: spark.RDD[CaratRate] = null

    allRates = FutureCaratDynamoDataAnalysis.DynamoDbItemLoop(DynamoDbDecoder.getAllItems(registrationTable),
      DynamoDbDecoder.getAllItems(registrationTable, _),
      handleRegs(sc, _, _, allUuids, allOses, allModels, plotDirectory), false, allRates)
      
    println("All uuIds: " + allUuids.mkString(", "))
    println("All oses: " + allOses.mkString(", "))
    println("All models: " + allModels.mkString(", "))

    if (allRates != null) {
      analyzeRateData(sc, allRates, allUuids, allOses, allModels, plotDirectory)
    }else
      null
  }
  
   /**
   * Handles a set of registration messages from the Carat DynamoDb.
   * Samples matching each registration identifier are got, rates calculated from them, and combined with `dist`.
   * uuids, oses and models are filled during registration message handling. Returns the updated version of `dist`.
   */
  def handleRegs(sc: SparkContext, regs: java.util.List[java.util.Map[String, AttributeValue]], dist: spark.RDD[CaratRate], uuids: scala.collection.mutable.Set[String], oses: scala.collection.mutable.Set[String], models: scala.collection.mutable.Set[String], plotDirectory:String) = {
    /* FIXME: I would like to do this in parallel, but that would not let me re-use
     * all the data for the other uuids, resulting in n^2 execution time.
     */

    // Remove duplicates caused by re-registrations:
    var regSet: Set[(String, String, String)] = new HashSet[(String, String, String)]
    regSet ++= regs.map(x => {
      val uuid = { val attr = x.get(regsUuid); if (attr != null) attr.getS() else "" }
      val model = { val attr = x.get(regsModel); if (attr != null) attr.getS() else "" }
      val os = { val attr = x.get(regsOs); if (attr != null) attr.getS() else "" }
      (uuid, model, os)
    })

    var distRet: spark.RDD[CaratRate] = dist
    for (x <- regSet) {
      val uuid = x._1
      val model = x._2
      val os = x._3

      // Collect all uuids, models and oses in the same loop
      uuids += uuid
      models += model
      oses += os

      println("Handling reg:" + x)

      /* Limit attributesToGet here so that bandwidth is not used for nothing. Right now the memory attributes of samples are not considered. */
      distRet = FutureCaratDynamoDataAnalysis.DynamoDbItemLoop(DynamoDbDecoder.getItems(samplesTable, uuid, null, Seq(sampleKey, sampleProcesses,sampleTime,sampleBatteryState,sampleBatteryLevel,sampleEvent)),
        DynamoDbDecoder.getItems(samplesTable, uuid, _, Seq(sampleKey, sampleProcesses,sampleTime,sampleBatteryState,sampleBatteryLevel,sampleEvent)),
        FutureCaratDynamoDataAnalysis.handleSamples(sc, _, os, model, _),
        true,
        distRet)
      /* 
       * distRet here augments by 1 user at a time, so statistics for rates while adding a user at a time can be calculated here
       * 
       */
        //analyzeRateDataStdDevsOverTime(sc, distRet, uuids, oses, models, plotDirectory)
    }
    /*
     * TODO: Stddev of samples per user over time,
     * stddev of distributions (hog, etc) per all users over increasing number of users,
     * change of distance of distributions (hog, etc) over increasing number of users.
     */
    distRet
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
    
    var appsByUuid = new TreeMap[String, scala.collection.mutable.HashSet[String]]

    /*if (DEBUG) {
      val cc = allRates.collect()
      for (k <- cc)
        println(k)
    }*/
    
    val aPrioriDistribution = FutureCaratDynamoDataAnalysis.getApriori(allRates)

    val apps = allRates.map(x => {
      var sampleApps = x.allApps
      sampleApps --= FutureCaratDynamoDataAnalysis.DAEMONS_LIST
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
      plotDists(sc, "iOs " + os, "Other versions", fromOs, notFromOs, aPrioriDistribution, false, plotDirectory)
    }

    for (model <- models) {
      val fromModel = allRates.filter(_.model == model)
      val notFromModel = allRates.filter(_.model != model)
      // no distance check, not bug or hog
      plotDists(sc, model, "Other models", fromModel, notFromModel, aPrioriDistribution, false, plotDirectory)
    }

    var allHogs = new HashSet[String]
    /* Hogs: Consider all apps except daemons. */
    for (app <- allApps) {
      if (app != CARAT) {
        val filtered = allRates.filter(_.allApps.contains(app))
        val filteredNeg = allRates.filter(!_.allApps.contains(app))
        if (plotDists(sc, "Hog " + app, "Other apps", filtered, filteredNeg, aPrioriDistribution, true, plotDirectory)) {
          // this is a hog
          allHogs += app
        }
      }
    }

    var intersectEverReportedApps = new scala.collection.mutable.HashSet[String]
    var intersectPerSampleApps = new scala.collection.mutable.HashSet[String]

    for (uuid <- uuids) {
      val fromUuid = allRates.filter(_.uuid == uuid)

      val tempApps = fromUuid.map(x => {
        var sampleApps = x.allApps
        sampleApps --= FutureCaratDynamoDataAnalysis.DAEMONS_LIST
        sampleApps --= allHogs
        sampleApps
      }).collect()

      val uuidAppsTemp = fromUuid.map(x => {
        var sampleApps = x.allApps
        sampleApps --= FutureCaratDynamoDataAnalysis.DAEMONS_LIST
        sampleApps
      }).collect()

      var uuidApps = new scala.collection.mutable.HashSet[String]
      var nonHogApps = new scala.collection.mutable.HashSet[String]

      // Get all apps ever reported, also compute likely daemons
      for (k <- tempApps) {
        nonHogApps ++= k
        if (intersectPerSampleApps.size == 0)
          intersectPerSampleApps ++= k
        else if (k.size > 0)
          intersectPerSampleApps = intersectPerSampleApps.intersect(k)
      }

      for (k <- uuidAppsTemp) {
        uuidApps ++= k
      }

      //Another method to find likely daemons
      if (intersectEverReportedApps.size == 0)
        intersectEverReportedApps = uuidApps
      else if (uuidApps.size > 0)
        intersectEverReportedApps = intersectEverReportedApps.intersect(uuidApps)

      if (uuidApps.size > 0)
        similarApps(sc, allRates, aPrioriDistribution, uuid, uuidApps, plotDirectory)
      //else
      // Remove similar apps entry?

      val notFromUuid = allRates.filter(_.uuid != uuid)
      // no distance check, not bug or hog
      val (xmax, bucketed, bucketedNeg, ev, evNeg, evDistance) = FutureCaratDynamoDataAnalysis.getDistanceAndDistributions(sc, fromUuid, notFromUuid, aPrioriDistribution)
      if (bucketed != null && bucketedNeg != null) {
        distsWithUuid += ((uuid, bucketed))
        distsWithoutUuid += ((uuid, bucketedNeg))
        parametersByUuid += ((uuid, (xmax, ev, evNeg)))
        evDistanceByUuid += ((uuid, evDistance))
      }
      appsByUuid += ((uuid, uuidApps))

      /* Bugs: Only consider apps reported from this uuId. Only consider apps not known to be hogs. */
      for (app <- nonHogApps) {
        if (app != CARAT) {
          val appFromUuid = fromUuid.filter(_.allApps.contains(app))
          val appNotFromUuid = notFromUuid.filter(_.allApps.contains(app))
          plotDists(sc, "Bug "+app + " on " + uuid, app + " elsewhere", appFromUuid, appNotFromUuid, aPrioriDistribution, true, plotDirectory)
        }
      }
    }
    
    plotJScores(distsWithUuid, distsWithoutUuid, parametersByUuid, evDistanceByUuid, appsByUuid, plotDirectory)
    
    val removed = FutureCaratDynamoDataAnalysis.DAEMONS_LIST -- intersectEverReportedApps
    val removedPS = FutureCaratDynamoDataAnalysis.DAEMONS_LIST -- intersectPerSampleApps
    intersectEverReportedApps --= FutureCaratDynamoDataAnalysis.DAEMONS_LIST
    intersectPerSampleApps --= FutureCaratDynamoDataAnalysis.DAEMONS_LIST
    println("Daemons: " + FutureCaratDynamoDataAnalysis.DAEMONS_LIST)
    if (intersectEverReportedApps.size > 0)
      println("New possible daemons (ever reported): " + intersectEverReportedApps)
    if (intersectPerSampleApps.size > 0)
      println("New possible daemons (per sample): " + intersectPerSampleApps)
    if (removed.size > 0)
      println("Removed daemons (ever reported): " + removed)
    if (removedPS.size > 0)
      println("Removed daemons (per sample): " + removedPS)
    // return plot directory for caller
    dateString + "/" + PLOTS
  }
  
  /**
   * Main analysis function. Called on the entire collected set of CaratRates.
   */
  def analyzeRateData(sc:SparkContext, allRates: RDD[CaratRate],
    uuids: scala.collection.mutable.Set[String], oses: scala.collection.mutable.Set[String], models: scala.collection.mutable.Set[String], plotDirectory:String) = {
    
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
    
    var appsByUuid = new TreeMap[String, scala.collection.mutable.HashSet[String]]

    /*if (DEBUG) {
      val cc = allRates.collect()
      for (k <- cc)
        println(k)
    }*/
    
    val aPrioriDistribution = FutureCaratDynamoDataAnalysis.getApriori(allRates)

    val apps = allRates.map(x => {
      var sampleApps = x.allApps
      sampleApps --= FutureCaratDynamoDataAnalysis.DAEMONS_LIST
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
      plotDists(sc, "iOs " + os, "Other versions", fromOs, notFromOs, aPrioriDistribution, false, plotDirectory)
    }

    for (model <- models) {
      val fromModel = allRates.filter(_.model == model)
      val notFromModel = allRates.filter(_.model != model)
      // no distance check, not bug or hog
      plotDists(sc, model, "Other models", fromModel, notFromModel, aPrioriDistribution, false, plotDirectory)
    }

    var allHogs = new HashSet[String]
    /* Hogs: Consider all apps except daemons. */
    for (app <- allApps) {
      if (app != CARAT) {
        val filtered = allRates.filter(_.allApps.contains(app))
        val filteredNeg = allRates.filter(!_.allApps.contains(app))
        if (plotDists(sc, "Hog " + app, "Other apps", filtered, filteredNeg, aPrioriDistribution, true, plotDirectory)) {
          // this is a hog
          allHogs += app
        }
      }
    }

    var intersectEverReportedApps = new scala.collection.mutable.HashSet[String]
    var intersectPerSampleApps = new scala.collection.mutable.HashSet[String]

    for (uuid <- uuids) {
      val fromUuid = allRates.filter(_.uuid == uuid)

      val tempApps = fromUuid.map(x => {
        var sampleApps = x.allApps
        sampleApps --= FutureCaratDynamoDataAnalysis.DAEMONS_LIST
        sampleApps --= allHogs
        sampleApps
      }).collect()

      val uuidAppsTemp = fromUuid.map(x => {
        var sampleApps = x.allApps
        sampleApps --= FutureCaratDynamoDataAnalysis.DAEMONS_LIST
        sampleApps
      }).collect()

      var uuidApps = new scala.collection.mutable.HashSet[String]
      var nonHogApps = new scala.collection.mutable.HashSet[String]

      // Get all apps ever reported, also compute likely daemons
      for (k <- tempApps) {
        nonHogApps ++= k
        if (intersectPerSampleApps.size == 0)
          intersectPerSampleApps ++= k
        else if (k.size > 0)
          intersectPerSampleApps = intersectPerSampleApps.intersect(k)
      }

      for (k <- uuidAppsTemp) {
        uuidApps ++= k
      }


      //Another method to find likely daemons
      if (intersectEverReportedApps.size == 0)
        intersectEverReportedApps = uuidApps
      else if (uuidApps.size > 0)
        intersectEverReportedApps = intersectEverReportedApps.intersect(uuidApps)

      if (uuidApps.size > 0)
        similarApps(sc, allRates, aPrioriDistribution, uuid, uuidApps, plotDirectory)
      //else
      // Remove similar apps entry?

      val notFromUuid = allRates.filter(_.uuid != uuid)
      // no distance check, not bug or hog
      val (xmax, bucketed, bucketedNeg, ev, evNeg, evDistance) = FutureCaratDynamoDataAnalysis.getDistanceAndDistributions(sc, fromUuid, notFromUuid, aPrioriDistribution)
      if (bucketed != null && bucketedNeg != null) {
        distsWithUuid += ((uuid, bucketed))
        distsWithoutUuid += ((uuid, bucketedNeg))
        parametersByUuid += ((uuid, (xmax, ev, evNeg)))
        evDistanceByUuid += ((uuid, evDistance))
      }
      appsByUuid += ((uuid, uuidApps))

      /* Bugs: Only consider apps reported from this uuId. Only consider apps not known to be hogs. */
      for (app <- nonHogApps) {
        if (app != CARAT) {
          val appFromUuid = fromUuid.filter(_.allApps.contains(app))
          val appNotFromUuid = notFromUuid.filter(_.allApps.contains(app))
          plotDists(sc, "Bug "+app + " on " + uuid, app + " elsewhere", appFromUuid, appNotFromUuid, aPrioriDistribution, true, plotDirectory)
        }
      }
    }
    
    plotJScores(distsWithUuid, distsWithoutUuid, parametersByUuid, evDistanceByUuid, appsByUuid, plotDirectory)
    
    val removed = FutureCaratDynamoDataAnalysis.DAEMONS_LIST -- intersectEverReportedApps
    val removedPS = FutureCaratDynamoDataAnalysis.DAEMONS_LIST -- intersectPerSampleApps
    intersectEverReportedApps --= FutureCaratDynamoDataAnalysis.DAEMONS_LIST
    intersectPerSampleApps --= FutureCaratDynamoDataAnalysis.DAEMONS_LIST
    println("Daemons: " + FutureCaratDynamoDataAnalysis.DAEMONS_LIST)
    if (intersectEverReportedApps.size > 0)
      println("New possible daemons (ever reported): " + intersectEverReportedApps)
    if (intersectPerSampleApps.size > 0)
      println("New possible daemons (per sample): " + intersectPerSampleApps)
    if (removed.size > 0)
      println("Removed daemons (ever reported): " + removed)
    if (removedPS.size > 0)
      println("Removed daemons (per sample): " + removedPS)
    // return plot directory for caller
    dateString + "/" + PLOTS
  }

  /**
   * Calculate similar apps for device `uuid` based on all rate measurements and apps reported on the device.
   * Write them to DynamoDb.
   */
  def similarApps(sc:SparkContext, all: RDD[CaratRate], aPrioriDistribution: RDD[(Double, Double)], uuid: String, uuidApps: scala.collection.mutable.Set[String], plotDirectory:String) {
    val sCount = similarityCount(uuidApps.size)
    printf("SimilarApps uuid=%s sCount=%s uuidApps.size=%s\n", uuid, sCount, uuidApps.size)
    val similar = all.filter(_.allApps.intersect(uuidApps).size >= sCount)
    val dissimilar = all.filter(_.allApps.intersect(uuidApps).size < sCount)
    //printf("SimilarApps similar.count=%s dissimilar.count=%s\n",similar.count(), dissimilar.count())
    // no distance check, not bug or hog
    plotDists(sc, "Similar users with " + uuid, "Dissimilar users", similar, dissimilar, aPrioriDistribution, false, plotDirectory)
  }

  /* TODO: Generate a gnuplot-readable plot file of the bucketed distribution.
   * Create folders plots/data plots/plotfiles
   * Save it as "plots/data/titleWith-titleWithout".txt.
   * Also generate a plotfile called plots/plotfiles/titleWith-titleWithout.gnuplot
   */
  def plotDists(sc:SparkContext, title: String, titleNeg: String, one: RDD[CaratRate], two: RDD[CaratRate], aPrioriDistribution: RDD[(Double, Double)], isBugOrHog: Boolean, plotDirectory:String) = {
    val (xmax, bucketed, bucketedNeg, ev, evNeg, evDistance) = FutureCaratDynamoDataAnalysis.getDistanceAndDistributions(sc, one, two, aPrioriDistribution)

    if (bucketed != null && bucketedNeg != null && (!isBugOrHog || evDistance > 0)) {
      plot(title, titleNeg, xmax, bucketed, bucketedNeg, ev, evNeg, evDistance, plotDirectory)
    }
    isBugOrHog && evDistance > 0
  }

   /**
     * The J-Score is the % of people with worse = higher energy use.
     * therefore, it is the size of the set of evDistances that are higher than mine,
     * compared to the size of the user base.
     * Note that the server side multiplies the JScore by 100, and we store it here
     * as a fraction.
     */
  def plotJScores(distsWithUuid: TreeMap[String, RDD[(Int, Double)]],
    distsWithoutUuid: TreeMap[String, RDD[(Int, Double)]],
    parametersByUuid: TreeMap[String, (Double, Double, Double)],
    evDistanceByUuid: TreeMap[String, Double],
    appsByUuid: TreeMap[String, scala.collection.mutable.HashSet[String]], plotDirectory:String) {
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
        plot("Profile for " + k, "Other users", xmax, distWith, distWithout, ev, evNeg, jscore, plotDirectory, apps.toSeq)
      else
        printf("Error: Could not plot jscore, because: distWith=%s distWithout=%s apps=%s\n", distWith, distWithout, apps)
    }
  }
  
  def plot(title: String, titleNeg: String, xmax:Double,distWith: RDD[(Int, Double)],
    distWithout: RDD[(Int, Double)],
      ev:Double, evNeg:Double, evDistance:Double, plotDirectory:String, apps: Seq[String] = null) {
    val evTitle = title + " ev="+ProbUtil.nDecimal(ev, 3)
    val evTitleNeg = titleNeg + " ev=" + ProbUtil.nDecimal(evNeg, 3)
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
          plotfile.write("set term postscript eps enhanced color 'Arial' 24\nset xtics out\n" +
            "set size 1.93,1.1\n" +
            "set logscale x\n" +
            "set xrange [0.00001:"+(xmax+1)+"]\n" +
            "set xlabel \"Battery drain % / s\"\n" +
            "set ylabel \"Probability\"\n")
          if (plotDirectory != null)
            plotfile.write("set output \"" + plotDirectory + "/" + assignSubDir(plotDirectory, name) + name + ".eps\"\n")
          else
            plotfile.write("set output \"" + pdir + name + ".eps\"\n")
          plotfile.write("plot \"" + ddir + t1 + ".txt\" using 1:2 with linespoints lt rgb \"#f3b14d\" lw 2 title \"" + t1.replace("~", "\\\\~").replace("_", "\\\\_") +
              "\", " +
            "\"" + ddir + t2 + ".txt\" using 1:2 with linespoints lt rgb \"#007777\" lw 2 title \"" + t2.replace("~", "\\\\~").replace("_", "\\\\_")
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

      val data = dist.map(x => {
        val bucketStart = {
          if (x._1 == 0)
            0.0
          else
            xmax / (math.pow(logbase, buckets - x._1))
        }
        val bucketEnd = xmax / (math.pow(logbase, buckets - x._1 - 1))
        
        (bucketStart+bucketEnd)/2 +" "+ x._2
      }).collect()
      
      for (k <- data)
        datafile.write(k +"\n")
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
}
