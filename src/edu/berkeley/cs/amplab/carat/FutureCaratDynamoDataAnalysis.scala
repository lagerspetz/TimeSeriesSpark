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
import java.io.BufferedReader
import java.io.InputStreamReader
import edu.berkeley.cs.amplab.carat.dynamodb.DynamoAnalysisUtil
import edu.berkeley.cs.amplab.carat.dynamodb.RemoveDaemons
import java.io.File
import com.amazonaws.services.dynamodb.model.Key
import edu.berkeley.cs.amplab.carat.dynamodb.DynamoDbDecoder
import edu.berkeley.cs.amplab.carat.dynamodb.DynamoDbEncoder

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
 * Note: We do not store hogs or bugs with negative distance values.
 *
 * @author Eemil Lagerspetz
 */

object FutureCaratDynamoDataAnalysis {

  // Bucketing and decimal constants
  val BUCKETS = 100
  val SMALLEST_BUCKET = 0.0001
  val DECIMALS = 3
  var DEBUG = false
  val LIMIT_SPEED = false  

  val tmpdir = "/mnt/TimeSeriesSpark-unstable/spark-temp-future/"
  val RATES_CACHED_NEW = tmpdir+"cached-rates-new.dat"
  val RATES_CACHED = tmpdir+"cached-rates.dat"
  val LAST_SAMPLE = tmpdir+"last-sample.txt"
  val LAST_REG = tmpdir+"last-reg.txt"
  
  val last_sample = DynamoAnalysisUtil.readDoubleFromFile(LAST_SAMPLE)
  
  var last_sample_write = 0.0
  
  val last_reg = DynamoAnalysisUtil.readDoubleFromFile(LAST_REG)
  
  var last_reg_write = 0.0
  
  /**
   * Main program entry point.
   */
  def main(args: Array[String]) {
    var master = "local[1]"
    if (args != null && args.length >= 1) {
      master = args(0)
      if (args.length > 1 && args(1) == "DEBUG")
        DEBUG = true
    }
    // turn off INFO logging for spark:
    System.setProperty("hadoop.root.logger", "WARN,console")
    // Hopefully turn on ProbUtil debug logging:
    System.setProperty("log4j.logger.spark.timeseries.ProbUtil", "DEBUG")
    // Fix Spark running out of space on AWS.
    System.setProperty("spark.local.dir", "/mnt/TimeSeriesSpark/spark-temp")
    val sc = new SparkContext(master, "CaratDynamoDataAnalysis")
    analyzeData(sc)
    sys.exit(0)
  }

  /**
   * Main function. Called from main() after sc initialization.
   */

  def analyzeData(sc: SparkContext) {
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
      analyzeRateData(sc, allRates.cache(), uuidToOsAndModel, allOses, allModels)
    }
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
   * Main analysis function. Called on the entire collected set of CaratRates.
   */
  def analyzeRateData(sc: SparkContext, allRates: RDD[CaratRate],
    uuidToOsAndModel: scala.collection.mutable.HashMap[String, (String, String)], oses: scala.collection.mutable.Set[String], models: scala.collection.mutable.Set[String]) {
    //Remove Daemons
    println("Removing daemons from the database")
    DynamoAnalysisUtil.removeDaemons(DynamoAnalysisUtil.DAEMONS_LIST)
    //Remove old bugs
    println("Clearing bugs")
    DynamoDbDecoder.deleteAllItems(bugsTable, resultKey, hogKey)
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
      throw new Error("WARN: aPrioriDistribution is empty!")

    var allApps = allRates.flatMap(_.allApps).collect().toSet
    val DAEMONS_LIST_GLOBBED = DynamoAnalysisUtil.daemons_globbed(allApps)
    allApps --= DAEMONS_LIST_GLOBBED
    println("AllApps (no daemons): " + allApps)

    //Remove Daemons
    println("Removing daemons from the database")
    DynamoAnalysisUtil.removeDaemons(DAEMONS_LIST_GLOBBED)

    for (os <- oses) {
      val fromOs = allRates.filter(_.os == os)
      val notFromOs = allRates.filter(_.os != os)
      // no distance check, not bug or hog
      println("Considering os os=" + os)
      writeTripletUngrouped(sc, fromOs, notFromOs, aPrioriDistribution, DynamoDbEncoder.put(osTable, osKey, os, _, _, _, _, _, _),
        { println("ERROR: Delete called for a non-bug non-hog!") }, false)
    }

    for (model <- models) {
      val fromModel = allRates.filter(_.model == model)
      val notFromModel = allRates.filter(_.model != model)
      // no distance check, not bug or hog
      println("Considering model model=" + model)
      writeTripletUngrouped(sc, fromModel, notFromModel, aPrioriDistribution, DynamoDbEncoder.put(modelsTable, modelKey, model, _, _, _, _, _, _),
        { println("ERROR: Delete called for a non-bug non-hog!") }, false)
    }

    var allHogs = new HashSet[String]
    /* Hogs: Consider all apps except daemons. */
    for (app <- allApps) {
      val filtered = allRates.filter(_.allApps.contains(app))
      val filteredNeg = allRates.filter(!_.allApps.contains(app))
      println("Considering hog app=" + app)
      if (writeTripletUngrouped(sc, filtered, filteredNeg, aPrioriDistribution, DynamoDbEncoder.put(hogsTable, hogKey, app, _, _, _, _, _, _),
        DynamoDbDecoder.deleteItem(hogsTable, app), true)) {
        // this is a hog
        allHogs += app
      }
    }
    
    val uuidArray = uuidToOsAndModel.keySet.toArray.sortWith((s, t) => {
      s < t
    })
    
    for (i <- 0 until uuidArray.length) {
      val uuid = uuidArray(i)
      val fromUuid = allRates.filter(_.uuid == uuid)

      var uuidApps = fromUuid.flatMap(_.allApps).collect().toSet
      uuidApps --= DynamoAnalysisUtil.DAEMONS_LIST
      val nonHogApps = uuidApps -- allHogs

      if (uuidApps.size > 0)
        similarApps(sc, allRates, uuid, uuidApps, aPrioriDistribution)
      //else
      // Remove similar apps entry?

      val notFromUuid = allRates.filter(_.uuid != uuid)
      // no distance check, not bug or hog
      println("Considering jscore uuid=" + uuid)
      val (xmax, bucketed, bucketedNeg, ev, evNeg, evDistance, usersWith, usersWithout) = DynamoAnalysisUtil.getDistanceAndDistributions(sc, fromUuid, notFromUuid, aPrioriDistribution, BUCKETS, SMALLEST_BUCKET, DECIMALS, DEBUG)
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
          println("Considering bug app=" + app + " uuid=" + uuid)
          writeTripletUngrouped(sc, appFromUuid, appNotFromUuid, aPrioriDistribution, DynamoDbEncoder.putBug(bugsTable, (resultKey, hogKey), (uuid, app), _, _, _, _, _, _),
            DynamoDbDecoder.deleteItem(bugsTable, uuid, app), true)
        }
      }
    }
    // Save J-Scores of all users.
    writeJScores(distsWithUuid, distsWithoutUuid, parametersByUuid, evDistanceByUuid, appsByUuid)
  }

  /**
   * Calculate similar apps for device `uuid` based on all rate measurements and apps reported on the device.
   * Write them to DynamoDb.
   */
  def similarApps(sc: SparkContext, all: RDD[CaratRate], uuid: String, uuidApps: Set[String], aPrioriDistribution: Array[(Double, Double)]) {
    val sCount = similarityCount(uuidApps.size)
    printf("SimilarApps uuid=%s sCount=%s uuidApps.size=%s\n", uuid, sCount, uuidApps.size)
    val similar = all.filter(_.allApps.intersect(uuidApps).size >= sCount)
    val dissimilar = all.filter(_.allApps.intersect(uuidApps).size < sCount)
    //printf("SimilarApps similar.count=%s dissimilar.count=%s\n",similar.count(), dissimilar.count())
    // no distance check, not bug or hog
    println("Considering similarApps uuid=" + uuid)
    writeTripletUngrouped(sc, similar, dissimilar, aPrioriDistribution, DynamoDbEncoder.put(similarsTable, similarKey, uuid, _, _, _, _, _, _),
      { DynamoDbDecoder.deleteItem(similarsTable, uuid) }, false)
  }

  /**
   * Write the probability distributions, the distance, and the xmax value to DynamoDb. Ungrouped CaratRates variant.
   */
  def writeTripletUngrouped(sc: SparkContext, one: RDD[CaratRate], two: RDD[CaratRate], aPrioriDistribution: Array[(Double, Double)], putFunction: (Double, Seq[(Int, Double)], Seq[(Int, Double)], Double, Double, Double) => Unit,
    deleteFunction: => Unit, isBugOrHog: Boolean) = {
    val (xmax, bucketed, bucketedNeg, ev, evNeg, evDistance, usersWith, usersWithout) = DynamoAnalysisUtil.getDistanceAndDistributions(sc, one, two, aPrioriDistribution, BUCKETS, SMALLEST_BUCKET, DECIMALS, DEBUG)
    if (bucketed != null && bucketedNeg != null) {
      if (evDistance > 0 || !isBugOrHog) {
        putFunction(xmax, bucketed.collect(), bucketedNeg.collect(), evDistance, ev, evNeg)
      } else if (evDistance <= 0 && isBugOrHog) {
        /* We should probably remove it in this case. */
        deleteFunction
      }
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
  def writeJScores(distsWithUuid: TreeMap[String, RDD[(Int, Double)]],
    distsWithoutUuid: TreeMap[String, RDD[(Int, Double)]],
    parametersByUuid: TreeMap[String, (Double, Double, Double)],
    evDistanceByUuid: TreeMap[String, Double],
    appsByUuid: TreeMap[String, Set[String]]) {
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
          ProbUtil.nDecimal(dists.filter(_ > temp).size * 1.0 / dists.size, DECIMALS)
      }
      val distWith = distsWithUuid.get(k).getOrElse(null)
      val distWithout = distsWithoutUuid.get(k).getOrElse(null)
      val apps = appsByUuid.get(k).getOrElse(null)
      if (distWith != null && distWithout != null && apps != null)
        DynamoDbEncoder.put(resultsTable, resultKey, k, xmax, distWith.collect(), distWithout.collect(), jscore, ev, evNeg, apps.toSeq)
      else
        printf("Error: Could not save jscore, because: distWith=%s distWithout=%s apps=%s\n", distWith, distWithout, apps)
    }
    //DynamoDbEncoder.put(xmax, bucketed.toArray[(Int, Double)], bucketedNeg.toArray[(Int, Double)], jScore, ev, evNeg)
  }
}
