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
import edu.berkeley.cs.amplab.carat.dynamodb.RemoveDaemons
import java.io.File
import java.io.FileInputStream
import java.io.FileWriter
import edu.berkeley.cs.amplab.carat.dynamodb.DynamoAnalysisUtil

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

object CaratDynamoDataAnalysis {

  // Bucketing and decimal constants
  val BUCKETS = 100
  val SMALLEST_BUCKET = 0.0001
  val DECIMALS = 3
  var DEBUG = false
  val LIMIT_SPEED = false
  val ABNORMAL_RATE = 9

  // Daemons list, read from S3
  val DAEMONS_LIST = DynamoAnalysisUtil.readS3LineSet(BUCKET_WEBSITE, DAEMON_FILE)

  // For saving rates so they do not have to be fetched from DynamoDB every time
  val RATES_CACHED = "/mnt/TimeSeriesSpark/spark-temp/stable-cached-rates.dat"
    // to make sure that the old RATES_CACHED is not overwritten while it is being worked on
  val RATES_CACHED_NEW = "/mnt/TimeSeriesSpark/spark-temp/stable-cached-rates-new.dat"
  val LAST_SAMPLE = "/mnt/TimeSeriesSpark/spark-temp/stable-last-sample.txt"
  val LAST_REG = "/mnt/TimeSeriesSpark/spark-temp/stable-last-reg.txt"

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
    // replace old rate file
    DynamoAnalysisUtil.replaceOldRateFile(RATES_CACHED, RATES_CACHED_NEW)
    sys.exit(0)
  }

  /**
   * Main function. Called from main() after sc initialization.
   */

  def analyzeData(sc: SparkContext) {
    // Unique uuIds, Oses, and Models from registrations.
    val allUuids = new scala.collection.mutable.HashSet[String]
    val allModels = new scala.collection.mutable.HashSet[String]
    val allOses = new scala.collection.mutable.HashSet[String]

    // Master RDD for old data.
    println("Getting old rates from %s".format(RATES_CACHED))
    val oldRates: spark.RDD[CaratRate] = {
      val f = new File(RATES_CACHED)
      if (f.exists()) {
        sc.objectFile(RATES_CACHED)
      } else
        null
    }
    // for all data
    var allRates: spark.RDD[CaratRate] = null

    println("Retrieving rates from DynamoDb starting with timestamp=%f".format(last_reg))
    if (last_reg > 0) {
      allRates = DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.filterItemsAfter(registrationTable, regsTimestamp, last_reg + ""),
        DynamoDbDecoder.filterItemsAfter(registrationTable, regsTimestamp, last_reg + "", _),
        handleRegs(sc, _, _, allUuids, allOses, allModels), false, allRates)
    } else {
      allRates = DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.getAllItems(registrationTable),
        DynamoDbDecoder.getAllItems(registrationTable, _),
        handleRegs(sc, _, _, allUuids, allOses, allModels), false, allRates)
    }

    if (oldRates != null)
      allRates = allRates.union(oldRates)
    println("All uuIds: " + allUuids.mkString(", "))
    println("All oses: " + allOses.mkString(", "))
    println("All models: " + allModels.mkString(", "))

    if (allRates != null) {
      println("Saving rates for next time")
      allRates.saveAsObjectFile(RATES_CACHED_NEW)
      DynamoAnalysisUtil.saveDoubleToFile(last_sample_write, LAST_SAMPLE)
      DynamoAnalysisUtil.saveDoubleToFile(last_reg_write, LAST_REG)
      println("Analysing data")
      analyzeRateData(allRates, allUuids, allOses, allModels)
    }
  }

  /**
   * Handles a set of registration messages from the Carat DynamoDb.
   * Samples matching each registration identifier are got, rates calculated from them, and combined with `dist`.
   * uuids, oses and models are filled during registration message handling. Returns the updated version of `dist`.
   */
  def handleRegs(sc: SparkContext, regs: java.util.List[java.util.Map[String, AttributeValue]], dist: spark.RDD[CaratRate], uuids: scala.collection.mutable.Set[String], oses: scala.collection.mutable.Set[String], models: scala.collection.mutable.Set[String]) = {
    /* FIXME: I would like to do this in parallel, but that would not let me re-use
     * all the data for the other uuids, resulting in n^2 execution time.
     */

    // Get last reg timestamp for set saving
    if (regs.size > 0) {
      last_reg_write = regs.last.get(regsTimestamp).getN().toDouble
    }

    // Remove duplicates caused by re-registrations:
    var regSet: Set[(String, String, String)] = DynamoAnalysisUtil.regSet(regs)

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
      if (last_sample > 0) {
        distRet = DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.getItemsAfterRangeKey(samplesTable, uuid, last_sample + "", null, Seq(sampleKey, sampleProcesses, sampleTime, sampleBatteryState, sampleBatteryLevel, sampleEvent)),
          DynamoDbDecoder.getItemsAfterRangeKey(samplesTable, uuid, last_sample + "", _, Seq(sampleKey, sampleProcesses, sampleTime, sampleBatteryState, sampleBatteryLevel, sampleEvent)),
          handleSamples(sc, _, os, model, _),
          true,
          distRet)
      } else {
        distRet = DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.getItems(samplesTable, uuid, null, Seq(sampleKey, sampleProcesses, sampleTime, sampleBatteryState, sampleBatteryLevel, sampleEvent)),
          DynamoDbDecoder.getItems(samplesTable, uuid, _, Seq(sampleKey, sampleProcesses, sampleTime, sampleBatteryState, sampleBatteryLevel, sampleEvent)),
          handleSamples(sc, _, os, model, _),
          true,
          distRet)
      }
    }
    distRet
  }

  /**
   * Process a bunch of samples, assumed to be in order by uuid and timestamp.
   * will return an RDD of CaratRates. Samples need not be from the same uuid.
   */
  def handleSamples(sc: SparkContext, samples: java.util.List[java.util.Map[java.lang.String, AttributeValue]], os: String, model: String, rates: RDD[CaratRate]) = {
    if (DEBUG)
      if (samples.size < 100) {
        for (x <- samples) {
          for (k <- x) {
            if (k._2.isInstanceOf[Seq[String]])
              print("(" + k._1 + ", length=" + k._2.asInstanceOf[Seq[String]].size + ") ")
            else
              print(k + " ")
          }
          println()
        }
      }

    if (samples.size > 0) {
      val lastSample = samples.last
      last_sample_write = lastSample.get(sampleTime).getN().toDouble
    }

    val mapped = samples.map(DynamoAnalysisUtil.sampleMapper)
    var rateRdd = sc.parallelize[CaratRate]({
      DynamoAnalysisUtil.rateMapperPairwise(os, model, mapped)
    })

    if (rates != null)
      rateRdd = rateRdd.union(rates)
    rateRdd
  }

  /**
   * Main analysis function. Called on the entire collected set of CaratRates.
   */
  def analyzeRateData(allRates: RDD[CaratRate],
    uuids: scala.collection.mutable.Set[String], oses: scala.collection.mutable.Set[String], models: scala.collection.mutable.Set[String]) {
    //Remove Daemons
    println("Removing daemons from the database")
    RemoveDaemons.main(Array("DAEMONS"))
    //Remove old bugs
    println("Clearing bugs")
    DynamoDbDecoder.deleteAllItems(bugsTable, resultKey, hogKey)
    /**
     * uuid distributions, xmax, ev and evNeg
     * FIXME: With many users, this is a lot of data to keep in memory.
     * Consider changing the algorithm and using RDDs.
     */
    var distsWithUuid = new TreeMap[String, TreeMap[Int, Double]]
    var distsWithoutUuid = new TreeMap[String, TreeMap[Int, Double]]
    /* xmax, ev, evNeg */
    var parametersByUuid = new TreeMap[String, (Double, Double, Double)]
    /* evDistances*/
    var evDistanceByUuid = new TreeMap[String, Double]
    // apps by Uuid for all devices
    var appsByUuid = new TreeMap[String, Set[String]]

    var allApps = allRates.flatMap(_.allApps).collect().toSet
    allApps --= DAEMONS_LIST

    // mediaremoted does not get removed here, why?
    println("AllApps (no daemons): " + allApps)

    for (os <- oses) {
      val fromOs = allRates.filter(_.os == os)
      val notFromOs = allRates.filter(_.os != os)
      // no distance check, not bug or hog
      println("Considering os os=" + os)
      writeTripletUngrouped(fromOs, notFromOs, DynamoDbEncoder.put(osTable, osKey, os, _, _, _, _, _, _),
        { println("Delete not implemented for OS versions.") }, false)
    }

    for (model <- models) {
      val fromModel = allRates.filter(_.model == model)
      val notFromModel = allRates.filter(_.model != model)
      // no distance check, not bug or hog
      println("Considering model model=" + model)
      writeTripletUngrouped(fromModel, notFromModel, DynamoDbEncoder.put(modelsTable, modelKey, model, _, _, _, _, _, _),
        { println("Delete not implemented for models.") }, false)
    }

    var allHogs = new HashSet[String]
    /* Hogs: Consider all apps except daemons. */
    for (app <- allApps) {
      val filtered = allRates.filter(_.allApps.contains(app))
      val filteredNeg = allRates.filter(!_.allApps.contains(app))
      println("Considering hog app=" + app)
      if (writeTripletUngrouped(filtered, filteredNeg, DynamoDbEncoder.put(hogsTable, hogKey, app, _, _, _, _, _, _),
        DynamoDbDecoder.deleteItem(hogsTable, app), true)) {
        // this is a hog
        allHogs += app
      }
    }
    // quick fix:
    /*
      1. delete all bugs and similar apps
      2. add hogs
      3. remove non-hogs from hogs database
      4. add bugs
      5. Add similar apps
     */
    /* Right way:
      1. remove non-hogs
      2. remove new hogs from bugs table
      3. remove non-bugs from bugs table
      4. insert new hogs
      5. insert new bugs
      6. remove similarApps sets for users with zero intersections while adding new similarApps sets
    */
    val globalNonHogs = allApps -- allHogs
    println("Removing non-hogs from the hogs table: " + globalNonHogs)
    DynamoDbDecoder.deleteItems(hogsTable, hogKey, globalNonHogs.map(x => {
      (hogKey, x)
    }).toArray: _*)

    for (uuid <- uuids) {
      val fromUuid = allRates.filter(_.uuid == uuid)

       var uuidApps = fromUuid.flatMap(_.allApps).collect().toSet
      uuidApps --= DAEMONS_LIST
      val nonHogApps = uuidApps -- allHogs

      if (uuidApps.size > 0)
        similarApps(allRates, uuid, uuidApps)
      //else
      // Remove similar apps entry?

      val notFromUuid = allRates.filter(_.uuid != uuid)
      // no distance check, not bug or hog
      println("Considering jscore uuid=" + uuid)
      val (xmax, bucketed, bucketedNeg, ev, evNeg, evDistance) = getDistanceAndDistributions(fromUuid, notFromUuid)
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
          writeTripletUngrouped(appFromUuid, appNotFromUuid, DynamoDbEncoder.putBug(bugsTable, (resultKey, hogKey), (uuid, app), _, _, _, _, _, _),
            DynamoDbDecoder.deleteItem(bugsTable, uuid, app), true)
        }
      }
    }
    println("Saving J-Scores")
    // Save J-Scores of all users.
    writeJScores(distsWithUuid, distsWithoutUuid, parametersByUuid, evDistanceByUuid, appsByUuid)
  }

  /**
   * Calculate similar apps for device `uuid` based on all rate measurements and apps reported on the device.
   * Write them to DynamoDb.
   */
  def similarApps(all: RDD[CaratRate], uuid: String, uuidApps: Set[String]) {
    val sCount = similarityCount(uuidApps.size)
    printf("SimilarApps uuid=%s sCount=%s uuidApps.size=%s\n", uuid, sCount, uuidApps.size)
    val similar = all.filter(_.allApps.intersect(uuidApps).size >= sCount)
    val dissimilar = all.filter(_.allApps.intersect(uuidApps).size < sCount)
    //printf("SimilarApps similar.count=%s dissimilar.count=%s\n",similar.count(), dissimilar.count())
    // no distance check, not bug or hog
    println("Considering similarApps uuid=" + uuid)
    writeTripletUngrouped(similar, dissimilar, DynamoDbEncoder.put(similarsTable, similarKey, uuid, _, _, _, _, _, _),
      { DynamoDbDecoder.deleteItem(similarsTable, uuid) }, false)
  }

  /**
   * Get the distributions, xmax, ev's and ev distance of two collections of CaratRates.
   */
  def getDistanceAndDistributions(one: RDD[CaratRate], two: RDD[CaratRate]) = {
    // probability distribution: r, count/sumCount

    /* Figure out max x value (maximum rate) and bucket y values of 
     * both distributions into n buckets, averaging inside a bucket
     */

    /* FIXME: Should not flatten RDD's, but figure out how to transform an
     * RDD of Rates => RDD of UniformDists => RDD of Double,Double pairs (Bucketed values)  
     */
    val flatOne = one.map(x => {
      if (x.isRateRange())
        x.rateRange
      else
        new UniformDist(x.rate, x.rate)
    }).collect()
    val flatTwo = two.map(x => {
      if (x.isRateRange())
        x.rateRange
      else
        new UniformDist(x.rate, x.rate)
    }).collect()

    var evDistance = 0.0

    if (flatOne.size > 0 && flatTwo.size > 0) {
      println("rates=" + flatOne.size + " ratesNeg=" + flatTwo.size)
      if (flatOne.size < 10) {
        println("Less than 10 rates in \"with\": " + flatOne.mkString("\n"))
      }

      if (flatTwo.size < 10) {
        println("Less than 10 rates in \"without\": " + flatTwo.mkString("\n"))
      }

      if (DEBUG) {
        ProbUtil.debugNonZero(flatOne.map(_.getEv), flatTwo.map(_.getEv), "rates")
      }
      // Log bucketing:
      val (xmax, bucketed, bucketedNeg, ev, evNeg) = ProbUtil.logBucketDistributionsByX(flatOne, flatTwo, BUCKETS, SMALLEST_BUCKET, DECIMALS)

      evDistance =  DynamoAnalysisUtil.evDiff(ev, evNeg)
      printf("evWith=%s evWithout=%s evDistance=%s\n", ev, evNeg, evDistance)

      if (DEBUG) {
        ProbUtil.debugNonZero(bucketed.map(_._2), bucketedNeg.map(_._2), "bucket")
      }

      (xmax, bucketed, bucketedNeg, ev, evNeg, evDistance)
    } else
      (0.0, null, null, 0.0, 0.0, 0.0)
  }

  /**
   * Write the probability distributions, the distance, and the xmax value to DynamoDb. Ungrouped CaratRates variant.
   */
  def writeTripletUngrouped(one: RDD[CaratRate], two: RDD[CaratRate], putFunction: (Double, Seq[(Int, Double)], Seq[(Int, Double)], Double, Double, Double) => Unit,
    deleteFunction: => Unit, isBugOrHog: Boolean) = {
    val (xmax, bucketed, bucketedNeg, ev, evNeg, evDistance) = getDistanceAndDistributions(one, two)
    if (bucketed != null && bucketedNeg != null) {
      if (evDistance > 0 || !isBugOrHog) {
        putFunction(xmax, bucketed.toArray[(Int, Double)], bucketedNeg.toArray[(Int, Double)], evDistance, ev, evNeg)
      } else if (evDistance <= 0 && isBugOrHog) {
        /* We should probably remove it in this case. */
        deleteFunction
      }
    } else
      deleteFunction
    isBugOrHog && evDistance > 0
  }

  /**
   * The J-Score is the % of people with worse = higher energy use.
   * therefore, it is the size of the set of evDistances that are higher than mine,
   * compared to the size of the user base.
   * Note that the server side multiplies the JScore by 100, and we store it here
   * as a fraction.
   */
  def writeJScores(distsWithUuid: TreeMap[String, TreeMap[Int, Double]],
    distsWithoutUuid: TreeMap[String, TreeMap[Int, Double]],
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
        DynamoDbEncoder.put(resultsTable, resultKey, k, xmax, distWith.toArray[(Int, Double)], distWithout.toArray[(Int, Double)], jscore, ev, evNeg, apps.toSeq)
      else
        printf("Error: Could not save jscore, because: distWith=%s distWithout=%s apps=%s\n", distWith, distWithout, apps)
    }
    //DynamoDbEncoder.put(xmax, bucketed.toArray[(Int, Double)], bucketedNeg.toArray[(Int, Double)], jScore, ev, evNeg)
  }
}
