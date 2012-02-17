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

object CaratDynamoDataAnalysis {
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

    val sc = new SparkContext(master, "CaratDynamoDataAnalysis")
    analyzeData(sc)
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

    // Master RDD for all data.
    var allRates: spark.RDD[CaratRate] = null

    allRates = DynamoDbItemLoop(DynamoDbDecoder.getAllItems(registrationTable),
      DynamoDbDecoder.getAllItems(registrationTable, _),
      handleRegs(sc, _, _, allUuids, allOses, allModels), false, allRates)

    println("All uuIds: " + allUuids.mkString(", "))
    println("All oses: " + allOses.mkString(", "))
    println("All models: " + allModels.mkString(", "))

    if (allRates != null) {
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

      distRet = DynamoDbItemLoop(DynamoDbDecoder.getItems(samplesTable, uuid),
        DynamoDbDecoder.getItems(samplesTable, uuid, _),
        handleSamples(sc, _, os, model, _),
        true,
        distRet)
    }
    distRet
  }

  /**
   * Generic Carat DynamoDb loop function. Gets items from a table using keys given, and continues until the table scan is complete.
   * This function achieves a block by block read until the end of a table, regardless of throughput or manual limits.
   */
  def DynamoDbItemLoop(tableAndValueToKeyAndResults: => (com.amazonaws.services.dynamodb.model.Key, java.util.List[java.util.Map[String, AttributeValue]]),
    tableAndValueToKeyAndResultsContinue: com.amazonaws.services.dynamodb.model.Key => (com.amazonaws.services.dynamodb.model.Key, java.util.List[java.util.Map[String, AttributeValue]]),
    stepHandler: (java.util.List[java.util.Map[String, AttributeValue]], spark.RDD[edu.berkeley.cs.amplab.carat.CaratRate]) => spark.RDD[edu.berkeley.cs.amplab.carat.CaratRate],
    prefix:Boolean,/*prefixer: (java.util.List[java.util.Map[String, AttributeValue]]) => java.util.List[java.util.Map[String, AttributeValue]],*/
    dist: spark.RDD[edu.berkeley.cs.amplab.carat.CaratRate]) = {
    var finished = false

    var (key, results) = tableAndValueToKeyAndResults
    println("Got: " + results.size + " results.")

    var distRet: spark.RDD[CaratRate] = null
    distRet = stepHandler(results, dist)

    while (key != null) {

      println("Continuing from key=" + key)
      var (key2, results2) = tableAndValueToKeyAndResultsContinue(key)
      /* Re-use last zero-drain samples here, if any.
       * Not used now; taking the final sample is enough. */
      /*if (prefixer != null)
        results2.prepend(... ++= prefixer(results))*/
      results2.prepend(results.last)
      results = results2
      key = key2
      println("Got: " + results.size + " results.")

      distRet = stepHandler(results, distRet)
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
      rateMapperPairwise(os, model, mapped)
    })
    if (rates != null)
      rateRdd = rateRdd.union(rates)
    rateRdd
  }

  /**
   * Map samples into CaratRates. `os` and `model` are inserted for easier later processing.
   * Consider sample pairs with non-blc endpoints rates from 0 to prevBatt - batt with uniform probability.
   */
  def rateMapperPairwise(os: String, model: String, observations: Seq[(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, Seq[String])]) = {
    val charge = "charging"
    val discharge = "unplugged"
    val blc = "batterylevelchanged"
    // Observations format: (uuid, time, batteryLevel, event, batteryState, apps)
    var prevD = 0.0
    var prevBatt = 0.0
    var prevEvent = ""
    var prevState = ""
    var prevApps: Seq[String] = Array[String]()

    var d = 0.0
    var batt = 0.0
    var event = ""
    var state = ""
    var apps: Seq[String] = Array[String]()

    var negDrainSamples = 0
    var abandonedSamples = 0
    var chargingSamples = 0
    var zeroBLCSamples = 0
    var allZeroSamples = 0

    var rates = new ArrayBuffer[CaratRate]

    for (k <- observations) {
      d = k._2.toDouble
      batt = k._3.toDouble
      event = k._4.trim().toLowerCase()
      state = k._5.trim().toLowerCase()
      apps = k._6

      if (state != charge) {
        /* Record rates. First time fall through */
        if (prevD != 0 && prevD != d) {
          if (prevBatt - batt < 0) {
            printf("prevBatt %s batt %s for d1=%s d2=%s uuid=%s\n", prevBatt, batt, prevD, d, k._1)
            negDrainSamples += 1
          } else if (prevBatt == 0 && batt == 0){
            /* Assume simulator, ignore */
            printf("prevBatt %s batt %s for d1=%s d2=%s uuid=%s\n", prevBatt, batt, prevD, d, k._1)
            allZeroSamples += 1
          }else {
            /* now prevBatt - batt >= 0 */
            if (prevEvent == blc && event == blc) {
              /* Point rate */
              val r = new CaratRate(k._1, os, model, prevD, d, prevBatt, batt,
                prevEvent, event, prevApps, apps)
              if (r.rate() == 0) {
                // This should never happen
                println("RATE ERROR: BatteryLevelChanged with zero rate: " + r.toString(false))
                zeroBLCSamples += 1
              } else {
                if (considerRate(r)) {
                  rates += r
                } else {
                  abandonedSamples += 1
                }
              }
            }else{
              /* One endpoint not BLC, use uniform distribution rate */
              val r = new CaratRate(k._1, os, model, prevD, d, prevBatt, batt, new UniformDist(prevBatt, batt, prevD, d),
                prevEvent, event, prevApps, apps)
               if (considerRate(r)) {
                rates += r
              } else {
                 println("Abandoned uniform rate with abnormally high EV: "+ r.toString(false))
                abandonedSamples += 1
              }
            }
          }
        }
      } else {
        chargingSamples += 1
      }
      prevD = d
      prevBatt = batt
      prevEvent = event
      prevState = state
      prevApps = apps
    }

    printf("Abandoned %s all zero, %s charging, %s negative drain, %s > %s drain, %s zero drain BLC samples.\n", allZeroSamples, chargingSamples, negDrainSamples, abandonedSamples, ABNORMAL_RATE, zeroBLCSamples)
    rates.toSeq
  }

  /**
   * Check rate for abnormally high drain in a short time. Return true if the rate is not abnormally high.
   */
  def considerRate(r: CaratRate, oldObs: ArrayBuffer[(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, Seq[String])]) = {
    if (r.rate() > ABNORMAL_RATE) {
      printf("Abandoning abnormally high rate " + r)
      println("All observations included for the abnormally high rate: " + "(" + oldObs.size + ")")
      for (j <- oldObs) {
        printf("uuid=%s time=%s batt=%s event=%s trigger=%s\n", j._1, j._2, j._3, j._4, j._5)
      }
      false
    } else
      true
  }

  def considerRate(r: CaratRate) = {
    if (r.isUniform()){
      if (r.rateRange.getEv() > ABNORMAL_RATE) {
        false
      }else
        true
    }else{
    if (r.rate() > ABNORMAL_RATE) {
      printf("Abandoning abnormally high rate " + r.toString(false))
      false
    } else
      true
    }
  }

  /**
   * Create a probability density function out of a set of CaratRates.
   */
  def prob(rates: Array[CaratRate]) = {
    var sum = 0.0
    var buf = new TreeMap[Double, Double]
    for (d <- rates) {
      if (d.isUniform()) {
        val disc = d.rateRange.discretize(DECIMALS)
        for (k <- disc) {
          var count = buf.get(k).getOrElse(0.0) + 1.0 / disc.size
          buf += ((k, count))
        }
      } else {
        var count = buf.get(d.rate).getOrElse(0.0) + 1.0
        buf += ((d.rate, count))
      }
      // sum increases by one in either case.
      sum += 1
    }

    for (k <- buf)
      buf += ((k._1, k._2 / sum))
    buf
  }

  /**
   * Create a probability density function out of Doubles.
   */
  def prob(rates: Array[Double]) = {
    var sum = 0.0
    var buf = new TreeMap[Double, Double]
    for (d <- rates) {
      var count = buf.get(d).getOrElse(0.0) + 1.0
      buf += ((d, count))
      // sum increases by one in either case.
      sum += 1
    }

    for (k <- buf)
      buf += ((k._1, k._2 / sum))
    buf
  }
  
  /**
   * Create a probability density function out of UniformDistances.
   */
  def probUniform(rates: Array[UniformDist]) = {
    var min = 200.0
    var max = 0.0
    var buf = new TreeMap[Double, Double]
    /* Find min and max x*/
    for (d <- rates) {
      if (d.from < min)
        min = d.from
      if (d.to > max)
        max = d.to
    }
    
    /* Iterate from max to min in 3 decimal precision */
    
    val f = ProbUtil.nInt(min, DECIMALS)
    val to = ProbUtil.nInt(max, DECIMALS)+1
    
    var mul = 1.0
    for (k <- 0 until DECIMALS)
      mul *= 10
    
    var bigtotal = 0.0
    for (k <- f until to){
      val kreal = k/mul
      /* Get rates that contain the 3 decimal accurate value of k,
       * take their uniform probability, sum all of it up,
       * and place it in the k'th bucket in the TreeMap.*/
      val count = rates.filter(x => {
      !x.isPoint() && x.contains(kreal)}).map(_.prob()).sum
      bigtotal += count
      var prev = buf.get(kreal).getOrElse(0.0) + count
      buf += ((kreal, prev))
    }
    for (k <- buf)
      buf += ((k._1, k._2 / bigtotal))

    val pointRates = rates.filter(_.isPoint)
    
    var sum = 0.0
    var pointBuf = new TreeMap[Double, Double]
    for (k <- pointRates){
      val dec = ProbUtil.nDecimal(k.from, DECIMALS) 
      var prev = pointBuf.get(dec).getOrElse(0.0) + 1.0
      pointBuf += ((dec, prev))
      sum += 1
    }
    
    for (k <- pointBuf){
      val nk = ((k._1,  k._2 / sum))
      val prev = buf.get(k._1).getOrElse(0.0) + nk._2
      buf += ((k._1, prev))
    }
    
    buf
  }

  /**
   * Main analysis function. Called on the entire collected set of CaratRates.
   */
  def analyzeRateData(allRates: RDD[CaratRate],
    uuids: scala.collection.mutable.Set[String], oses: scala.collection.mutable.Set[String], models: scala.collection.mutable.Set[String]) {
    /* Daemon apps, hardcoded for now */
    var daemons: Set[String] = Set(
      "aggregated",
      "apsd",
      "BTServer",
      "Carat",
      "configd",
      "calaccessd",
      "dataaccessd",
      "fseventsd",
      "iapd",
      "imagent",
      "installd",
      "kernel_task",
      "launchd",
      "librariand",
      "locationd",
      "lockdownd",
      "lsd",
      "mDNSResponder",
      "mediaremoted",
      "mediaserverd",
      "MobileMail",
      "MobilePhone",
      "MobileSafari",
      "networkd",
      "notifyd",
      "pasteboardd",
      "powerd",
      "sandboxd",
      "securityd",
      "SpringBoard",
      "syslogd",
      "ubd",
      "UserEventAgent",
      "wifid",
      "WindowServer", "dynamic_pager", "logind", "fontd", 
      "warmd", "coreservicesd", "autofsd", "warmd_agent",
      "filecoordination", "mds", "hidd", "kextd", "diskarbitrationd",
      "mdworker")

    /**
     * uuid distributions, xmax, ev and evNeg
     */
    var distsWithUuid = new TreeMap[String, TreeMap[Int, Double]]
    var distsWithoutUuid = new TreeMap[String, TreeMap[Int, Double]]
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

    val apps = allRates.map(x => {
      var sampleApps = x.allApps
      sampleApps --= daemons
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
      println("Considering os os=" + os)
      writeTripletUngrouped(fromOs, notFromOs, DynamoDbEncoder.put(osTable, osKey, os, _, _, _, _, _, _),
          { println("ERROR: Delete called for a non-bug non-hog!") }, false)
    }

    for (model <- models) {
      val fromModel = allRates.filter(_.model == model)
      val notFromModel = allRates.filter(_.model != model)
      // no distance check, not bug or hog
      println("Considering model model=" + model)
      writeTripletUngrouped(fromModel, notFromModel, DynamoDbEncoder.put(modelsTable, modelKey, model, _, _, _, _, _, _),
          { println("ERROR: Delete called for a non-bug non-hog!") }, false)
    }

    var allHogs = new HashSet[String]
    /* Hogs: Consider all apps except daemons. */
    for (app <- allApps) {
      if (app != CARAT) {
        val filtered = allRates.filter(_.allApps.contains(app))
        val filteredNeg = allRates.filter(!_.allApps.contains(app))
        println("Considering hog app=" + app)
        if (writeTripletUngrouped(filtered, filteredNeg, DynamoDbEncoder.put(appsTable, appKey, app, _, _, _, _, _, _),
          DynamoDbDecoder.deleteItem(appsTable, app), true)) {
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
      sampleApps --= daemons
      sampleApps --= allHogs
      sampleApps
      }).collect()

      var uuidApps = new scala.collection.mutable.HashSet[String]

      // Get all apps ever reported, also compute likely daemons
      for (k <- tempApps) {
        uuidApps ++= k
        if (intersectPerSampleApps.size == 0)
          intersectPerSampleApps ++= k
        else if (k.size > 0)
          intersectPerSampleApps = intersectPerSampleApps.intersect(k)
      }

      //Another method to find likely daemons
      if (intersectEverReportedApps.size == 0)
        intersectEverReportedApps = uuidApps
      else if (uuidApps.size > 0)
        intersectEverReportedApps = intersectEverReportedApps.intersect(uuidApps)

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
      for (app <- uuidApps) {
        if (app != CARAT) {
          val appFromUuid = fromUuid.filter(_.allApps.contains(app))
          val appNotFromUuid = notFromUuid.filter(_.allApps.contains(app))
          println("Considering bug app=" + app + " uuid=" + uuid)
          writeTripletUngrouped(appFromUuid, appNotFromUuid, DynamoDbEncoder.putBug(bugsTable, (resultKey, appKey), (uuid, app), _, _, _, _, _, _),
            DynamoDbDecoder.deleteItem(bugsTable, uuid, app), true)
        }
      }
    }
    
    writeJScores(distsWithUuid, distsWithoutUuid, parametersByUuid, evDistanceByUuid, appsByUuid)
    
    val removed = daemons -- intersectEverReportedApps
    val removedPS = daemons -- intersectPerSampleApps
    intersectEverReportedApps --= daemons
    intersectPerSampleApps --= daemons
    println("Daemons: " + daemons)
    if (intersectEverReportedApps.size > 0)
      println("New possible daemons (ever reported): " + intersectEverReportedApps)
    if (intersectPerSampleApps.size > 0)
      println("New possible daemons (per sample): " + intersectPerSampleApps)
    if (removed.size > 0)
      println("Removed daemons (ever reported): " + removed)
    if (removedPS.size > 0)
      println("Removed daemons (per sample): " + removedPS)
  }

  /**
   * Calculate similar apps for device `uuid` based on all rate measurements and apps reported on the device.
   * Write them to DynamoDb.
   */
  def similarApps(all: RDD[CaratRate], uuid: String, uuidApps: scala.collection.mutable.Set[String]) {
    val sCount = similarityCount(uuidApps.size)
    printf("SimilarApps uuid=%s sCount=%s uuidApps.size=%s\n", uuid, sCount, uuidApps.size)
    val similar = all.filter(_.allApps.intersect(uuidApps).size >= sCount)
    val dissimilar = all.filter(_.allApps.intersect(uuidApps).size < sCount)
    //printf("SimilarApps similar.count=%s dissimilar.count=%s\n",similar.count(), dissimilar.count())
    // no distance check, not bug or hog
    println("Considering similarApps uuid=" + uuid)
    writeTripletUngrouped(similar, dissimilar, DynamoDbEncoder.put(similarsTable, similarKey, uuid, _, _, _, _, _, _), 
        { println("ERROR: Delete called for a non-bug non-hog!") }, false)
  }

  def getDistanceAndDistributions(one: RDD[CaratRate], two: RDD[CaratRate]) = {
      // probability distribution: r, count/sumCount

    /* Figure out max x value (maximum rate) and bucket y values of 
     * both distributions into n buckets, averaging inside a bucket
     */
    
    val flatOne = one.map(x => {
      if (x.isUniform())
        x.rateRange
        else
          new UniformDist(x.rate, x.rate)
    }).collect()
    val flatTwo = two.map(x => {
      if (x.isUniform())
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
    
      val (xmax, bucketed, bucketedNeg, ev, evNeg) = ProbUtil.bucketDistributionsByX(flatOne, flatTwo, buckets, DECIMALS)

      evDistance = evDiff(ev, evNeg)
      printf("evWith=%s evWithout=%s evDistance=%s\n", ev, evNeg, evDistance)

      if (DEBUG) {
        ProbUtil.debugNonZero(bucketed.map(_._2), bucketedNeg.map(_._2), "bucket")
      }
      
      (xmax, bucketed, bucketedNeg, ev, evNeg, evDistance)
    }else
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
  def writeJScores(distsWithUuid: TreeMap[String, TreeMap[Int, Double]],
    distsWithoutUuid: TreeMap[String, TreeMap[Int, Double]],
    parametersByUuid: TreeMap[String, (Double, Double, Double)],
    evDistanceByUuid: TreeMap[String, Double],
    appsByUuid: TreeMap[String, scala.collection.mutable.HashSet[String]]) {
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
        DynamoDbEncoder.put(resultsTable, resultKey, k, xmax, distWith.toArray[(Int, Double)], distWithout.toArray[(Int, Double)], jscore, ev, evNeg, apps.toSeq)
      else
        printf("Error: Could not save jscore, because: distWith=%s distWithout=%s apps=%s\n", distWith, distWithout, apps)
    }
    //DynamoDbEncoder.put(xmax, bucketed.toArray[(Int, Double)], bucketedNeg.toArray[(Int, Double)], jScore, ev, evNeg)
  }
  
  /**
   * New metric: Use EV difference for hog and bug decisions.
   * TODO: Variance and its use in the decision?
   * Multimodal distributions -> EV does not match true energy usage profile?
   * m = 1 - evWith / evWithout
   *
   * m > 0 -> Hog
   * m <= 0 -> not
   */
  def evDiff(evWith: Double, evWithout: Double) = {
    1.0 - evWithout / evWith
  }
}
