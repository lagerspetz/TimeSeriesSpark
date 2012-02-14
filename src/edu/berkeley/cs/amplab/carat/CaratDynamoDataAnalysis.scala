package edu.berkeley.cs.amplab.carat

import spark._
import spark.SparkContext._
import spark.timeseries._
import scala.collection.mutable.ArrayBuffer
import scala.collection.Seq
import scala.collection.mutable.Set
import scala.collection.mutable.HashSet
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
    val allUuids = new HashSet[String]
    val allModels = new HashSet[String]
    val allOses = new HashSet[String]

    // Master RDD for all data.
    var allRates: spark.RDD[CaratRate] = null

    allRates = DynamoDbItemLoop(DynamoDbDecoder.getAllItems(registrationTable),
      DynamoDbDecoder.getAllItems(registrationTable, _),
      handleRegs(sc, _, _, allUuids, allOses, allModels), null, allRates)

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
  def handleRegs(sc: SparkContext, regs: java.util.List[java.util.Map[String, AttributeValue]], dist: spark.RDD[CaratRate], uuids: Set[String], oses: Set[String], models: Set[String]) = {
    /* FIXME: I would like to do this in parallel, but that would not let me re-use
     * all the data for the other uuids, resulting in n^2 execution time.
     */

    // Remove duplicates caused by re-registrations:
    val regSet: Set[(String, String, String)] = new HashSet[(String, String, String)]
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
        lastZeroSamplesPrefixer,
        distRet)
    }
    distRet
  }

  /**
   * Take a set of samples from Dynamo and return the last that have the same battery level.
   * The returned samples are prepended to the next batch retrieved from Dynamo to even out
   * high battery drain rates.
   *
   * @return the last samples that have the same battery level.
   */
  def lastZeroSamplesPrefixer(list: java.util.List[java.util.Map[String, AttributeValue]]) = {
    var res: java.util.List[java.util.Map[String, AttributeValue]] =
      new java.util.ArrayList[java.util.Map[String, AttributeValue]]
    println("Prefixer started, last.batteryLevel = " + list.last.get(sampleBatteryLevel))

    var batteryLevel = { val attr = list.last.get(sampleBatteryLevel); if (attr != null) attr.getN() else "" }
    if (batteryLevel != "") {
      val batt = batteryLevel.toDouble
      println("Last battery = " + batt)
      var count = 0
      var streak = true
      for (k <- list.reverse) {
        batteryLevel = { val attr = k.get(sampleBatteryLevel); if (attr != null) attr.getN() else "" }
        if (batteryLevel != "" && streak) {
          var nBatt = batteryLevel.toDouble
          println("nBatt = " + nBatt)
          if (nBatt == batt)
            count += 1
          else
            streak = false
        }
      }
      if (DEBUG && count > 0)
        println("Found a prefix of " + count + " zero drain samples for the next batch.")
      if (count > 0)
        res ++= list.subList(list.length - count, list.length)
    }
    res
  }

  /**
   * Generic Carat DynamoDb loop function. Gets items from a table using keys given, and continues until the table scan is complete.
   * This function achieves a block by block read until the end of a table, regardless of throughput or manual limits.
   */
  def DynamoDbItemLoop(tableAndValueToKeyAndResults: => (com.amazonaws.services.dynamodb.model.Key, java.util.List[java.util.Map[String, AttributeValue]]),
    tableAndValueToKeyAndResultsContinue: com.amazonaws.services.dynamodb.model.Key => (com.amazonaws.services.dynamodb.model.Key, java.util.List[java.util.Map[String, AttributeValue]]),
    stepHandler: (java.util.List[java.util.Map[String, AttributeValue]], spark.RDD[edu.berkeley.cs.amplab.carat.CaratRate]) => spark.RDD[edu.berkeley.cs.amplab.carat.CaratRate],
    prefixer: (java.util.List[java.util.Map[String, AttributeValue]]) => java.util.List[java.util.Map[String, AttributeValue]],
    dist: spark.RDD[edu.berkeley.cs.amplab.carat.CaratRate]) = {
    var finished = false

    var (key, results) = tableAndValueToKeyAndResults
    println("Got: " + results.size + " results.")

    var distRet: spark.RDD[CaratRate] = null
    distRet = stepHandler(results, dist)

    while (key != null) {

      println("Continuing from key=" + key)
      var (key2, results2) = tableAndValueToKeyAndResultsContinue(key)
      /* Re-use last zero-drain samples here, if any. */
      if (prefixer != null)
        results2 ++= prefixer(results)
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
              s(1)
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

    var rates = new ArrayBuffer[CaratRate]

    for (k <- observations) {
      d = k._2.toDouble
      batt = k._3.toDouble
      event = k._4.trim().toLowerCase()
      state = k._5.trim().toLowerCase()
      apps = k._6

      if (state != charge) {
        /* Record rates. First time fall through */
        if (prevD != 0) {
          if (prevBatt - batt < 0) {
            printf("prevBatt %s batt %s for observation %s\n", prevBatt, batt, k)
            negDrainSamples += 1
          } else {
            /* now prevBatt - batt >= 0 */
            if (prevEvent == blc && event == blc) {
              /* Point rate */
              val r = new CaratRate(k._1, os, model, prevD, d, prevBatt, batt,
                prevEvent, event, prevApps, apps)
               if (r.rate() == 0){
                // This should never happen
                println("RATE ERROR: BatteryLevelChanged with zero rate: "+ r)
              }
              if (considerRate(r)) {
                rates += r
              } else {
                abandonedSamples += 1
              }
            }else{
              /* One endpoint not BLC, use uniform distribution rate */
              val r = new CaratRate(k._1, os, model, prevD, d, prevBatt, batt, new UniformDist(prevBatt, batt, prevD, d),
                prevEvent, event, prevApps, apps)
               if (considerRate(r)) {
                rates += r
              } else {
                 println("Abandoned uniform rate with abnormally high EV: "+ r)
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

    printf("Abandoned %s charging samples, %s negative drain samples, and %s > %s drain samples\n", chargingSamples, negDrainSamples, abandonedSamples, ABNORMAL_RATE)
    rates.toSeq
  }

  /**
   * Map samples into CaratRates. `os` and `model` are inserted for easier later processing.
   */
  def rateMapper(os: String, model: String, observations: Seq[(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, Seq[String])]) = {
    // (uuid, time, batteryLevel, event, batteryState, apps)
    var prevD = 0.0
    var prevBatt = 0.0
    var prevEvents = new HashSet[String]()
    var prevApps = new HashSet[String]()

    val charge = "charging"
    val discharge = "unplugged"

    var rates = new ArrayBuffer[CaratRate]

    var d = 0.0
    var events = ArrayBuffer[String]()
    var apps: Seq[String] = Array[String]()
    var batt = 0.0
    var unplugged = false

    var negDrainSamples = 0
    var abandonedSamples = 0
    var chargingSamples = 0

    /**
     * If a sample is taken as a result of a batteryStatusChange, then we can
     * consider the battery reading to be exactly at the measured value (say,
     * 90%). For any other trigger, however, the sample is actually implying
     * a 5% range of values, each equally likely, a priori. Rather than
     * treating these rates, therefore, as point value guesses dropped into
     * the appropriate buckets, we should treat them as continuous
     * probability distributions.
     *
     * For example, say we have two consecutive samples:
     * 90% batteryStateChanged
     * 85% locationChange
     * The first value is exact. The second value is actually a uniform
     * distribution between [80, 85]. (Actually, I've forgotten whether it
     * rounds up or down.) Anyway, that means the battery change is also
     * uniformly distributed [5, 10].
     *
     * You have these distributions even when two consecutive samples have
     * the same battery reading:
     * t_1 85% locationChange
     * t_2 85% locationChange
     * The rate here == [0, 5] / (t_2 - t_1)
     *
     * These only collapse to point values when both ends are
     * batteryStatusChange triggers:
     * 90% batteryStatusChange
     * 85% batteryStatusChange
     * We can compute an exact rate, which is to say a distribution that is a
     * Dirac delta function at exactly this value.
     *
     * By the way, are we computing these rates based on the timestamps of
     * the event causing the trigger, rather than the trigger delivery? The
     * delivery may be delayed, so we should be doing the former.
     */

    var oldObs = new ArrayBuffer[(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, Seq[String])]
    for (k <- observations) {
      d = k._2.toDouble
      batt = k._3.toDouble
      events = new ArrayBuffer[String]
      events ++= k._4.trim().toLowerCase().split(" ")
      events += k._5.trim().toLowerCase()
      apps = k._6
      if (DEBUG)
        printf("batt=%f prevBatt=%f drain=%f events=%s apps=%s\n", batt, prevBatt, prevBatt - batt, events, apps)

      if (events.contains(discharge)) {
        unplugged = true
      } else if (events.contains(charge)) {
        unplugged = false
      }

      /* Ignore measurements until unplugged event
       * and record until pluggedIn */
      if (unplugged) {
        oldObs += k
        // First time set start date and starting battery
        if (prevD == 0) {
          prevD = d
          prevBatt = batt
        }

        if (DEBUG)
          printf("unplugged batt=%f prevBatt=%f drain=%f events=%s apps=%s\n", batt, prevBatt, prevBatt - batt, events, apps)
        // take periods where battery life has changed
        if (batt - prevBatt > 0 || prevBatt - batt > 0) {
          if (prevBatt - batt < 0) {
            printf("prevBatt %s batt %s for observation %s\n", prevBatt, batt, k)
            negDrainSamples += oldObs.size
          } else {
            if (events.last.equalsIgnoreCase("batteryStateChanged")) {
              val r = new CaratRate(k._1, os, model, prevD, d, prevBatt, batt,
                prevEvents.toArray, events.toArray, prevApps.toArray, apps)
              if (considerRate(r, oldObs))
                rates += r
              else
                abandonedSamples += oldObs.size
            } else {
              /* Consider sample a range of rates */
              /* Normally, each rate r will get a count of 1. This should get a count of 0.2,
               * since it is a range of drain between 1 to 5 %.
               */

            }

          }
          oldObs = new ArrayBuffer[(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, Seq[String])]
          // Even if period was ignored, set new start date and starting battery
          prevD = d
          prevBatt = batt
          // Reset, current apps and events added below
          prevEvents = new HashSet[String]()
          prevApps = new HashSet[String]()
        }
        /* FIXME: Last one is not "last", since DynamoDb gives out items in batches.
         * However, this may lead to some higher drain rates, or apps or events being ignored.
         * for example, when we have an interval at the end of batch:
         * battery 0.95 0.95, apps Carat, AngryBirds, Mail, Safari
         * And in the beginning of the next batch:
         * battery 0.95 0.90, apps Mail, Safari
         * The drop of 5% will register for the time spent in the new batch only,
         * causing a faster drain rate to show up. 
         */
        if (k == observations.last) {
          if (prevD != d) {
            if (prevBatt - batt >= 0) {
              val r = new CaratRate(k._1, os, model, prevD, d, prevBatt, batt,
                prevEvents.toArray, events.toArray, prevApps.toArray, apps)
              if (considerRate(r, oldObs))
                rates += r
              else
                abandonedSamples += oldObs.size
              if (r.rate() == 0) {
                printf("Zero rate %f for time1=%f time2=%f batt=%f prevBatt=%f drain=%f events=%s apps=%s\n", r.rate(), prevD, d, batt, prevBatt, prevBatt - batt, events, apps)
                println("All observations included for the zero rate: " + "(" + oldObs.size + ")")
                for (j <- oldObs) {
                  printf("uuid=%s time=%s batt=%s event=%s apps=%s\n", j._1, j._2, j._3, j._4, j._5, j._6.mkString(", "))
                }
              }
            } else {
              printf("[last] prevBatt %s batt %s for observation %s\n", prevBatt, batt, k)
              negDrainSamples += oldObs.size
            }
            oldObs = new ArrayBuffer[(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, Seq[String])]
          }
        }
        prevApps ++= apps
        prevEvents ++= events
      } else
        chargingSamples += 1
    }
    printf("Abandoned %s charging samples, %s negative drain samples, and %s > %s drain samples\n", chargingSamples, negDrainSamples, abandonedSamples, ABNORMAL_RATE)
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
      printf("Abandoning abnormally high rate " + r)
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
   * Create a probability density function out of a set of CaratRates.
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
  
  def probUniform(rates: Array[UniformDist]) = {
    var sum = 0.0
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
    
    val f = nInt(min)
    val to = nInt(max)+1
    
    var mul = 1.0
    for (k <- 0 until DECIMALS)
      mul *= 10
      
    for (k <- f until to){
      val kreal = k/mul
      /* Get rates that contain the 3 decimal accurate value of k,
       * take their uniform probability, sum all of it up,
       * and place it in the k'th bucket in the TreeMap.*/
      val count = rates.filter(_.contains(kreal)).map(_.prob()).sum
      var prev = buf.get(kreal).getOrElse(0.0) + count
      buf += ((kreal, prev))
    }

    for (k <- buf)
      buf += ((k._1, k._2 / rates.size))
    buf
  }

  /**
   * Main analysis function. Called on the entire collected set of CaratRates.
   */
  def analyzeRateData(allRates: RDD[CaratRate],
    uuids: Set[String], oses: Set[String], models: Set[String]) {
    /* Daemon apps, hardcoded for now */
    var daemons: Set[String] = Set(
      "BTServer",
      "Carat",
      "MobileMail",
      "MobilePhone",
      "MobileSafari",
      "SpringBoard",
      "UserEventAgent",
      "aggregated",
      "apsd",
      "configd",
      "dataaccessd",
      "fseventsd",
      "iapd",
      "imagent",
      "installd",
      "kernel_task",
      "launchd",
      "locationd",
      "lockdownd",
      "lsd",
      "mDNSResponder",
      "mediaremoted",
      "mediaserverd",
      "networkd",
      "notifyd",
      "pasteboardd",
      "powerd",
      "sandboxd",
      "securityd",
      "syslogd",
      "ubd",
      "wifid")

    /*if (DEBUG) {
      val cc = allRates.collect()
      for (k <- cc)
        println(k)
    }*/

    val apps = allRates.map(_.getAllApps()).collect()

    var allApps = new HashSet[String]
    for (k <- apps)
      allApps ++= k
    allApps.removeAll(daemons)
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
        val filtered = allRates.filter(_.getAllApps().contains(app))
        val filteredNeg = allRates.filter(!_.getAllApps().contains(app))
        println("Considering hog app=" + app)
        if (writeTripletUngrouped(filtered, filteredNeg, DynamoDbEncoder.put(appsTable, appKey, app, _, _, _, _, _, _),
          DynamoDbDecoder.deleteItem(appsTable, app), true)) {
          // this is a hog
          allHogs += app
        }
      }
    }

    var intersectEverReportedApps = new HashSet[String]
    var intersectPerSampleApps = new HashSet[String]

    for (uuid <- uuids) {
      val fromUuid = allRates.filter(_.uuid == uuid)

      val tempApps = fromUuid.map(_.getAllApps()).collect()

      var uuidApps = new HashSet[String]
      var nonHogs = new HashSet[String]

      // Get all apps ever reported, also compute likely daemons
      for (k <- tempApps) {
        uuidApps ++= k
        if (intersectPerSampleApps.size == 0)
          intersectPerSampleApps = k
        else if (k.size > 0)
          intersectPerSampleApps = intersectPerSampleApps.intersect(k)
      }

      //Another method to find likely daemons
      if (intersectEverReportedApps.size == 0)
        intersectEverReportedApps = uuidApps
      else if (uuidApps.size > 0)
        intersectEverReportedApps = intersectEverReportedApps.intersect(uuidApps)

      nonHogs ++= uuidApps
      nonHogs.removeAll(allHogs)

      /* allApps has daemons removed, so this will result in daemons being removed from uuidApps. */
      uuidApps = uuidApps.intersect(allApps)
      if (uuidApps.size > 0)
        similarApps(allRates, uuid, uuidApps)
      //else
      // Remove similar apps entry?

      val notFromUuid = allRates.filter(_.uuid != uuid)
      // no distance check, not bug or hog
      println("Considering jscore uuid=" + uuid)
      writeTripletUngrouped(fromUuid, notFromUuid, DynamoDbEncoder.put(resultsTable, resultKey, uuid, _, _, _, _, _, _, uuidApps.toSeq),
          { println("ERROR: Delete called for a non-bug non-hog!") }, false)

      /* Bugs: Only consider apps reported from this uuId. Only consider apps not known to be hogs. */
      for (app <- nonHogs) {
        if (app != CARAT) {
          val appFromUuid = fromUuid.filter(_.getAllApps().contains(app))
          val appNotFromUuid = notFromUuid.filter(_.getAllApps().contains(app))
          println("Considering bug app=" + app + " uuid=" + uuid)
          writeTripletUngrouped(appFromUuid, appNotFromUuid, DynamoDbEncoder.putBug(bugsTable, (resultKey, appKey), (uuid, app), _, _, _, _, _, _),
            DynamoDbDecoder.deleteItem(bugsTable, uuid, app), true)
        }
      }
    }
    val removed = daemons.clone() -- intersectEverReportedApps
    val removedPS = daemons.clone() -- intersectPerSampleApps
    intersectEverReportedApps.removeAll(daemons)
    intersectPerSampleApps.removeAll(daemons)
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
  def similarApps(all: RDD[CaratRate], uuid: String, uuidApps: Set[String]) {
    val sCount = similarityCount(uuidApps.size)
    printf("SimilarApps uuid=%s sCount=%s uuidApps.size=%s\n", uuid, sCount, uuidApps.size)
    val similar = all.filter(_.getAllApps().intersect(uuidApps).size >= sCount)
    val dissimilar = all.filter(_.getAllApps().intersect(uuidApps).size < sCount)
    //printf("SimilarApps similar.count=%s dissimilar.count=%s\n",similar.count(), dissimilar.count())
    // no distance check, not bug or hog
    println("Considering similarApps uuid=" + uuid)
    writeTripletUngrouped(similar, dissimilar, DynamoDbEncoder.put(similarsTable, similarKey, uuid, _, _, _, _, _, _), 
        { println("ERROR: Delete called for a non-bug non-hog!") }, false)
  }

  /**
   * Write the probability distributions, the distance, and the xmax value to DynamoDb. Ungrouped CaratRates variant.
   */
  def writeTripletUngrouped(one: RDD[CaratRate], two: RDD[CaratRate], putFunction: (Double, Seq[(Int, Double)], Seq[(Int, Double)], Double, Double, Double) => Unit,
    deleteFunction: => Unit, isBugOrHog: Boolean) = {

    // probability distribution: r, count/sumCount

    /* Figure out max x value (maximum rate) and bucket y values of 
       * both distributions into n buckets, averaging inside a bucket
       */
    /*
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
    */
    
    // For now, to keep values stable in the db cheat with distributions:
     val flatOne = one.map(_.rate).collect()
    val flatTwo = two.map(_.rate).collect()
    
    if (DEBUG) {
      debugNonZero(flatOne, flatTwo, "rates")
    }

    var evDistance = 0.0

    println("rates=" + flatOne.size + " ratesNeg=" + flatTwo.size)
    if (flatOne.size > 0 && flatTwo.size > 0) {
      val values = prob(flatOne)
      val others = prob(flatTwo)

      if (DEBUG) {
        debugNonZero(values.map(_._2), others.map(_._2), "prob")
        plot(values, others)
      }else
        debugNonZero(values.map(_._2), others.map(_._2), "prob")

      val ev = getEv(values)
      val evNeg = getEv(others)

      evDistance = evDiff(ev, evNeg)
      /*if (DEBUG) {
        val distance = getDistanceNonCumulative(values, others)
        val dAbsSigned = getDistanceAbs(values, others)
        val dWeighted = getDistanceWeighted(values, others)
        val (iOne, iTwo) = getCumulativeIntegrals(values, others)

        printf("evDistance=%s distance=%s signed KS distance=%s X-weighted distance=%s Integrals=%s, %s, Integral difference(With-Without)=%s\n", evDistance, distance, dAbsSigned, dWeighted, iOne, iTwo, (iOne - iTwo))
      } else*/
        printf("evDistance=%s\n", evDistance)

      if (evDistance >= 0 || !isBugOrHog) {
        val (maxX, bucketed, bucketedNeg) = bucketDistributionsByX(values, others)
        if (DEBUG) {
          debugNonZero(bucketed.map(_._2), bucketedNeg.map(_._2), "bucket")
        }
        putFunction(maxX, bucketed.toArray[(Int, Double)], bucketedNeg.toArray[(Int, Double)], evDistance, ev, evNeg)
      } else if (evDistance < 0 && isBugOrHog) {
        /* We should probably remove it in this case. */
        deleteFunction
      }
    }
    isBugOrHog && evDistance > 0
  }
  
  /**
   *
   * New metric: Use EV difference for hog and bug decisions.
   * TODO: Variance and its use in the decision?
   * Multimodal distributions -> EV does not match true energy usage profile?
   * -> Use some variant of KS and EV difference ...
   *
   * E1 = without
   * E2 = with
   *
   * m = 1 - E1 / E2
   *
   * 1 - 0.5 / 0.7
   *
   * 1 - 0.5 / 0.3
   *
   *
   * m > 0 -> Hog
   * m <= 0 -> not
   *
   */
  def evDiff(evWith: Double, evWithout: Double) = {
    1.0 - evWithout / evWith
  }

  /**
   * Get the expected value of a probability distribution.
   * The EV is x*y / sum(y), where sum(y) is 1 for a probability distribution.
   */
  def getEv(values: TreeMap[Double, Double]) = {
    val m = values.map(x => {
      x._1 * x._2
    }).toSeq
    m.sum
  }

  /**
   * Debug: Print non-zero values of two sets.
   */
  def debugNonZero(one: Iterable[Double], two: Iterable[Double], kw1: String) {
    debugNonZeroOne(one, kw1)
    debugNonZeroOne(two, kw1 + "Neg")
  }

  def debugNonZeroUniform(one: Iterable[UniformDist], two: Iterable[UniformDist], kw1: String) {
    if (DEBUG) {
      println("Nonzero " + kw1 + ": " + one.mkString(" "))
    }

    if (DEBUG) {
      println("Nonzero " + kw1 + "Neg: " + two.mkString(" "))
    }
  }

    /**
   * Debug: Print non-zero values of a set.
   */
  def debugNonZeroOne(one: Iterable[Double], kw: String) {
    if (DEBUG) {
      val nz = one.filter(_ > 0)
      println("Nonzero " + kw + ": " + nz.mkString(" ") + " sum=" + nz.sum)
    }
  }

  /**
   * Debug: Utility function that helps plotting distributions by outputting them one pair per line.
   */
  def plot(values: TreeMap[Double, Double], others: TreeMap[Double, Double]) {
    println("prob")
    for (k <- values)
      println(k._1 + " " + k._2)
    println("probNeg")
    for (k <- others)
      println(k._1 + " " + k._2)
  }

  /**
   * Bucket given distributions into `buckets` buckets, and return the maximum x value and the bucketed distributions.
   */
  def bucketDistributionsByX(values: TreeMap[Double, Double], others: TreeMap[Double, Double]) = {
    var bucketed = new TreeMap[Int, Double]
    var bucketedNeg = new TreeMap[Int, Double]

    val xmax = math.max(values.last._1, others.last._1)

    for (k <- values) {
      val x = k._1 / xmax
      var bucket = (x * buckets).toInt
      if (bucket >= buckets)
        bucket = buckets - 1
      var old = bucketed.get(bucket).getOrElse(0.0)
      bucketed += ((bucket, nDecimal(old + k._2)))
    }

    for (k <- others) {
      val x = k._1 / xmax
      var bucket = (x * buckets).toInt
      if (bucket >= buckets)
        bucket = buckets - 1
      var old = bucketedNeg.get(bucket).getOrElse(0.0)
      bucketedNeg += ((bucket, nDecimal(old + k._2)))
    }

    for (k <- 0 until buckets) {
      if (!bucketed.contains(k))
        bucketed += ((k, 0.0))
      if (!bucketedNeg.contains(k))
        bucketedNeg += ((k, 0.0))
    }

    (xmax, bucketed, bucketedNeg)
  }

  /**
   * Get our own variant of the KS distance,
   * where negative values are ignored, from a regular, non-cumulative distribution.
   * The cumulative distribution values are constructed on the fly and discarded afterwards.
   */
  def getDistanceNonCumulative(one: TreeMap[Double, Double], two: TreeMap[Double, Double]) = {
    // Definitions:
    // result will be here
    var maxDistance = -2.0
    // represents previous value of distribution with a smaller starting value
    var prevTwo = (-2.0, 0.0)
    // represents next value of distribution with a smaller starting value
    var nextTwo = prevTwo
    // Guess which distribution has a smaller starting value
    var smaller = one
    var bigger = two

    /* Swap if the above assignment was not the right guess: */
    if (one.size > 0 && two.size > 0) {
      if (one.head._1 > two.head._1) {
        smaller = two
        bigger = one
      }
    }

    // use these to keep the cumulative distribution current value
    var sumOne = 0.0
    var sumTwo = 0.0

    //println("one.size=" + one.size + " two.size=" + two.size)

    // advance the smaller dist manually
    var smallIter = smaller.iterator
    // and the bigger automatically
    for (k <- bigger) {
      // current value of bigger dist
      sumOne += k._2

      // advance smaller past bigger, keep prev and next
      // from either side of the current value of bigger
      while (smallIter.hasNext && nextTwo._1 <= k._1) {
        var temp = smallIter.next
        sumTwo += temp._2

        // assign cumulative dist value
        nextTwo = (temp._1, sumTwo)
        //println("nextTwo._1=" + nextTwo._1 + " k._1=" + k._1)
        if (nextTwo._1 <= k._1) {
          prevTwo = nextTwo
        }
      }

      /* now nextTwo >= k > prevTwo */

      /* (NoApp - App) gives a high positive number
         * if the app uses a more energy. This is because
         * if the app distribution is shifted to the right,
         * it has a high probability of running at a high drain rate,
         * and so its cumulative dist value is lower, and NoApp
         * has a higher value. Inverse for low energy usage. */
      val distance = {
        if (smaller != two)
          sumOne - prevTwo._2
        else
          prevTwo._2 - sumOne
      }
      if (distance > maxDistance)
        maxDistance = distance
    }
    maxDistance
  }

  /**
   * Get a signed distance variant of the KS metric from a regular, non-cumulative distribution.
   * The cumulative distribution values are constructed on the fly and discarded afterwards.
   */
  def getDistanceAbs(one: TreeMap[Double, Double], two: TreeMap[Double, Double]) = {
    // Definitions:
    // result will be here
    var maxDistance = 0.0
    // represents previous value of distribution with a smaller starting value
    var prevTwo = (0.0, 0.0)
    // represents next value of distribution with a smaller starting value
    var nextTwo = prevTwo
    // Guess which distribution has a smaller starting value
    var smaller = one
    var bigger = two

    /* Swap if the above assignment was not the right guess: */
    if (one.size > 0 && two.size > 0) {
      if (one.head._1 > two.head._1) {
        smaller = two
        bigger = one
      }
    }

    // use these to keep the cumulative distribution current value
    var sumOne = 0.0
    var sumTwo = 0.0

    //println("one.size=" + one.size + " two.size=" + two.size)

    // advance the smaller dist manually
    var smallIter = smaller.iterator
    // and the bigger automatically
    for (k <- bigger) {
      // current value of bigger dist
      sumOne += k._2

      // advance smaller past bigger, keep prev and next
      // from either side of the current value of bigger
      while (smallIter.hasNext && nextTwo._1 <= k._1) {
        var temp = smallIter.next
        sumTwo += temp._2

        // assign cumulative dist value
        nextTwo = (temp._1, sumTwo)
        //println("nextTwo._1=" + nextTwo._1 + " k._1=" + k._1)
        if (nextTwo._1 <= k._1) {
          prevTwo = nextTwo
        }
      }

      /* now nextTwo >= k > prevTwo */

      /* (NoApp - App) gives a high positive number
         * if the app uses a more energy. This is because
         * if the app distribution is shifted to the right,
         * it has a high probability of running at a high drain rate,
         * and so its cumulative dist value is lower, and NoApp
         * has a higher value. Inverse for low energy usage. */
      val distance = {
        if (smaller != two)
          sumOne - prevTwo._2
        else
          prevTwo._2 - sumOne
      }
      /* Absolute value comparison, but signed assignment:
       * Calculates the greatest distance point, no matter if
       * negative or positive, but keeps in mind the sign for
       * the final result.
       */
      if (math.abs(distance) > math.abs(maxDistance))
        maxDistance = distance
    }
    maxDistance
  }

  /**
   * Get the weighted distance: average of x values times the distance, from a non-cumulative distribution.
   * The cumulative distribution values are constructed on the fly and discarded afterwards.
   *
   * Calculates the weighted distance: average of x values times the distance.
   */
  def getDistanceWeighted(one: TreeMap[Double, Double], two: TreeMap[Double, Double]) = {
    // Definitions:
    // result will be here
    var maxDistance = 0.0
    // represents previous value of distribution with a smaller starting value
    var prevTwo = (0.0, 0.0)
    // represents next value of distribution with a smaller starting value
    var nextTwo = prevTwo
    // Guess which distribution has a smaller starting value
    var smaller = one
    var bigger = two

    /* Swap if the above assignment was not the right guess: */
    if (one.size > 0 && two.size > 0) {
      if (one.head._1 > two.head._1) {
        smaller = two
        bigger = one
      }
    }

    // use these to keep the cumulative distribution current value
    var sumOne = 0.0
    var sumTwo = 0.0

    //println("one.size=" + one.size + " two.size=" + two.size)

    // advance the smaller dist manually
    var smallIter = smaller.iterator
    // and the bigger automatically
    for (k <- bigger) {
      // current value of bigger dist
      sumOne += k._2

      // advance smaller past bigger, keep prev and next
      // from either side of the current value of bigger
      while (smallIter.hasNext && nextTwo._1 <= k._1) {
        var temp = smallIter.next
        sumTwo += temp._2

        // assign cumulative dist value
        nextTwo = (temp._1, sumTwo)
        //println("nextTwo._1=" + nextTwo._1 + " k._1=" + k._1)
        if (nextTwo._1 <= k._1) {
          prevTwo = nextTwo
        }
      }

      /* now nextTwo >= k > prevTwo */

      /* (NoApp - App) gives a high positive number
         * if the app uses a more energy. This is because
         * if the app distribution is shifted to the right,
         * it has a high probability of running at a high drain rate,
         * and so its cumulative dist value is lower, and NoApp
         * has a higher value. Inverse for low energy usage. */

      /*
       * Weighted distance: average of x values times the distance.
       */
      val distance = {
        if (smaller != two)
          (sumOne - prevTwo._2) * ((k._1 - prevTwo._1) / 2)
        else
          (prevTwo._2 - sumOne) * ((k._1 - prevTwo._1) / 2)
      }
      if (math.abs(distance) > math.abs(maxDistance))
        maxDistance = distance
    }
    maxDistance
  }

  /**
   * Get the cumulative integral values of both distributions.
   *
   */
  def getCumulativeIntegrals(one: TreeMap[Double, Double], two: TreeMap[Double, Double]) = {
    var integralOne = 0.0
    var integralTwo = 0.0

    var old = 0.0
    var sum = 0.0
    for (k <- one) {
      sum += k._2
      integralOne += sum * (k._1 - old)
      old = k._1
    }

    old = 0.0
    sum = 0.0
    for (k <- two) {
      sum += k._2
      integralTwo += sum * (k._1 - old)
      old = k._1
    }
    (integralOne, integralTwo)
  }
  
  def nDecimal(orig:Double) = {
    var result = orig
    var mul = 1
    for (k <- 0 until DECIMALS)
      mul *= 10
    result = math.round(result*mul)
    result/mul
  }
  
   def nInt(orig:Double) = {
    var mul = 1
    for (k <- 0 until DECIMALS)
      mul *= 10
    val r = math.round(orig*mul)
    r
  }
}
