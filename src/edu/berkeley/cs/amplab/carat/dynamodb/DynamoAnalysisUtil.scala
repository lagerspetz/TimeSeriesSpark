package edu.berkeley.cs.amplab.carat.dynamodb

import com.amazonaws.services.dynamodb.model.AttributeValue
import scala.collection.immutable.HashSet
import java.io.File
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileInputStream
import java.io.FileWriter
import collection.JavaConversions._
import edu.berkeley.cs.amplab.carat._
import scala.collection.mutable.ArrayBuffer
import spark.timeseries.UniformDist
import spark._
import spark.SparkContext._
import com.amazonaws.services.dynamodb.model.Key
import scala.collection.immutable.HashMap

object DynamoAnalysisUtil {

  // constants for battery state and sample triggers
  val MODEL_SIMULATOR = "Simulator"
  val STATE_CHARGING = "charging"
  val STATE_DISCHARGING = "unplugged"
  val TRIGGER_BATTERYLEVELCHANGED = "batterylevelchanged"
  val ABNORMAL_RATE = 9

  def readDoubleFromFile(file: String) = {
    val f = new File(file)
    if (!f.exists() && !f.createNewFile())
      throw new Error("Could not create %s for reading!".format(file))
    val rd = new BufferedReader(new InputStreamReader(new FileInputStream(f)))
    var s = rd.readLine()
    rd.close()
    if (s != null && s.length > 0) {
      s.toDouble
    } else
      0.0
  }

  def saveDoubleToFile(d: Double, file: String) {
    val f = new File(file)
    if (!f.exists() && !f.createNewFile())
      throw new Error("Could not create %s for saving %f!".format(file, d))
    val wr = new FileWriter(file)
    wr.write(d + "\n")
    wr.close()
  }

  def readS3LineSet(bucket: String, file: String) = {
    var r: Set[String] = new HashSet[String]
    val rd = new BufferedReader(new InputStreamReader(S3Decoder.get(bucket, file)))
    var s = rd.readLine()
    while (s != null) {
      r += s
      s = rd.readLine()
    }
    println("%s/%s downloaded: %s".format(bucket, file, r))
    r
  }

  def regSet(regs: java.util.List[java.util.Map[String, AttributeValue]]) = {
    var regSet = new HashMap[String, (String, String)]
    regSet ++= regs.map(x => {
      val uuid = { val attr = x.get(regsUuid); if (attr != null) attr.getS() else "" }
      val model = { val attr = x.get(regsModel); if (attr != null) attr.getS() else "" }
      val os = { val attr = x.get(regsOs); if (attr != null) attr.getS() else "" }
      (uuid, (model, os))
    })
    regSet
  }

  def replaceOldRateFile(oldPath: String, newPath: String) {
    val rem = Runtime.getRuntime().exec(Array("/bin/rm", "-rf", oldPath))
    rem.waitFor()
    val move = Runtime.getRuntime().exec(Array("/bin/mv", newPath, oldPath))
    move.waitFor()
  }

  /**
   * Generic Carat DynamoDb loop function. Gets items from a table using keys given, and continues until the table scan is complete.
   * This function achieves a block by block read until the end of a table, regardless of throughput or manual limits.
   */
  def DynamoDbItemLoop(tableAndValueToKeyAndResults: => (com.amazonaws.services.dynamodb.model.Key, java.util.List[java.util.Map[String, AttributeValue]]),
    tableAndValueToKeyAndResultsContinue: com.amazonaws.services.dynamodb.model.Key => (com.amazonaws.services.dynamodb.model.Key, java.util.List[java.util.Map[String, AttributeValue]]),
    stepHandler: (java.util.List[java.util.Map[String, AttributeValue]], spark.RDD[edu.berkeley.cs.amplab.carat.CaratRate]) => spark.RDD[edu.berkeley.cs.amplab.carat.CaratRate],
    prefix: Boolean, /*prefixer: (java.util.List[java.util.Map[String, AttributeValue]]) => java.util.List[java.util.Map[String, AttributeValue]],*/
    dist: spark.RDD[edu.berkeley.cs.amplab.carat.CaratRate]) = {
    var finished = false

    var (key, results) = tableAndValueToKeyAndResults
    println("Got: " + results.size + " results.")

    var distRet: spark.RDD[CaratRate] = null
    distRet = stepHandler(results, dist)

    while (key != null) {
      println("Continuing from key=" + key)
      var (key2, results2) = tableAndValueToKeyAndResultsContinue(key)
      /* Add last sample here as the starting point for a Rate from it to the next retrieved one */
      if (prefix)
        results2.prepend(results.last)
      results = results2
      key = key2
      println("Got: " + results.size + " results.")

      distRet = stepHandler(results, distRet)
    }
    distRet
  }

  def sampleMapper(x: java.util.Map[String, AttributeValue]) = {
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
    if (evWith == 0 && evWithout == 0)
      0.0
    else
      1.0 - evWithout / evWith
  }

  /**
   * Map samples into CaratRates. `os` and `model` are inserted for easier later processing.
   * Consider sample pairs with non-blc endpoints rates from 0 to prevBatt - batt with uniform probability.
   */
  def rateMapperPairwise(os: String, model: String, observations: Seq[(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, Seq[String])]) = {
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
    var pointRates = 0

    var rates = new ArrayBuffer[CaratRate]

    for (k <- observations) {
      d = k._2.toDouble
      batt = k._3.toDouble
      event = k._4.trim().toLowerCase()
      state = k._5.trim().toLowerCase()
      apps = k._6

      if (state != STATE_CHARGING) {
        /* Record rates. First time fall through */
        if (prevD != 0 && prevD != d) {
          if (prevBatt - batt < 0) {
            printf("prevBatt %s batt %s for d1=%s d2=%s uuid=%s\n", prevBatt, batt, prevD, d, k._1)
            negDrainSamples += 1
          } else if (prevBatt == 0 && batt == 0) {
            /* Assume simulator, ignore */
            printf("prevBatt %s batt %s for d1=%s d2=%s uuid=%s\n", prevBatt, batt, prevD, d, k._1)
            allZeroSamples += 1
          } else {
            /* now prevBatt - batt >= 0 */
            if (prevEvent == TRIGGER_BATTERYLEVELCHANGED && event == TRIGGER_BATTERYLEVELCHANGED) {
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
                  pointRates += 1
                } else {
                  abandonedSamples += 1
                }
              }
            } else {
              /* One endpoint not BLC, use uniform distribution rate */
              val r = new CaratRate(k._1, os, model, prevD, d, prevBatt, batt, new UniformDist(prevBatt, batt, prevD, d),
                prevEvent, event, prevApps, apps)
              if (considerRate(r)) {
                rates += r
              } else {
                println("Abandoned uniform rate with abnormally high EV: " + r.toString(false))
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

    println(nzf("Recorded %s point rates ", pointRates) + "abandoned " +
      nzf("%s all zero, ", allZeroSamples) +
      nzf("%s charging, ", chargingSamples) +
      nzf("%s negative drain, ", negDrainSamples) +
      nzf("%s > " + ABNORMAL_RATE + " drain, ", abandonedSamples) +
      nzf("%s zero drain BLC", zeroBLCSamples) + " samples.")
    rates.toSeq
  }

  /**
   * Map samples into CaratRates. `os` and `model` are inserted for easier later processing.
   * Consider sample pairs with non-blc endpoints rates from 0 to prevBatt - batt with uniform probability.
   */
  def rateMapperPairwise(uuidToOsAndModel: scala.collection.mutable.HashMap[String, (String, String)],
    observations: Seq[(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, Seq[String])]) = {
    // Observations format: (uuid, time, batteryLevel, event, batteryState, apps)
    var prevUuid = ""
    var prevD = 0.0
    var prevBatt = 0.0
    var prevEvent = ""
    var prevState = ""
    var prevApps: Seq[String] = Array[String]()

    var uuid = ""
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
    var pointRates = 0

    var rates = new ArrayBuffer[CaratRate]
    val obsSorted = observations.sortWith((x, y) => {
      // order lexicographically by uuid
      if (x._1 < y._1)
        true
      else if (x._1 > y._1)
        false
      else if (x._1 == y._1 && x._2 < y._2)
        true
      else if (x._1 == y._1 && x._2 > y._2)
        false
      else
        true
      // if time and uuid are the same, sort first parameter first
    })
    for (k <- obsSorted) {
      uuid = k._1
      val (os, model) = uuidToOsAndModel.get(k._1).getOrElse("", "")
      d = k._2.toDouble
      batt = k._3.toDouble
      event = k._4.trim().toLowerCase()
      state = k._5.trim().toLowerCase()
      apps = k._6
      if (model != MODEL_SIMULATOR) {
        if (state != STATE_CHARGING) {
          /* Record rates. First time fall through.
           * Note: same date or different uuid does not result
           * in discard of the sample as a starting point for a rate.
           * However, we cannot have a rate across UUIDs or the same timestamp.
           */
          if (prevD != 0 && prevD != d && prevUuid == uuid) {
            if (prevBatt - batt < 0) {
              printf("prevBatt %s batt %s for d1=%s d2=%s uuid=%s\n", prevBatt, batt, prevD, d, k._1)
              negDrainSamples += 1
            } else if (prevBatt == 0 && batt == 0) {
              /* Assume simulator, ignore */
              printf("prevBatt %s batt %s for d1=%s d2=%s uuid=%s\n", prevBatt, batt, prevD, d, k._1)
              allZeroSamples += 1
            } else {

              /* now prevBatt - batt >= 0 */
              if (prevEvent == TRIGGER_BATTERYLEVELCHANGED && event == TRIGGER_BATTERYLEVELCHANGED) {
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
                    pointRates += 1
                  } else {
                    abandonedSamples += 1
                  }
                }
              } else {
                /* One endpoint not BLC, use uniform distribution rate */
                val r = new CaratRate(k._1, os, model, prevD, d, prevBatt, batt, new UniformDist(prevBatt, batt, prevD, d),
                  prevEvent, event, prevApps, apps)
                if (considerRate(r)) {
                  rates += r
                } else {
                  println("Abandoned uniform rate with abnormally high EV: " + r.toString(false))
                  abandonedSamples += 1
                }
              }
            }
          }
        } else {
          chargingSamples += 1
          // do not use charging samples as even starting points.
          prevD = 0
        }
      } else{
        // simulator samples also reset prevD
        prevD = 0
      }
      prevUuid = uuid
      prevD = d
      prevBatt = batt
      prevEvent = event
      prevState = state
      prevApps = apps
    }

    println(nzf("Recorded %s point rates ", pointRates) + "abandoned " +
      nzf("%s all zero, ", allZeroSamples) +
      nzf("%s charging, ", chargingSamples) +
      nzf("%s negative drain, ", negDrainSamples) +
      nzf("%s > " + ABNORMAL_RATE + " drain, ", abandonedSamples) +
      nzf("%s zero drain BLC", zeroBLCSamples) + " samples.")
    rates.toSeq
  }

  def nzf(formatString: String, number: Int) = {
    if (number > 0)
      formatString.format(number)
    else
      ""
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
    if (r.isRateRange()) {
      if (r.rateRange.getEv() > ABNORMAL_RATE) {
        false
      } else
        true
    } else {
      if (r.rate() > ABNORMAL_RATE) {
        printf("Abandoning abnormally high rate " + r.toString(false))
        false
      } else
        true
    }
  }

  /**
   * FIXME: Filters inside a map of another RDD are not allowed, so we call collect on the returned a priori distribution here.
   * If this becomes a memory problem, averaging in the a priori dist should be done.
   */
  def getApriori(allRates: RDD[CaratRate]) = {
    // get BLCs
    assert(allRates != null, "AllRates should not be null when calculating aPriori.")
    val ap = allRates.filter(!_.isRateRange())
    assert(ap.count > 0, "AllRates should contain some rates that are not rateRanges when calculating aPriori.")
    // Get their rates and frequencies (1.0 for all) and group by rate 
    val grouped = ap.map(x => {
      ((x.rate, 1.0))
    }).groupByKey()
    // turn arrays of 1.0s to frequencies
    println("Collecting aPriori.")
    grouped.map(x => { (x._1, x._2.sum) }).collect()
  }

  def removeDaemons(daemonSet: Set[String]) {
    // add hog table key (which is the same as bug table app key)
    val kd = CaratDynamoDataAnalysis.DAEMONS_LIST.map(x => {
      (hogKey, x)
    }).toSeq
    DynamoDbItemLoop(DynamoDbDecoder.filterItems(hogsTable, kd: _*),
      DynamoDbDecoder.filterItemsFromKey(hogsTable, _, kd: _*),
      removeHogs(_, _))

    DynamoDbItemLoop(DynamoDbDecoder.filterItems(bugsTable, kd: _*),
      DynamoDbDecoder.filterItemsFromKey(bugsTable, _, kd: _*),
      removeBugs(_, _))
  }

  def removeBugs(key: Key, results: java.util.List[java.util.Map[String, AttributeValue]]) {
    for (k <- results) {
      val uuid = k.get(resultKey).getS()
      val app = k.get(hogKey).getS()
      println("Removing Bug: %s, %s".format(uuid, app))
      DynamoDbDecoder.deleteItem(bugsTable, uuid, app)
    }
  }

  def removeHogs(key: Key, results: java.util.List[java.util.Map[String, AttributeValue]]) {
    for (k <- results) {
      val app = k.get(hogKey).getS()
      println("Deleting: " + app)
      DynamoDbDecoder.deleteItem(hogsTable, app)
    }
  }

  /**
   * Generic DynamoDb loop function. Gets items from a table using keys given, and continues until the table scan is complete.
   * This function achieves a block by block read until the end of a table, regardless of throughput or manual limits.
   */
  def DynamoDbItemLoop(getKeyAndResults: => (Key, java.util.List[java.util.Map[String, AttributeValue]]),
    getKeyAndMoreResults: Key => (Key, java.util.List[java.util.Map[String, AttributeValue]]),
    handleResults: (Key, java.util.List[java.util.Map[String, AttributeValue]]) => Unit) {
    var index = 0
    var (key, results) = getKeyAndResults
    println("Got: " + results.size + " results.")
    handleResults(null, results)

    while (key != null) {
      index += 1
      println("Continuing from key=" + key)
      val (key2, results2) = getKeyAndMoreResults(key)
      results = results2
      handleResults(key, results)
      key = key2
      println("Got: " + results.size + " results.")
    }
  }
}