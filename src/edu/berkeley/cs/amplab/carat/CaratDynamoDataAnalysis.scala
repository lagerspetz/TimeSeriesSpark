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

  /**
   * Main program entry point.
   */
  def main(args: Array[String]) {
    var master = "local[1]"
    if (args != null || args.length >= 1)
      master = args(0)
    if (args != null || args.length > 1 && args(1) == "DEBUG")
      DEBUG = true
      
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
      handleRegs(sc, _, _, allUuids, allOses, allModels), allRates)
      
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

    //val parallel = sc.parallelize[java.util.Map[String, com.amazonaws.services.dynamodb.model.AttributeValue]](regs)
    //parallel.foreach(x => {

    // Remove duplicates caused by re-registrations:
    val regSet: Set[(String, String, String)] = new HashSet[(String, String, String)]
    regSet ++= regs.map(x => {
      val uuid = {val attr = x.get(regsUuid); if (attr != null) attr.getS() else "" }
      val model = {val attr = x.get(regsModel); if (attr != null) attr.getS() else "" }
      val os = {val attr = x.get(regsOs); if (attr != null) attr.getS() else "" }
      (uuid, model, os)
    })

    var distRet: spark.RDD[CaratRate] = dist
    for (x <- regSet) {
      /*
       * Data format guess:
       * Registration(uuId:0FC81205-55D0-46E5-8C80-5B96F17B5E7B, platformId:iPhone Simulator, osVersion:5.0)
       */
      val uuid = x._1
      val model = x._2
      val os = x._3

      // Collect all uuids, models and oses in the same loop
      uuids += uuid
      models += model
      oses += os

      println("Handling reg:" + x)
      /* 
       * FIXME: With incremental processing, the LAST sample or a few last samples
       * (as many as have a zero battery drain) should be re-used in the next batch. 
       */
      distRet = DynamoDbItemLoop(DynamoDbDecoder.getItems(samplesTable, uuid), DynamoDbDecoder.getItems(samplesTable, uuid, _), handleSamples(sc, _, os, model, _), distRet)
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
    dist: spark.RDD[edu.berkeley.cs.amplab.carat.CaratRate]) = {
    /* 
       * FIXME: With incremental processing, the LAST sample or a few last samples
       * (as many as have a zero battery drain) should be re-used in the next batch. 
       */
    var finished = false

    var (key, results) = tableAndValueToKeyAndResults
    println("Got: " + results.size + " results.")

    var distRet: spark.RDD[CaratRate] = null
    distRet = stepHandler(results, dist)

    if (key == null)
      finished = true

    while (!finished) {
      // avoid overloading "provisionedThroughput"
      if (LIMIT_SPEED)
        Thread.sleep(1000)

      println("Continuing from key=" + key)
      var (key2, results2) = tableAndValueToKeyAndResultsContinue(key)
      results = results2
      key = key2
      println("Got: " + results.size + " results.")

      distRet = stepHandler(results, distRet)
      if (key == null)
        finished = true
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
        //println("uuid=" + uuid + " apps=" + apps)

        val time = { val attr = x.get(sampleTime); if (attr != null) attr.getN() else "" }
        val batteryState = { val attr = x.get(sampleBatteryState); if (attr != null) attr.getS() else "" }
        val batteryLevel = { val attr = x.get(sampleBatteryLevel); if (attr != null) attr.getN() else "" }
        val event = { val attr = x.get(sampleEvent); if (attr != null) attr.getS() else "" }
        (uuid, time, batteryLevel, event, batteryState, apps)
      })
      rateMapper(os, model, mapped)
    })
    if (rates != null)
      rateRdd = rateRdd.union(rates)
    rateRdd
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
    var pluggedIn = false

    for (k <- observations) {
      d = k._2.toDouble
      batt = k._3.toDouble
      events = new ArrayBuffer[String]
      events ++= k._4.trim().toLowerCase().split(" ")
      events += k._5.trim().toLowerCase()
      apps = k._6
      if (DEBUG)
        printf("batt=%f prevBatt=%f drain=%f events=%s apps=%s\n", batt, prevBatt, prevBatt-batt, events, apps)

      if (events.contains(discharge)) {
        unplugged = true
        pluggedIn = false
      }else if (events.contains(charge)){
        pluggedIn = true
        unplugged = false
      }

      /* Ignore measurements until unplugged event
       * and record until pluggedIn */
      if (unplugged) {
        if (prevD == 0) {
          prevD = d
          prevBatt = batt
        }
        if (!pluggedIn) {
          if (DEBUG)
            printf("unplugged batt=%f prevBatt=%f drain=%f events=%s apps=%s\n", batt, prevBatt, prevBatt-batt, events, apps)
          // take periods where battery life has changed
          if (batt - prevBatt >= 0.01 || prevBatt - batt >= 0.01) {
            if (prevBatt -batt < 0){
              printf("prevBatt %s batt %s for observation %s\n", prevBatt, batt, k)
            }else{
              rates += new CaratRate(k._1, os, model, prevD, d, prevBatt, batt,
              prevEvents.toArray, events.toArray, prevApps.toArray, apps)
            }
            prevD = d
            prevBatt = batt
            // Reset, current apps and events added below
            prevEvents = new HashSet[String]()
            prevApps = new HashSet[String]()
          }
        }
        /* 
       * done every time to make sure the set contains all
       * the apps and events that happened between the two points
       * Do not add if there are trailing measurements
       * Do add the one with pluggedIn but nothing after it
       */
        // last one ...
        /* FIXME: Last one is not "last" many times, since this is incremental.
         * This should work anyway. However, the state of plugged in or not should be remembered.
         * Current solution: Use charging and discharging battery state for this.
         */
        if (k == observations.last && !pluggedIn) {
          if (prevD != d) {
            if (prevBatt -batt >= 0){
              rates += new CaratRate(observations.last._1, os, model, prevD, d, prevBatt, batt,
              prevEvents.toArray, events.toArray, prevApps.toArray, apps)
            }else
              printf("[last] prevBatt %s batt %s for observation %s\n", prevBatt, batt, k)
          }
        }

        if (!pluggedIn) {
          prevApps ++= apps
          prevEvents ++= events
        }

        if (events.contains(charge)) {
          pluggedIn = true
          unplugged = false
          prevD = 0
          prevBatt = 0
        }
      }
    }
    
    rates.toSeq
  }

  /**
   * Filter a set of (String, Seq[CaratRates]). Useful for writing a filter that
   * applies to a single CaratRate.
   */
  def distributionFilter(rates: (String, Seq[CaratRate]), filter: CaratRate => Boolean) = {
    (rates._1, rates._2.filter(filter))
  }

  /**
   * Use with distributionFilter. A convenience function that matches apps of the CaratRate by exact String.
   */
  def appFilter(rate: CaratRate, app: String) = rate.apps1.contains(app) || rate.apps2.contains(app)

  /**
   * Use with distributionFilter. A convenience function that is the inverse of `appFilter`.
   */
  def negativeAppFilter(rate: CaratRate, app: String) = !appFilter(rate, app)

  /**
   * Create a probability function out of a set of CaratRates.
   */
  def prob(rates: Array[CaratRate]) = {
    var sum = 0.0
    var buf = new TreeMap[Double, Double]
    for (d <- rates) {
      var count = buf.get(d.rate)
      buf += ((d.rate, count.getOrElse(0.0) + 1.0))
      sum += 1
    }

    for (k <- buf)
      buf += ((k._1, k._2 / sum))

    buf
  }

  /**
   * Main analysis function. Called on the entire collected set of CaratRates.
   */
  def analyzeRateData(allRates: RDD[CaratRate],
    uuids: Set[String], oses: Set[String], models: Set[String]) {
    if (DEBUG){
      val cc = allRates.collect()
      for (k <- cc)
        println(k)
    }
    
    val apps = allRates.map(_.getAllApps()).collect()

    var allApps = new HashSet[String]
    for (k <- apps)
      allApps ++= k
    println("AllApps: " + allApps)

    for (os <- oses) {
      val fromOs = allRates.filter(_.os == os)
      val notFromOs = allRates.filter(_.os != os)
      // no distance check, not bug or hog
      writeTripletUngrouped(fromOs, notFromOs, DynamoDbEncoder.put(osTable, osKey, os, _, _, _, _), false)
    }

    for (model <- models) {
      val fromModel = allRates.filter(_.model == model)
      val notFromModel = allRates.filter(_.model != model)
      // no distance check, not bug or hog
      writeTripletUngrouped(fromModel, notFromModel, DynamoDbEncoder.put(modelsTable, modelKey, model, _, _, _, _), false)
    }

    for (uuid <- uuids) {
      /*
       * TODO: if there are other combinations with uuid, they go into this loop
       */

      val fromUuid = allRates.filter(_.uuid == uuid)

      val tempApps = fromUuid.map(_.getAllApps()).collect()

      var uuidApps = new HashSet[String]
      for (k <- tempApps)
        uuidApps ++= k

      similarApps(allRates, uuid, uuidApps)

      val notFromUuid = allRates.filter(_.uuid != uuid)
      // no distance check, not bug or hog
      writeTripletUngrouped(fromUuid, notFromUuid, DynamoDbEncoder.put(resultsTable, resultKey, uuid, _, _, _, _, uuidApps.toSeq), false)

      /* Only consider apps reported from this uuId. */
      for (app <- uuidApps) {
        if (app != CARAT) {
          val appFromUuid = fromUuid.filter(_.getAllApps().contains(app))
          val appNotFromUuid = notFromUuid.filter(_.getAllApps().contains(app))
          writeTripletUngrouped(appFromUuid, appNotFromUuid, DynamoDbEncoder.putBug(bugsTable, (resultKey, appKey), (uuid, app), _, _, _, _))
        }
      }
    }

    for (app <- allApps) {
      if (app != CARAT) {
        val filtered = allRates.filter(_.getAllApps().contains(app))
        val filteredNeg = allRates.filter(!_.getAllApps().contains(app))
        writeTripletUngrouped(filtered, filteredNeg, DynamoDbEncoder.put(appsTable, appKey, app, _, _, _, _))
      }
    }
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
    writeTripletUngrouped(similar, dissimilar, DynamoDbEncoder.put(similarsTable, similarKey, uuid, _, _, _, _), false)
  }

  /**
   * Write the probability distributions, the distance, and the xmax value to DynamoDb. Ungrouped CaratRates variant.
   */
  def writeTripletUngrouped(one: RDD[CaratRate], two: RDD[CaratRate], putFunction: (Double, Seq[(Int, Double)], Seq[(Int, Double)], Double) => Unit, distanceCheck: Boolean = true) = {
    // probability distribution: r, count/sumCount

    /* Figure out max x value (maximum rate) and bucket y values of 
       * both distributions into n buckets, averaging inside a bucket
       */
    val flatOne = one.collect()
    val flatTwo = two.collect()

    println("Nonzero rates: " + flatOne.filter(_.rate() > 0).map(_.rate()).mkString(" "))
    println("Nonzero ratesNeg: "  + flatTwo.filter(_.rate() > 0).map(_.rate()).mkString(" "))
    
    val values = prob(flatOne)
    val others = prob(flatTwo) 
    println("prob1.size=" + values.size + " prob2.size=" + others.size)
    if (values.size > 0 && others.size > 0) {
      /*debug */
      if (DEBUG){
        val nzp = values.filter(_._2 > 0)
        val nnzp = others.filter(_._2 > 0)
        var sum = 0.0
        var nsum = 0.0
        for (k <- nzp)
          sum += k._2
        for (k <- nnzp)
          nsum += k._2
        
        println("Nonzero prob: " + nzp.mkString(" ") + " sum="+sum)
        println("Nonzero probNeg: "  + nnzp.mkString(" ") + " sum="+nsum)
      }
      val distance = getDistanceNonCumulative(values, others)

      if (distance >= 0 || !distanceCheck) {
        val (maxX, bucketed, bucketedNeg) = bucketDistributionsByX(values, others)
        if (DEBUG) {
          println("Nonzero bucket: " + bucketed.filter(_._2 > 0).mkString(" "))
          println("Nonzero bucketNeg: " + bucketedNeg.filter(_._2 > 0).mkString(" "))
        }
        putFunction(maxX, bucketed.toArray[(Int, Double)], bucketedNeg.toArray[(Int, Double)], distance)
      }
    }
  }
  
  
   /**
   * Bucket given distributions into `buckets` buckets, and return the maximum x value and the bucketed distributions. 
   */
  def bucketDistributionsByX(values: TreeMap[Double, Double], others: TreeMap[Double, Double]) = {
    val bucketed = new TreeMap[Int, Double]
    val bucketedNeg = new TreeMap[Int, Double]

    val xmax = math.max(values.last._1, others.last._1)

    for (k <- values) {
      val x = k._1 / xmax
      val bucket = (x * buckets).toInt
      var old = bucketed.get(bucket).getOrElse(0.0)
      bucketed.put(bucket, old + k._2)
    }
    
    for (k <- others){
      val x = k._1 / xmax
      val bucket = (x * buckets).toInt
      var old = bucketedNeg.get(bucket).getOrElse(0.0)
      bucketedNeg.put(bucket, old + k._2)
    }
    
    for (k <- 0 until buckets){
      if (!bucketed.contains(k))
        bucketed.put(0, 0.0)
      if (!bucketedNeg.contains(k))
        bucketedNeg.put(0, 0.0)
    }
    
    (xmax, bucketed, bucketedNeg)
  }

  /**
   * Bucket given distributions into `buckets` buckets, and return the maximum x value and the bucketed distributions. 
   */
  def bucketDistributionsOld(values: TreeMap[Double, Double], others: TreeMap[Double, Double]) = {
    /*maxX defines the buckets. Each bucket is
     * k /100 * maxX to k+1 / 100 * maxX.
     * Therefore, do not store the bucket starts and ends, only bucket numbers from 0 to 99.*/
    val bucketed = new ArrayBuffer[(Int, Double)]
    val bucketedNeg = new ArrayBuffer[(Int, Double)]

    val maxX = math.max(values.last._1, others.last._1)
    
    val iter = values.iterator
    val otherIter = others.iterator
    
    var nx = (0.0, 0.0)
    var nnx = (0.0, 0.0)
    
    for (k <- 0 until buckets) {
      val start = k * maxX / buckets
      val end = (k+1) * maxX / buckets
      
      var sumV = 0.0
      var xValue = end
      var yValue = 0.0
      
      if (iter.hasNext){
        nx = iter.next()
        xValue = nx._1
        yValue = nx._2
      }

      while ((xValue > start || (start == 0 && xValue >= start)) && xValue <= end) {
        sumV += yValue
        if (iter.hasNext) {
          nx = iter.next()
          xValue = nx._1
          yValue = nx._2
        }
      }

      var sumO = 0.0
      xValue = end
      yValue = 0.0
      
      if (otherIter.hasNext){
        nnx = otherIter.next()
        xValue = nnx._1
        yValue = nnx._2
      }

      while ((xValue > start || (start == 0 && xValue >= start)) && xValue <= end) {
        sumO += yValue
        if (otherIter.hasNext) {
          nnx = otherIter.next()
          xValue = nnx._1
          yValue = nnx._2
        }
      }
      
      bucketed += ((k, nDecimal(sumV)))
      bucketedNeg += ((k, nDecimal(sumO)))
    }

    (maxX, bucketed, bucketedNeg)
  }

  def nDecimal(orig: Double) = {
    var result = orig
    var mul = 1
    for (k <- 0 until DECIMALS)
      mul *= 10
    result = math.round(result * mul)
    result / mul
  }

  /**
   * Get the distance from a regular, non-cumulative distribution.
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
}
