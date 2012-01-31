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
   * Todo: do not store hogs or bugs with negative distance values.
   *
   */

  // Bucketing and decimal constants
  val buckets = 100
  val DECIMALS = 3
  val DEBUG = false

  val LIMIT_SPEED = false

  /**
   * Main program entry point.
   */
  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "CaratDynamoDataAnalysis")
    analyzeData(sc)
    sys.exit(0)
  }

  /**
   * Main function. Called from main() after sc initialization.
   */

  def analyzeData(sc: SparkContext) = {
    /* Get unique uuIds, Oses, and Models from registrations.
     * 
     * TODO: This should ideally be done inside an RDD part by part.
     * However, the current implementation does things in batches,
     * so it should be sufficient for memory saving for now.
     */
    val (allUuids, allOses, allModels) = {
      val resultUuids = new HashSet[String]
      val resultModels = new HashSet[String]
      val resultOses = new HashSet[String]
      var finished = false

      var (key, regs) = DynamoDbDecoder.getAllItems(registrationTable)
      println("Got: " + regs.size + " registrations.")
      for (k <- regs) {
        resultUuids += k.get(regsUuid).getOrElse("").toString()
        resultModels += k.get(regsModel).getOrElse("").toString()
        resultOses += k.get(regsOs).getOrElse("").toString()
      }

      if (key == null)
        finished = true
      while (!finished) {
        println("Continuing from key=" + key)
        var (key2, regs2) = DynamoDbDecoder.getAllItems(registrationTable, key)
        regs = regs2
        key = key2
        println("Got: " + regs.size + " registrations.")
        for (k <- regs) {
          resultUuids += k.get(regsUuid).getOrElse("").toString()
          resultModels += k.get(regsModel).getOrElse("").toString()
          resultOses += k.get(regsOs).getOrElse("").toString()
        }
        if (key == null)
          finished = true
      }
      (resultUuids, resultOses, resultModels)
    }

    println("All uuIds: " + allUuids.mkString(", "))
    println("All oses: " + allOses.mkString(", "))
    println("All models: " + allModels.mkString(", "))

    var allRates: spark.RDD[CaratRate] = null
    var allData: spark.RDD[(String, Seq[CaratRate])] = null

    allRates = DynamoDbItemLoop(DynamoDbDecoder.getAllItems(registrationTable),
      DynamoDbDecoder.getAllItems(registrationTable, _),
      handleRegs(sc, _, _), allRates)

    if (allRates != null) {
      allData = allRates.map(x => {
        (x.uuid, x)
      }).groupByKey()
    }

    if (allData != null)
      analyzeRateData(allData, allUuids, allOses, allModels)
  }

  /**
   * Handles a set of registration messages from the Carat DynamoDb.
   * Samples matching each registration identifier are got, rates calculated from them, and combined with `dist`.
   */
  def handleRegs(sc: SparkContext, regs: Seq[Map[String, Any]], dist: spark.RDD[CaratRate]) = {
    /* FIXME: I would like to do this in parallel, but that would not let me re-use
     * all the data for the other uuids, resulting in n^2 execution time.
     */

    //val parallel = sc.parallelize[java.util.Map[String, com.amazonaws.services.dynamodb.model.AttributeValue]](regs)
    //parallel.foreach(x => {

    // Remove duplicates caused by re-registrations:
    val regSet: Set[(String, String, String)] = new HashSet[(String, String, String)]
    regSet ++= regs.map(x => {
      val uuid = x.get(regsUuid).getOrElse("").toString()
      val model = x.get(regsModel).getOrElse("").toString()
      val os = x.get(regsOs).getOrElse("").toString()
      (uuid, model, os)
    })

    var distRet: spark.RDD[CaratRate] = dist
    for (x <- regSet) {
      /*
       * Data format guess:
       * Registration(uuId:0FC81205-55D0-46E5-8C80-5B96F17B5E7B, platformId:iPhone Simulator, systemVersion:5.0)
       */
      val uuid = x._1
      val model = x._2
      val os = x._3
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
  def DynamoDbItemLoop(tableAndValueToKeyAndResults: => (com.amazonaws.services.dynamodb.model.Key, scala.collection.mutable.Buffer[Map[String, Any]]),
    tableAndValueToKeyAndResultsContinue: com.amazonaws.services.dynamodb.model.Key => (com.amazonaws.services.dynamodb.model.Key, scala.collection.mutable.Buffer[Map[String, Any]]),
    stepHandler: (scala.collection.mutable.Buffer[Map[String, Any]], spark.RDD[edu.berkeley.cs.amplab.carat.CaratRate]) => spark.RDD[edu.berkeley.cs.amplab.carat.CaratRate],
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
        Thread.sleep(1)

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
  def handleSamples(sc: SparkContext, samples: Seq[Map[java.lang.String, Any]], os: String, model: String, rates: RDD[CaratRate]) = {
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
        val uuid = x.get(sampleKey).getOrElse("").toString()
        val apps = x.get(sampleProcesses).getOrElse(new ArrayBuffer[String]).asInstanceOf[Seq[String]].map(w => {
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

        val time = x.get(sampleTime).getOrElse("").toString()
        val batteryState = x.get(sampleBatteryState).getOrElse("").toString()
        val batteryLevel = x.get(sampleBatteryLevel).getOrElse("").toString()
        val event = x.get(sampleEvent).getOrElse("").toString()
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

      if (events.contains(discharge)) {
        unplugged = true
        pluggedIn = false
      }

      /* Ignore measurements until unplugged event
       * and record until pluggedIn */
      if (unplugged) {
        if (prevD == 0) {
          prevD = d
          prevBatt = batt
        }
        if (!pluggedIn) {
          // take periods where battery life has changed
          if (batt - prevBatt >= 1 || prevBatt - batt >= 1) {

            rates += new CaratRate(k._1, os, model, prevD, d, prevBatt, batt,
              prevEvents.toArray, events.toArray, prevApps.toArray, apps)
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
        if ((k == observations.last && !pluggedIn) || (!pluggedIn && (events.contains(charge)))) {
          if (prevD != d) {
            rates += new CaratRate(observations.last._1, os, model, prevD, d, prevBatt, batt,
              prevEvents.toArray, events.toArray, prevApps.toArray, apps)
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
  def prob(rates: RDD[(String, Seq[CaratRate])]) = {
    rates.mapValues(x => {
      var sum = 0.0
      var buf = new TreeMap[Double, Double]
      for (d <- x) {
        var count = buf.get(d.rate)
        buf += ((d.rate, count.getOrElse(0.0) + 1.0))
        sum += 1
      }

      for (k <- buf)
        buf += ((k._1, k._2 / sum))

      buf
    })
  }

  def analyzeRateData(rateData: RDD[(String, Seq[CaratRate])],
    uuids: Set[String], oses: Set[String], models: Set[String]) {
    val apps = rateData.map(x => {
      var buf = new HashSet[String]
      for (k <- x._2) {
        buf ++= k.getAllApps()
      }
      buf
    }).collect()

    var allApps = new HashSet[String]
    for (k <- apps)
      allApps ++= k
    println("AllApps: " + allApps)

    for (os <- oses) {
      val fromOs = rateData.map(distributionFilter(_, _.os == os))
      val notFromOs = rateData.map(distributionFilter(_, _.os != os))
      // no distance check, not bug or hog
      writeTriplet(fromOs, notFromOs, DynamoDbEncoder.put(osTable, osKey, os, _, _, _, _), false)
    }

    for (model <- models) {
      val fromModel = rateData.map(distributionFilter(_, _.model == model))
      val notFromModel = rateData.map(distributionFilter(_, _.model != model))
      // no distance check, not bug or hog
      writeTriplet(fromModel, notFromModel, DynamoDbEncoder.put(modelsTable, modelKey, model, _, _, _, _), false)
    }

    for (uuid <- uuids) {
      /*
       * TODO: if there are other combinations with uuid, they go into this loop
       */

      val fromUuid = rateData.filter(_._1 == uuid)

      val tempApps = fromUuid.map(x => {
        var buf = new HashSet[String]
        for (k <- x._2) {
          buf ++= k.getAllApps()
        }
        buf
      }).collect()

      var uuidApps = new HashSet[String]
      for (k <- tempApps)
        uuidApps ++= k

      similarApps(rateData, uuid, uuidApps)

      val notFromUuid = rateData.filter(_._1 != uuid)
      // no distance check, not bug or hog
      writeTriplet(fromUuid, notFromUuid, DynamoDbEncoder.put(resultsTable, resultKey, uuid, _, _, _, _, uuidApps.toSeq), false)

      for (app <- allApps) {
        if (app != CARAT) {
          val appFromUuid = fromUuid.map(distributionFilter(_, appFilter(_, app)))
          val appNotFromUuid = notFromUuid.map(distributionFilter(_, appFilter(_, app)))
          writeTriplet(appFromUuid, appNotFromUuid, DynamoDbEncoder.putBug(bugsTable, (resultKey, appKey), (uuid, app), _, _, _, _))
        }
      }
    }

    for (app <- allApps) {
      if (app != CARAT) {
        val filtered = rateData.map(distributionFilter(_, appFilter(_, app)))
        val filteredNeg = rateData.map(distributionFilter(_, negativeAppFilter(_, app)))
        writeTriplet(filtered, filteredNeg, DynamoDbEncoder.put(appsTable, appKey, app, _, _, _, _))
      }
    }
  }

  def similarApps(all: RDD[(String, Seq[CaratRate])], uuid: String, uuidApps: Set[String]) {
    val sCount = similarityCount(uuidApps.size)
    printf("SimilarApps uuid=%s sCount=%s uuidApps.size=%s\n", uuid, sCount, uuidApps.size)
    val similar = all.map(distributionFilter(_, _.getAllApps().intersect(uuidApps).size >= sCount))
    val dissimilar = all.map(distributionFilter(_, _.getAllApps().intersect(uuidApps).size < sCount))
    //printf("SimilarApps similar.count=%s dissimilar.count=%s\n",similar.count(), dissimilar.count())
    // no distance check, not bug or hog
    writeTriplet(similar, dissimilar, DynamoDbEncoder.put(similarsTable, similarKey, uuid, _, _, _, _), false)
  }

  def writeTriplet(one: RDD[(String, Seq[CaratRate])], two: RDD[(String, Seq[CaratRate])], putFunction: (Double, Seq[(Int, Double)], Seq[(Int, Double)], Double) => Unit, distanceCheck: Boolean = true) = {
    // probability distribution: r, count/sumCount

    /* Figure out max x value (maximum rate) and bucket y values of 
       * both distributions into n buckets, averaging inside a bucket
       */
    val probOne = prob(one)
    val probTwo = prob(two)

    val values = flatten(probOne)
    val others = flatten(probTwo)

    println("prob1.size=" + values.size + " prob2.size=" + others.size)
    if (values.size > 0 && others.size > 0) {
      val cumulative = flatten(probOne.mapValues(x => {
          var sum = 0.0
          var buf = new TreeMap[Double, Double]
          for (d <- x) {
            sum += d._2
            buf += ((d._1, sum))
          }
          buf
        }))
        val cumulativeNeg = flatten(probTwo.mapValues(x => {
          var sum = 0.0
          var buf = new TreeMap[Double, Double]
          for (d <- x) {
            sum += d._2
            buf += ((d._1, sum))
          }
          buf
        }))
      val oldDist = getDistance(cumulative, cumulativeNeg)
      val distance = getDistanceNonCumulative(values, others, cumulative, cumulativeNeg)
      
      println("old Dist: " + oldDist + "\n" +
        "distance: " + distance)
      if (distance >= 0 || !distanceCheck) {
        val (maxX, bucketed, bucketedNeg) = bucketDistributions(values, others)
        putFunction(maxX, bucketed, bucketedNeg, distance)
      }
    }
  }

  def bucketDistributions(values: Array[(Double, Double)], others: Array[(Double, Double)]) = {
    /*maxX defines the buckets. Each bucket is
     * k /100 * maxX to k+1 / 100 * maxX.
     * Therefore, do not store the bucket starts and ends, only bucket numbers from 0 to 99.*/
    val bucketed = new ArrayBuffer[(Int, Double)]
    val bucketedNeg = new ArrayBuffer[(Int, Double)]

    val maxX = math.max(values.last._1, others.last._1)

    var valueIndex = 0
    var othersIndex = 0
    for (k <- 0 until buckets) {
      val start = maxX / buckets * k
      val end = maxX / buckets * (k + 1)
      var sumV = 0.0
      var sumO = 0.0
      var countV = valueIndex
      var countO = valueIndex
      while (valueIndex < values.size && values(valueIndex)._1 >= start && values(valueIndex)._1 < end) {
        sumV += values(valueIndex)._2
        valueIndex += 1
      }
      countV = valueIndex - countV
      while (othersIndex < others.size && others(othersIndex)._1 >= start && others(othersIndex)._1 < end) {
        sumO += others(othersIndex)._2
        othersIndex += 1
      }
      countO = othersIndex - countO
      if (countV > 0) {
        bucketed += ((k, nDecimal(sumV / countV)))
      } else
        bucketed += ((k, 0.0))
      if (countO > 0) {
        bucketedNeg += ((k, nDecimal(sumO / countO)))
      } else
        bucketedNeg += ((k, 0.0))
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

  def getDistanceNonCumulative(one: Array[(Double, Double)], two: Array[(Double, Double)], debug1: Array[(Double, Double)], debug2: Array[(Double, Double)]) = {
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
    
    var smallerDbg = debug1
    var biggerDbg = debug2

    /* Swap if the above assignment was not the right guess: */
    if (one.size > 0 && two.size > 0) {
      if (one.head._1 > two.head._1) {
        smaller = two
        bigger = one
      }
    }
    
    if (smallerDbg.size > 0 && biggerDbg.size > 0) {
      if (smallerDbg.head._1 > biggerDbg.head._1) {
        smaller = debug2
        bigger = debug1
      }
    }
    
    var bigCounter = 0
    var smallNext = 0
    var smallLast = 0

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
      if (bigCounter < biggerDbg.size)
        printf("bigger: %s debug: %s\n",sumOne,biggerDbg(bigCounter)._2)
      else
        printf("bigger: %s\n",sumOne)
      bigCounter+=1
      
      // advance smaller past bigger, keep prev and next
      // from either side of the current value of bigger
      while (smallIter.hasNext && nextTwo._1 <= k._1) {
        var temp = smallIter.next
        sumTwo += temp._2
        if (smallNext < smallerDbg.size)
          printf("smaller: %s debug: %s\n",sumTwo,smallerDbg(smallNext)._2)
        else
          printf("smaller: %s\n",sumTwo)
        
        // assign cumulative dist value
        nextTwo = (temp._1, sumTwo)
        //println("nextTwo._1=" + nextTwo._1 + " k._1=" + k._1)
        if (nextTwo._1 <= k._1){
          prevTwo = nextTwo
          smallLast = smallNext
        }
        smallNext += 1
      }

      /* now nextTwo >= k > prevTwo */

      /* (NoApp - App) gives a high positive number
         * if the app uses a more energy. This is because
         * if the app distribution is shifted to the right,
         * it has a high probability of running at a high drain rate,
         * and so its cumulative dist value is lower, and NoApp
         * has a higher value. Inverse for low energy usage. */
      val distance = prevTwo._2 - sumOne
      if (smallNext < smallerDbg.size && bigCounter < biggerDbg.size)
        printf("%s debug\n%s prevTwo \n%s k\n%s kbug\n%s nextTwo\n%s debug\ndistance:%s\n", smallerDbg(smallLast), prevTwo, k, biggerDbg(bigCounter), nextTwo, smallerDbg(smallNext), distance)
      if (distance > maxDistance)
        maxDistance = distance
    }
    maxDistance
  }

  def getDistance(one: Array[(Double, Double)], two: Array[(Double, Double)]) = {
    // FIXME: use of collect may cause memory issues.
    var maxDistance = -2.0
    var prevOne = (-2.0, 0.0)
    var prevTwo = (-2.0, 0.0)
    var nextTwo = prevTwo

    var smaller = one
    var bigger = two

    /* Swap if the above assignment was not the right guess: */
    if (one.size > 0 && two.size > 0) {
      if (one.head._1 > two.head._1) {
        smaller = two
        bigger = one
      }
    }

    //println("one.size=" + one.size + " two.size=" + two.size)

    var smallIter = smaller.iterator
    for (k <- bigger) {
      var distance = 0.0

      while (smallIter.hasNext && nextTwo._1 <= k._1) {
        nextTwo = smallIter.next
        //println("nextTwo._1=" + nextTwo._1 + " k._1=" + k._1)
        if (nextTwo._1 <= k._1)
          prevTwo = nextTwo
      }

      /* now nextTwo has the bigger one,
         * prevTwo the one directly below k
         */

      /* NoApp - App gives a high positive number
         * if the app uses a more energy. This is because
         * if the app distribution is shifted to the right,
         * it has a high probability of running at a high drain rate,
         * and so its cumulative dist value is lower, and NoApp
         * has a higher value. Inverse for low energy usage. */
      distance = prevTwo._2 - k._2
      if (distance > maxDistance)
        maxDistance = distance
    }
    maxDistance
  }
}
