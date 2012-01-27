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
 * A program for calculating the rates, sorting them, and calculating their cumulative probability distribution
 * from a Carat data file of the format:
 * {{{
 * time, uid, battery level, space separated events, running apps
 * }}}
 * For example:
 * {{{
 * Mon Dec 26 09:43:36 PST 2011, 46, 99, batteryStatusChanged unplugged, Safari, Mail
 * Mon Dec 26 12:13:11 PST 2011, 46, 80, batteryStatusChanged, Safari, Mail, Angry Birds
 * Mon Dec 26 09:51:00 PST 2011, 86, 100, batteryStatusChanged unplugged, Safari, Mail
 * Mon Dec 26 17:16:00 PST 2011, 46, 66, batteryStatusChanged, Safari, Mail, Amazon Kindle
 * Mon Dec 26 12:23:11 PST 2011, 86, 68, batteryStatusChanged, Safari, Mail, Angry Birds, Opera
 * Mon Dec 26 16:11:00 PST 2011, 86, 65, batteryStatusChanged, Safari, Mail, Opera
 * Mon Dec 26 23:20:12 PST 2011, 46, 53, batteryStatusChanged pluggedIn usbPower, Safari, Mail
 * Mon Dec 27 01:10:01 PST 2011, 86, 20, batteryStatusChanged pluggedIn, Safari, Mail
 * }}}
 *
 * Usage:
 * {{{
 *  CaratFakeDataAnalysis.main(Array("fakedatafile.txt"))
 * }}}
 * The results will be called dumpsys-script-data.txt and powermon-file-data.txt.
 */

object CaratDynamoDataAnalysis {

  // Bucketing and decimal constants
  val buckets = 100
  val DECIMALS = 3

   /**
   * Main program entry point.
   */
  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "CaratDataAnalysis")
    analyzeData(sc)
    sys.exit(0)
  }

  /**
   * We will not be building distributions for all possible groups of apps.
   * I don't even think that would be tractable for very long. We need
   * distributions for (off the top of my head):
   * 1) App X is running
   * 2) App X is not running
   * 3) App X is running on uuid U
   * 4) App X is running on uuid != U
   * 5) OS Version == V
   * 6) OS Version != V
   * 7) Device Model == M
   * 8) Device Model != M
   * 9) uuid == U
   * 10) uuid != U
   *
   * We also need to compute distributions for users that run "similar"
   * apps. I am open to suggestions for how to do this efficiently while
   * still providing a number that means something.
   */

  def analyzeData(sc: SparkContext) = {
    /* get registrations.
     * This should ideally be done inside an RDD part by part.
     * A slightly better implementation than the below would also
     * be to work with one registration message at a time,
     * computing stuff for it and then requesting the next one.
     */

    /* 
     * Gather all uuids...
     * TODO: I think these we need in memory, unless we want to
     * request them again each time we need to get other devices' samples. 
     */

    val (allUuids, allOses, allModels) = {
      val resultUuids = new HashSet[String]
      val resultModels = new HashSet[String]
      val resultOses = new HashSet[String]
      var finished = false

      var (key, regs) = DynamoDbDecoder.getAllItems(registrationTable)
      for (k <- regs){
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

    var allData:spark.RDD[(String, Seq[CaratRate])] = null
    
    var finished = false
    var (key, regs) = DynamoDbDecoder.getAllItems(registrationTable)
    println("Got: " + regs.size + " registrations.")
    allData = handleRegs(sc, regs, allUuids)
    if (key == null)
      finished = true
    while (!finished) {
      println("Continuing from key=" + key)
      var (key2, regs2) = DynamoDbDecoder.getAllItems(registrationTable, key)
      regs = regs2
      key = key2
      println("Got: " + regs.size + " registrations.")
      if (allData != null)
        allData = allData.union(handleRegs(sc, regs, allUuids))
      if (key == null)
        finished = true
    }
    if (allData != null)
      analyzeRateData(allData, allUuids, allOses, allModels)
  }

  /**
   * 
   */
  def handleRegs(sc: SparkContext, regs: Seq[Map[String, Any]], allUuids: Set[String]) = {
    /* FIXME: I would like to do this in parallel, but that would not let me re-use
     * all the data for the other uuids, resulting in n^2 execution time.
     */
    
    //val parallel = sc.parallelize[java.util.Map[String, com.amazonaws.services.dynamodb.model.AttributeValue]](regs)
    //parallel.foreach(x => {
    
    // Remove duplicates caused by re-registrations:
    val regSet:Set[(String, String, String)] = new HashSet[(String, String, String)]
    regSet ++= regs.map(x => {
      val uuid = x.get(regsUuid).getOrElse("").toString()
      val model = x.get(regsModel).getOrElse("").toString()
      val os = x.get(regsOs).getOrElse("").toString()
      (uuid, model, os)
    })
    
    var dist: spark.RDD[CaratRate] = null
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
      var finished = false
      var (key, samples) = DynamoDbDecoder.getItems(samplesTable, uuid)
      println("Got: " + samples.size + " samples.")
      dist = handleSamples(sc, samples, os, model, dist)
      if (key == null)
        finished = true
      while (!finished) {
        // avoid overloading "provisionedThroughput"
        Thread.sleep(1) 
        println("Continuing samples from key=" + key)
        var (key2, samples2) = DynamoDbDecoder.getItems(samplesTable, uuid, key)
        samples = samples2
        key = key2
        println("Got: " + samples.size + " samples.")
        dist = handleSamples(sc, samples, os, model, dist)
        if (key == null)
          finished = true
      }
    }
    
    var result:spark.RDD[(String, Seq[CaratRate])] = null
    if (dist != null) {
      result = dist.map(x => {
        (x.uuid, x)
      }).groupByKey()
      //    })
    }
    result
  }

  /**
   * Process a bunch of samples, assumed to be in order by uuid and timestamp.
   * will return an RDD of CaratRates. Samples need not be from the same uuid.
   */
  def handleSamples(sc: SparkContext, samples: Seq[Map[java.lang.String, Any]], os: String, model: String, rates: RDD[CaratRate] = null) = {
    if (samples.size < 100){
      for (x <- samples)
       for (k <- x){
         if (k._2.isInstanceOf[Seq[String]])
               println("("+k._1 + ", length=" + k._2.asInstanceOf[Seq[String]].size+")")
         else
           println(k)
       }
    }
    var rateRdd = sc.parallelize[CaratRate]({
      val mapped = samples.map(x => {
        /* See properties in package.scala for data keys. */
        val uuid = x.get(sampleKey).getOrElse("").toString()
        val apps = x.get(sampleProcesses).getOrElse(Seq[String]()).asInstanceOf[Seq[String]].map(x => {
          if (x == null)
            ""
          else {
            val s = x.split(";")
            if (s.size > 1)
              s(1)
            else
              ""
          }
        })
        
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
    var apps:Seq[String] = Array[String]()
    var batt = 0.0
    var unplugged = false
    var pluggedIn = false

    for (k <- observations) {
      d = k._2.toDouble
      batt = k._3.toDouble
      events = new ArrayBuffer[String]
      events ++= k._4.trim().toLowerCase().split(" ")
      events += k._5
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

  def distributionFilter(rates: (String, Seq[CaratRate]), filter: CaratRate => Boolean) = {
    (rates._1, rates._2.filter(filter))
  }

  def appFilter(rate: CaratRate, app: String) = rate.apps1.contains(app) || rate.apps2.contains(app)

  def negativeAppFilter(rate: CaratRate, app: String) = !appFilter(rate, app)

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

  def analyzeRateData(rateData: RDD[(String, Seq[CaratRate])],  uuids: Set[String], oses: Set[String], models: Set[String]) {
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
  /* debug
    val coll = rateData.collect()
    for (k <- coll)
      println("Rate: " + k._1 + " -> "+ k._2)
      */
    for (os <- oses) {
      val fromOs = rateData.map(distributionFilter(_, _.os == os))
      val notFromOs = rateData.map(distributionFilter(_, _.os != os))

      writeTriplet(fromOs, notFromOs, osTable, osKey, os)
    }

    for (model <- models) {
      val fromModel = rateData.map(distributionFilter(_, _.model == model))
      val notFromModel = rateData.map(distributionFilter(_, _.model != model))

      writeTriplet(fromModel, notFromModel, modelsTable, modelKey, model)
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
      
      val notFromUuid = rateData.filter(_._1 != uuid)

      writeTriplet(fromUuid, notFromUuid, resultsTable, resultKey, uuid, uuidApps)

      for (app <- allApps) {
        val appFromUuid = fromUuid.map(distributionFilter(_, appFilter(_, app)))
        val appNotFromUuid = notFromUuid.map(distributionFilter(_, appFilter(_, app)))
        writeTriplet(appFromUuid, appNotFromUuid, bugsTable, (resultKey, appKey), (uuid, app))
      }
      similarApps(fromUuid, notFromUuid, uuid, uuidApps)
    }

    for (app <- allApps) {
      val filtered = rateData.map(distributionFilter(_, appFilter(_, app)))
      val filteredNeg = rateData.map(distributionFilter(_, negativeAppFilter(_, app)))
      writeTriplet(filtered, filteredNeg, appsTable, appKey, app)
    }
  }
  
  def similarApps(mine: RDD[(String, Seq[CaratRate])], others: RDD[(String, Seq[CaratRate])], uuid: String, uuidApps: Set[String]) {
    val sCount = similarityCount(uuidApps.size)
    val similarOthers = others.map(distributionFilter(_, _.getAllApps().intersect(uuidApps).size >= sCount))
     writeTriplet(mine, similarOthers, similarsTable, similarKey, uuid)
  }

  def writeTriplet(one: RDD[(String, Seq[CaratRate])], two: RDD[(String, Seq[CaratRate])], table: String, keyNames: (String, String), keyValues: (String, String)) {
    // probability distribution: r, count/sumCount

    /* Figure out max x value (maximum rate) and bucket y values of 
       * both distributions into n buckets, averaging inside a bucket
       */
    val probOne = prob(one)
    val probTwo = prob(two)

    val values = flatten(probOne)
    val others = flatten(probTwo)

    println("prob1.size=" + values.size + " prob2.size=" + others.size + " " + keyNames + "-" + keyValues)
    if (values.size > 0 && others.size > 0) {

      val distance = {
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
        getDistance(cumulative, cumulativeNeg)
      }

      val (maxX, bucketed, bucketedNeg) = bucketDistributions(values, others)

      DynamoDbEncoder.putBug(table, keyNames, keyValues, maxX, bucketed, bucketedNeg, distance)
    }
  }

   def writeTriplet(one: RDD[(String, Seq[CaratRate])], two: RDD[(String, Seq[CaratRate])], table: String, keyName: String, keyValue: String, uuidApps: Set[String] = new HashSet[String]) {
    // probability distribution: r, count/sumCount

    /* Figure out max x value (maximum rate) and bucket y values of 
       * both distributions into n buckets, averaging inside a bucket
       */
    val probOne = prob(one)
    val probTwo = prob(two)

    val values = flatten(probOne)
    val others = flatten(probTwo)

    println("prob.size=" + values.size + " prob2.size=" + others.size + " " + keyName + "-" + keyValue)
    if (values.size > 0 && others.size > 0) {
      val distance = {
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
        getDistance(cumulative, cumulativeNeg)
      }

      val (maxX, bucketed, bucketedNeg) = bucketDistributions(values, others)
      DynamoDbEncoder.put(table, keyName, keyValue, maxX, bucketed, bucketedNeg, distance, uuidApps.toSeq)
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
      if (one.head._1 < two.head._1) {
        smaller = two
        bigger = one
      }
    }

    //println("one.size=" + one.size + " two.size=" + two.size)

    var smallIter = smaller.iterator
    for (k <- bigger) {
      var distance = 0.0

      while (smallIter.hasNext && nextTwo._1 < k._1) {
        nextTwo = smallIter.next
        //println("nextTwo._1=" + nextTwo._1 + " k._1=" + k._1)
        if (nextTwo._1 < k._1)
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
