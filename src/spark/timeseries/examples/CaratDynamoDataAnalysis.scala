package spark.timeseries.examples

import spark._
import spark.SparkContext._
import spark.timeseries._
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import java.util.Date
import java.io.FileWriter
import scala.collection.Seq
import scala.collection.mutable.Set
import scala.collection.mutable.HashSet
import scala.collection.immutable.TreeMap
import scala.collection.immutable.SortedMap
import java.io.ObjectOutputStream
import com.amazonaws.services.dynamodb.model.AttributeValue
import com.amazonaws.services.dynamodb.model.Key
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
    exit(0)
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

    val allUuids: Set[String] = {
      val result = new HashSet[String]
      var finished = false

      var (key, regs) = DynamoDbDecoder.getAllItems(DynamoDbEncoder.registrationTable)
      result ++= regs.map(_.get(DynamoDbEncoder.regsUuid).getS())
      if (key == null)
        finished = true
      while (!finished) {
        println("Continuing from key=" + key)
        var (key2, regs2) = DynamoDbDecoder.getAllItems(DynamoDbEncoder.registrationTable, key)
        regs = regs2
        key = key2
        println("Got: " + regs.size + " registrations.")
        result ++= regs.map(_.get("uuid").getS())
        if (key == null)
          finished = true
      }
      result
    }

    var allData:spark.RDD[(String, Seq[spark.timeseries.examples.CaratRate])] = null
    
    var finished = false
    var (key, regs) = DynamoDbDecoder.getAllItems(DynamoDbEncoder.registrationTable)
    println("Got: " + regs.size + " registrations.")
    allData = handleRegs(sc, regs, allUuids)
    if (key == null)
      finished = true
    while (!finished) {
      println("Continuing from key=" + key)
      var (key2, regs2) = DynamoDbDecoder.getAllItems(DynamoDbEncoder.registrationTable, key)
      regs = regs2
      key = key2
      println("Got: " + regs.size + " registrations.")
      if (allData != null)
        allData = allData.union(handleRegs(sc, regs, allUuids))
      if (key == null)
        finished = true
    }
    if (allData != null)
      analyzeRateData(allData)
  }

  def handleRegs(sc: SparkContext, regs: Seq[java.util.Map[String, com.amazonaws.services.dynamodb.model.AttributeValue]], allUuids: Set[String]) = {
    /* FIXME: I would like to do this in parallel, but that would not let me re-use
     * all the data for the other uuids, resulting in n^2 execution time.
     */
    
    //val parallel = sc.parallelize[java.util.Map[String, com.amazonaws.services.dynamodb.model.AttributeValue]](regs)
    //parallel.foreach(x => {
    
    var dist: spark.RDD[spark.timeseries.examples.CaratRate] = null
    for (x <- regs) {
      /*
       * Data format guess:
       * Registration(uuId:0FC81205-55D0-46E5-8C80-5B96F17B5E7B, platformId:iPhone Simulator, systemVersion:5.0)
       */
      val uuid = x.get(DynamoDbEncoder.regsUuid).getS()
      val model = x.get(DynamoDbEncoder.regsModel).getS()
      val os = x.get(DynamoDbEncoder.regsOs).getS()

      /* 
       * FIXME: With incremental processing, the LAST sample or a few last samples
       * (as many as have a zero battery drain) should be re-used in the next batch. 
       */
      var finished = false
      var (key, samples) = DynamoDbDecoder.getItems(DynamoDbEncoder.samplesTable, uuid)
      println("Got: " + samples.size + " samples.")
      dist = handleSamples(sc, samples, os, model)
      if (key == null)
        finished = true
      while (!finished) {
        println("Continuing samples from key=" + key)
        var (key2, samples2) = DynamoDbDecoder.getItems(DynamoDbEncoder.samplesTable, uuid, key)
        samples = samples2
        key = key2
        println("Got: " + samples.size + " samples.")
        dist = handleSamples(sc, samples, os, model, dist)
        if (key == null)
          finished = true
      }
    }
    var result:spark.RDD[(String, Seq[spark.timeseries.examples.CaratRate])] = null
    if (dist != null) {
      result = dist.map(x => {
        (x.uuid, x)
      }).groupByKey()
      //    })
    }
    result
  }

  def handleSamples(sc: SparkContext, samples: Seq[java.util.Map[java.lang.String, com.amazonaws.services.dynamodb.model.AttributeValue]], os: String, model: String, rates: RDD[CaratRate] = null) = {
    var rateRdd = sc.parallelize[CaratRate]({
      val mapped = samples.map(x => {
      /*
       * Data format guess:
       * 
       * Sample(uuId:0ACCBB95-06DC-4C06-9B96-2D1BF8E8576E, timestamp:1.327476628456556E9,
       * piList:[ProcessInfo(pId:3030160, pName:lockdownd), ProcessInfo(pId:3029824, pName:imagent), ProcessInfo(pId:3031904, pName:aggregated),
       * ProcessInfo(pId:3032048, pName:absinthed.N88), ProcessInfo(pId:3032432, pName:notifyd), ProcessInfo(pId:3032608, pName:lsd),
       * ProcessInfo(pId:3032784, pName:fseventsd), ProcessInfo(pId:3032960, pName:Music~iphone), ProcessInfo(pId:2480128, pName:kernel_task),
       * ProcessInfo(pId:3032944, pName:Carat), ProcessInfo(pId:3033488, pName:aosnotifyd), ProcessInfo(pId:3033664, pName:configd),
       * ProcessInfo(pId:3033840, pName:syslogd), ProcessInfo(pId:3034016, pName:networkd), ProcessInfo(pId:3034192, pName:pasteboardd),
       * ProcessInfo(pId:3034368, pName:fairplayd.N88), ProcessInfo(pId:3034544, pName:sandboxd), ProcessInfo(pId:3034768, pName:lockbot),
       * ProcessInfo(pId:3034928, pName:Dusk), ProcessInfo(pId:3035104, pName:CommCenterClassi), ProcessInfo(pId:3035248, pName:MobileMail),
       * ProcessInfo(pId:3035408, pName:powerd), ProcessInfo(pId:3035568, pName:amfid), ProcessInfo(pId:3035744, pName:SpringBoard),
       * ProcessInfo(pId:3035904, pName:iapd), ProcessInfo(pId:3036080, pName:notification_pro), ProcessInfo(pId:2455168, pName:launchd),
       * ProcessInfo(pId:3036416, pName:mediaremoted), ProcessInfo(pId:3036576, pName:wifid), ProcessInfo(pId:3036944, pName:dataaccessd),
       * ProcessInfo(pId:3030608, pName:locationd), ProcessInfo(pId:3037184, pName:springboardservi), ProcessInfo(pId:3037360, pName:vampires),
       * ProcessInfo(pId:3037536, pName:debugserver), ProcessInfo(pId:3037712, pName:librariand), ProcessInfo(pId:3037888, pName:installd),
       * ProcessInfo(pId:2831616, pName:UserEventAgent), ProcessInfo(pId:3038224, pName:Pandora), ProcessInfo(pId:3038400, pName:syslog_relay),
       * ProcessInfo(pId:3038560, pName:awdd), ProcessInfo(pId:3038736, pName:mediaserverd), ProcessInfo(pId:3038912, pName:mDNSResponder),
       * ProcessInfo(pId:3039088, pName:BTServer), ProcessInfo(pId:3039264, pName:notification_pro), ProcessInfo(pId:3039424, pName:apsd),
       * ProcessInfo(pId:3039584, pName:ptpd), ProcessInfo(pId:3039760, pName:MobilePhone), ProcessInfo(pId:3039920, pName:ubd)],
       * batteryState:Charging, batteryLevel:0.800000011920929,
       * memoryWired:3003104, memoryActive:2870928, memoryInactive:2808624, memoryFree:2769920, memoryUser:2875632,
       * triggeredBy:applicationDidBecomeActive)
       */
        
        val uuid = x.get(DynamoDbEncoder.sampleKey).getS()
        val apps = x.get(DynamoDbEncoder.sampleProcesses).getSS().map(x => {
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
        val time = x.get(DynamoDbEncoder.sampleTime).getS()
        val batteryState = x.get(DynamoDbEncoder.sampleBatteryState).getS()
        val batteryLevel = x.get(DynamoDbEncoder.sampleBatteryLevel).getN()
        val event = x.get(DynamoDbEncoder.sampleEvent).getS()
        (uuid, time, batteryLevel, event, batteryState, apps)
      })
      rateMapper(os, model, mapped)
    })
    if (rates != null)
      rateRdd = rates.union(rateRdd)
    rateRdd
  }

  def rateMapper(os: String, model: String, observations: Seq[(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, Seq[String])]) = {
    // (uuid, time, batteryLevel, event, batteryState, apps)
    var prevD = 0L
    var prevBatt = 0.0
    var prevEvents = new HashSet[String]()
    var prevApps = new HashSet[String]()
    
    val charge = "charging"
    val discharge = "unplugged" 

    var rates = new ArrayBuffer[CaratRate]

    var d = 0L
    var events = ArrayBuffer[String]()
    var apps:Seq[String] = Array[String]()
    var batt = 0.0
    var unplugged = false
    var pluggedIn = false

    for (k <- observations) {
      d = k._2.toLong
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

  def analyzeRateData(rateData: RDD[(String, Seq[CaratRate])]) {
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

    val oses = rateData.map(x => {
      var buf = new HashSet[String]
      buf ++= x._2.map(_.os)
      buf
    }).collect()
    var allOses = new HashSet[String]
    for (k <- oses)
      allOses ++= k

    for (os <- allOses) {
      val fromOs = rateData.map(distributionFilter(_, _.os == os))
      val notFromOs = rateData.map(distributionFilter(_, _.os != os))

      writeTriplet(fromOs, notFromOs, DynamoDbEncoder.osTable, DynamoDbEncoder.osKey, os)
    }

    val models = rateData.map(x => {
      var buf = new HashSet[String]
      buf ++= x._2.map(_.model)
      buf
    }).collect()

    var allModels = new HashSet[String]
    for (k <- models)
      allModels ++= k

    for (model <- allModels) {
      val fromModel = rateData.map(distributionFilter(_, _.model == model))
      val notFromModel = rateData.map(distributionFilter(_, _.model != model))

      writeTriplet(fromModel, notFromModel, DynamoDbEncoder.modelsTable, DynamoDbEncoder.modelKey, model)
    }

    val uuids = rateData.map(_._1).collect()
    for (uuid <- uuids) {

      /*
       * TODO: if there are other combinations with uuid, they go into this loop
       */
      val fromUuid = rateData.filter(_._1 == uuid)
      val notFromUuid = rateData.filter(_._1 != uuid)

      writeTriplet(fromUuid, notFromUuid, DynamoDbEncoder.resultsTable, DynamoDbEncoder.resultKey, uuid + "")

      for (app <- allApps) {
        val appFromUuid = fromUuid.map(distributionFilter(_, appFilter(_, app)))
        val appNotFromUuid = notFromUuid.map(distributionFilter(_, appFilter(_, app)))
        writeTriplet(appFromUuid, appNotFromUuid, DynamoDbEncoder.bugsTable, (DynamoDbEncoder.resultKey, DynamoDbEncoder.appKey), (uuid + "", app))
      }
    }

    for (app <- allApps) {
      val filtered = rateData.map(distributionFilter(_, appFilter(_, app)))
      val filteredNeg = rateData.map(distributionFilter(_, negativeAppFilter(_, app)))
      writeTriplet(filtered, filteredNeg, DynamoDbEncoder.appsTable, DynamoDbEncoder.appKey, app)
    }
  }

  def writeTriplet(one: RDD[(String, Seq[CaratRate])], two: RDD[(String, Seq[CaratRate])], table: String, keyNames: (String, String), keyValues: (String, String)) {
    // probability distribution: r, count/sumCount

    /* TODO: Figure out max x value (maximum rate) and bucket y values of 
       * both distributions into n buckets, averaging inside a bucket
       */
    val probOne = prob(one)
    val probTwo = prob(two)

    val values = flatten(probOne)
    val others = flatten(probTwo)

    println("prob.size=" + values.size + " prob2.size=" + others.size + " " + keyNames + "-" + keyValues)
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

      // TODO: Normalize x range here
      val (maxX, bucketed, bucketedNeg) = bucketDistributions(values, others)

      DynamoDbEncoder.putBug(table, keyNames, keyValues, maxX, bucketed, bucketedNeg, distance)
    }
  }

  def writeTriplet(one: RDD[(String, Seq[CaratRate])], two: RDD[(String, Seq[CaratRate])], table: String, keyName: String, keyValue: String) {
    // probability distribution: r, count/sumCount

    /* TODO: Figure out max x value (maximum rate) and bucket y values of 
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

      DynamoDbEncoder.put(table, keyName, keyValue, maxX, bucketed, bucketedNeg, distance)
    }
  }

  def bucketDistributions(values: Array[(Double, Double)], others: Array[(Double, Double)]) = {
    /*maxX defines the buckets. Each bucket is
     * k /100 * maxX to k+1 / 100 * maxX.
     * Therefore, do not store the bucket starts and ends, only bucket numbers from 0 to 99.*/
    val bucketed = new ArrayBuffer[(Int, Double)]
    val bucketedNeg = new ArrayBuffer[(Int, Double)]

    val maxX = Math.max(values.last._1, others.last._1)
    // TODO: Bucket x ranges here
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
    result = Math.round(result * mul)
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
      if (one.first._1 < two.first._1) {
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

  def flatten(filtered: RDD[(String, TreeMap[Double, Double])]) = {
    // there are x treemaps. We need to flatten them but include the uuid.
    filtered.flatMap(x => {
      var result = new TreeMap[Double, Double]
      for (k <- x._2)
        result += ((k._1, k._2))
      result
    }).collect()
  }
}
