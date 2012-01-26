package edu.berkeley.cs.amplab.carat

import spark._
import spark.SparkContext._
import spark.timeseries._
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import scala.collection.Seq
import scala.collection.mutable.HashSet
import scala.collection.immutable.TreeMap

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

object PowerMonToDynamoDataAnalysis {

  val results = "carat.latestresults"
  val bugs = "carat.latestbugs"
  val appsTable = "carat.latestapps"
  val modelsTable = "carat.latestmodels"
  val osTable = "carat.latestos"

  val uuidKey = "uuId"
  val appKey = "appName"
    val osKey = "os"
      val modelKey = "model"
    
  val buckets = 100
  val DECIMALS = 3

  def uidMapper(x: String) = {
    val arr = x.trim().split(",[ ]*")
    val dfs = "EEE MMM dd HH:mm:ss zzz yyyy"
    val df = new SimpleDateFormat(dfs)
    val key = arr(1) toInt
    val date = df.parse(arr(0)).getTime()
    var dest = new Array[String](arr.length - 1)
    Array.copy(arr, 1, dest, 0, arr.length - 1)
    (key, (date, dest))
  }

  def rateMapper(observations: Seq[(Long, Array[String])]) = {
    var prevD = 0L
    var prevBatt = 0.0
    var prevEvents = new HashSet[String]
    var prevApps = new HashSet[String]

    var rates = new ArrayBuffer[CaratRate]

    var d = 0L
    var events = Array[String]()
    var apps = Array[String]()
    var batt = 0.0
    var unplugged = false
    var pluggedIn = false

    for (k <- observations) {
      d = k._1
      batt = k._2(1).toDouble
      events = k._2(2).trim().toLowerCase().split(" ")
      apps = new Array[String](k._2.length - 3)
      Array.copy(k._2, 3, apps, 0, k._2.length - 3)

      if (events.contains("unplugged")) {
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
            // TODO: these should come from the reg msg.
            val (os, model) = {
            if (k._2(0) == "85")
              ("5.0.1", "iPhone 4S")
            if (k._2(0) == "46")
              ("7.0.1RC1", "LG Optimus 2X") 
            else
              ("5.0.1", "iPhone")
            }
              
            rates += new CaratRate(k._2(0), os, model, prevD, d, prevBatt, batt,
              prevEvents.toArray, events, prevApps.toArray, apps)
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
        if ((k == observations.last && !pluggedIn) || (!pluggedIn && events.contains("pluggedin"))) {
          if (prevD != d) {
            val (os, model) = {
              val k = observations.last
            if (k._2(0) == "85")
              ("5.0.1", "iPhone 4S")
            if (k._2(0) == "46")
              ("7.0.1RC1", "LG Optimus 2X") 
            else
              ("5.0.1", "iPhone")
            }
            rates += new CaratRate(observations.last._2(0), os, model, prevD, d, prevBatt, batt,
              prevEvents.toArray, events, prevApps.toArray, apps)
          }
        }

        if (!pluggedIn) {
          prevApps ++= apps
          prevEvents ++= events
        }

        if (events.contains("pluggedin")) {
          pluggedIn = true
          unplugged = false
          prevD = 0
          prevBatt = 0
        }
      }
    }
    // last one:

    rates.toSeq
  }

  def separateUidTimeSeries(sc: SparkContext, dumpsysFile: String) = {

    val file = sc.textFile(dumpsysFile)
    //file.map(mapper).groupByKey()
    file.map(uidMapper).groupByKey()
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
  def getRates(sc: SparkContext, caratDataFile: String) = {
    val uidData = separateUidTimeSeries(sc, caratDataFile)
    uidData.mapValues(rateMapper)
  }

  def distributionFilter(rates: (Int, Seq[CaratRate]), filter: CaratRate => Boolean) = {
    (rates._1, rates._2.filter(filter))
  }

  def appFilter(rate: CaratRate, app: String) = rate.apps1.contains(app) || rate.apps2.contains(app)

  def negativeAppFilter(rate: CaratRate, app: String) = !appFilter(rate, app)

  /**
   * Main program entry point. Parses the files given on the command line and saves results as
   * `dumpsys-script-battery-level.txt` and `powermon-file-battery-level.txt`.
   */
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: S3DataAnalysis master caratDataFile.txt\n" +
        "Example: S3DataAnalysis local[1] caratDataFile.txt")
      return
    }
    val sc = new SparkContext(args(0), "CaratDataAnalysis")
    val caratDataFile = args(1)

    var dot = caratDataFile.lastIndexOf('.')
    if (dot < 1)
      dot = caratDataFile.length

    val caratFreq = caratDataFile.substring(0, dot) + "-frequency"

    analyzeRateData(getRates(sc, caratDataFile), caratFreq)
    sys.exit(0)
  }

  def prob(rates: RDD[(Int, Seq[CaratRate])]) = {
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

  def analyzeRateData(rateData: RDD[(Int, Seq[CaratRate])], caratFreq: String) {
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

      writeTriplet(fromOs, notFromOs, osTable, osKey, os)
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

      writeTriplet(fromModel, notFromModel, modelsTable, modelKey, model)
    }
   
    val uuids = rateData.map(_._1).collect()
    for (uuid <- uuids) {

      /*
       * TODO: if there are other combinations with uuid, they go into this loop
       */
      val fromUuid = rateData.filter(_._1 == uuid)
      val notFromUuid = rateData.filter(_._1 != uuid)

      writeTriplet(fromUuid, notFromUuid, results, uuidKey, uuid + "")

      for (app <- allApps) {
        val appFromUuid = fromUuid.map(distributionFilter(_, appFilter(_, app)))
        val appNotFromUuid = notFromUuid.map(distributionFilter(_, appFilter(_, app)))
        writeTriplet(appFromUuid, appNotFromUuid, bugs, (uuidKey, appKey), (uuid + "", app))
      }
    }

    for (app <- allApps) {
      val filtered = rateData.map(distributionFilter(_, appFilter(_, app)))
      val filteredNeg = rateData.map(distributionFilter(_, negativeAppFilter(_, app)))
      writeTriplet(filtered, filteredNeg, appsTable, appKey, app)
    }
  }

  def writeTriplet(one: RDD[(Int, Seq[CaratRate])], two: RDD[(Int, Seq[CaratRate])], table: String, keyNames: (String, String), keyValues: (String, String)) {
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

  def writeTriplet(one: RDD[(Int, Seq[CaratRate])], two: RDD[(Int, Seq[CaratRate])], table: String, keyName: String, keyValue: String) {
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
      
      val maxX = math.max(values.last._1, others.last._1)
      // TODO: Bucket x ranges here
      var valueIndex = 0
      var othersIndex = 0
      for (k <- 0 until buckets){
        val start = maxX / buckets * k
        val end = maxX / buckets * (k+1)
        var sumV = 0.0
        var sumO = 0.0
        var countV = valueIndex
        var countO = valueIndex
        while (valueIndex < values.size && values(valueIndex)._1 >= start && values(valueIndex)._1 < end){
          sumV += values(valueIndex)._2
          valueIndex+=1
        }
        countV = valueIndex - countV
        while (othersIndex < others.size && others(othersIndex)._1 >= start && others(othersIndex)._1 < end){
          sumO += others(othersIndex)._2
          othersIndex+=1
        }
        countO = othersIndex - countO
        if (countV > 0){
          bucketed += ((k, nDecimal(sumV / countV)))
      }else
          bucketed += ((k, 0.0))
        if (countO > 0){
          bucketedNeg += ((k, nDecimal(sumO / countO)))
  } else
          bucketedNeg += ((k, 0.0))
      }
    
    (maxX, bucketed, bucketedNeg)
  }
  
  def nDecimal(orig:Double) = {
    var result = orig
    var mul = 1
    for (k <- 0 until DECIMALS)
      mul *= 10
    result = math.round(result*mul)
    result/mul
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

  def flatten(filtered: RDD[(Int, TreeMap[Double, Double])]) = {
    // there are x treemaps. We need to flatten them but include the uuid.
    filtered.flatMap(x => {
      var result = new TreeMap[Double, Double]
      for (k <- x._2)
        result += ((k._1, k._2))
      result
    }).collect()
  }
}
