package spark.timeseries.examples

import spark._
import spark.SparkContext._
import spark.timeseries._
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import java.util.Date
import java.io.FileWriter
import scala.collection.mutable.HashSet
import scala.collection.immutable.TreeMap
import scala.collection.immutable.SortedMap

/**
 * A program for calculating the rates, sorting them, and calculating their cumulative probability distribution
 * from a Carat data file of the format:
 * {{{
 * time, uid, battery level, space separated events, running apps
 * }}}
 * For example:
 * {{{
 Mon Dec 26 09:43:36 PST 2011, 46, 99, batteryStatusChanged unplugged, Safari, Mail 
 Mon Dec 26 12:13:11 PST 2011, 46, 80, batteryStatusChanged, Safari, Mail, Angry Birds
 Mon Dec 26 09:51:00 PST 2011, 86, 100, batteryStatusChanged unplugged, Safari, Mail
 Mon Dec 26 17:16:00 PST 2011, 46, 66, batteryStatusChanged, Safari, Mail, Amazon Kindle
 Mon Dec 26 12:23:11 PST 2011, 86, 68, batteryStatusChanged, Safari, Mail, Angry Birds, Opera
 Mon Dec 26 16:11:00 PST 2011, 86, 65, batteryStatusChanged, Safari, Mail, Opera
 Mon Dec 26 23:20:12 PST 2011, 46, 53, batteryStatusChanged pluggedIn usbPower, Safari, Mail
 Mon Dec 27 01:10:01 PST 2011, 86, 20, batteryStatusChanged pluggedIn, Safari, Mail
 * }}}
 * 
 * Usage:
 * {{{
 *  CaratFakeDataAnalysis.main(Array("fakedatafile.txt"))
 * }}}
 * The results will be called dumpsys-script-data.txt and powermon-file-data.txt.
 */

object CaratDataAnalysis{

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
    var prevEvents = new HashSet[String]()
    var prevApps = new HashSet[String]()
    
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
      
      if (events.contains("unplugged")){
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
        if (!pluggedIn){
          // take periods where battery life has changed
          if (batt - prevBatt >= 1 || prevBatt - batt >= 1) {
            rates += new CaratRate(k._2(0), "1.0", "FakeFone", prevD, d, prevBatt, batt,
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
            rates += new CaratRate(observations.last._2(0), "1.0", "FakeFone", prevD, d, prevBatt, batt,
              prevEvents.toArray, events, prevApps.toArray, apps)
          }
        }
        
        if (!pluggedIn){
          prevApps ++= apps
          prevEvents ++= events
        }
        
        if (events.contains("pluggedin")){
          pluggedIn = true
          unplugged = false
          prevD = 0
          prevBatt = 0
        }
      }
    }
    // last one:
    
    rates
  }
  
  def separateUidTimeSeries(sc: SparkContext, dumpsysFile: String) = {
    val file = sc.textFile(dumpsysFile)
    //file.map(mapper).groupByKey()
    file.map(uidMapper).groupByKey()
  }
  
  
/**
  We will not be building distributions for all possible groups of apps.
I don't even think that would be tractable for very long. We need
distributions for (off the top of my head):
1) App X is running
2) App X is not running
3) App X is running on uuid U
4) App X is running on uuid != U
5) OS Version == V
6) OS Version != V
7) Device Model == M
8) Device Model != M
9) uuid == U
10) uuid != U

We also need to compute distributions for users that run "similar"
apps. I am open to suggestions for how to do this efficiently while
still providing a number that means something.
*/
  
  def getRates(sc: SparkContext, caratDataFile:String) = {
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
    `dumpsys-script-battery-level.txt` and `powermon-file-battery-level.txt`.
   */
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: CaratDataAnalysis master caratDataFile.txt\n"+
        "Example: CaratDataAnalysis local[1] caratDataFile.txt")
      return
    }
    val sc = new SparkContext(args(0), "CaratDataAnalysis")
    val caratDataFile = args(1)

    var dot = caratDataFile.lastIndexOf('.')
    if (dot < 1)
    dot = caratDataFile.length
    
    val caratFreq = caratDataFile.substring(0,dot)+"-frequency"
    
    analyzeRateData(getRates(sc, caratDataFile), caratFreq)
    sys.exit(0)
  }
  
  def prob(rates: RDD[(Int, Seq[CaratRate])]) = {
    rates.mapValues(x => {
        var sum = 0.0
        var buf = new TreeMap[Double, Double]
        for (d <- x){
          var count = buf.get(d.rate)
          buf += ((d.rate,count.getOrElse(0.0)+1.0))
          sum+= 1
        }
        
        for (k <- buf)
          buf += ((k._1, k._2 / sum))
          
        buf
      })
  }

  def probArray(rates: RDD[(Int, ArrayBuffer[CaratRate])]) = {
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
  
  def analyzeRateData(rateData: RDD[(Int, ArrayBuffer[CaratRate])], caratFreq:String) {
    val apps = rateData.map(x => {
      var buf = new HashSet[String]
      for (k <- x._2){
        buf ++= k.getAllApps()
      }
      buf
    }).collect()
    
    var allApps = new HashSet[String]
    for (k <- apps)
      allApps ++= k
      
    val uuids = rateData.map(_._1).collect()
    for (uuid <- uuids){
      /*
       * TODO: if there are other combinations with uuid, they go into this loop
       */
      val fromUuid = rateData.filter(_._1 == uuid)
      val notFromUuid = rateData.filter(_._1 != uuid)
      
      val probFromUuid = probArray(fromUuid)
      val probNotFromUuid = probArray(notFromUuid)
      
      writeProbs(probFromUuid, caratFreq, "-uuid=" + uuid)
      writeProbs(probNotFromUuid, caratFreq, "-not-uuid=" + uuid)
      /* bug detection: app x and uuid
       * appx and not uuid 
       */
      cumulative(probFromUuid, probNotFromUuid, caratFreq, "-uuid="+uuid, "-not-uuid="+uuid)
       
      for (app <- allApps){
        val appFromUuid = prob(fromUuid.map(distributionFilter(_, appFilter(_, app))))
        val appNotFromUuid = prob(notFromUuid.map(distributionFilter(_, appFilter(_, app))))

        writeProbs(appFromUuid, caratFreq, "-bug-uuid=" + uuid + "-app="+app)
        writeProbs(appNotFromUuid, caratFreq, "-bug-not-uuid=" + uuid+"-app="+app)
        
        cumulative(appFromUuid, appNotFromUuid, caratFreq, "-bug-uuid=" + uuid + "-app="+app, "-bug-not-uuid=" + uuid+"-app="+app)
      }
    }
    
    for (app <- allApps){
      val filtered = rateData.map(distributionFilter(_, appFilter(_, app)))
      val filteredNeg = rateData.map(distributionFilter(_, negativeAppFilter(_, app)))
      
      // absolute values: r, count (not actually needed; just for debugging)
     // writeValues(filtered, caratFreq, "-app=" + app)
     // writeValues(filteredNeg, caratFreq, "-not-app=" + app)
      
      // probability distribution: r, count/sumCount
      val probs = prob(filtered)
      val probsNeg = prob(filteredNeg)
      
      writeProbs(probs, caratFreq + "-prob", "-app=" + app)
      writeProbs(probsNeg, caratFreq +"-prob", "-not-app=" + app)

      // cumulative distribution: r, Sum(count(0) to count(k))/sumCount
      
      /* TODO: Figure out max x value (maximum rate) and bucket y values of 
       * both distributions into n buckets, averaging inside a bucket
       */
      
      cumulative(probs, probsNeg, caratFreq, "-app=" + app, "-not-app=" + app)
    }
  }
  
  def cumulative(one: RDD[(Int, TreeMap[Double, Double])], two: RDD[(Int, TreeMap[Double, Double])], caratFreq:String, nameOne: String, nameTwo: String) {
   val cumulative = one.mapValues(x => {
        var sum = 0.0
        var buf = new TreeMap[Double, Double]
        for (d <- x) {
          sum += d._2
          buf += ((d._1, sum))
        }
        buf
      })

      val cumulativeNeg = two.mapValues(x => {
        var sum = 0.0
        var buf = new TreeMap[Double, Double]
        for (d <- x) {
          sum += d._2
          buf += ((d._1, sum))
        }
        buf
      })
      
      writeProbs(cumulative, caratFreq + "-prob-c", nameOne)
      writeProbs(cumulativeNeg, caratFreq +"-prob-c", nameTwo)
      println("cumulative1.size=" + cumulative.count() + " two.size=" + cumulativeNeg.count() + " " + nameOne+"-"+nameTwo)
      
      val distance = getDistances(cumulative, cumulativeNeg, nameOne+"-"+nameTwo)
      writeDistance(distance, caratFreq + "-distance", nameOne+"-"+nameTwo)
  }

  def getDistances(one: RDD[(Int, TreeMap[Double, Double])], two: RDD[(Int, TreeMap[Double, Double])], debug: String) = {
      // FIXME: use of collect may cause memory issues.
      var oneFlat = one.flatMap(x => {
        var result = new TreeMap[Double, Double]
        for (k <- x._2)
          result += ((k._1, k._2))
        result
      }).collect()

      var twoFlat = two.flatMap(x => {
        var result = new TreeMap[Double, Double]
        for (k <- x._2)
          result += ((k._1, k._2))
        result
      }).collect()
      
    
    var maxDistance = -2.0
      var prevOne = (-2.0, 0.0)
      var prevTwo = (-2.0, 0.0)
      var nextTwo = prevTwo
      
      var smaller = oneFlat
      var bigger = twoFlat

      /* Swap if the above assignment was not the right guess: */
      if (oneFlat.size > 0 && twoFlat.size > 0) {
        if (oneFlat.head._1 < twoFlat.head._1) {
          smaller = twoFlat
          bigger = oneFlat
        }
      }
      
      println("one.size=" + oneFlat.size + " two.size=" + twoFlat.size + " " + debug)
      
      var smallIter = smaller.iterator
      for (k <- bigger){
        var distance = 0.0
        
        while (smallIter.hasNext && nextTwo._1 < k._1){
          nextTwo = smallIter.next
          println("nextTwo._1=" + nextTwo._1 + " k._1=" + k._1)
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
  
  def getDistancesBuggy(one: RDD[(Int, TreeMap[Double, Double])], two: RDD[(Int, TreeMap[Double, Double])], debug:String) = {
    // FIXME: this does not work, because values are joined by key.
    // in bug detection, the key is different, so the joins only have one side.
    var joined = one.join(two)
    
    joined.mapValues(x => {
      var maxDistance = -2.0
      var prevOne = (-2.0, 0.0)
      var prevTwo = (-2.0, 0.0)
      var nextTwo = prevTwo
      
      var smaller = x._1
      var bigger = x._2

      /* Swap if the above assignment was not the right guess: */
      if (x._1.size > 0 && x._2.size > 0) {
        if (x._2.firstKey < x._1.firstKey) {
          smaller = x._2
          bigger = x._1
        }
      }
      
      println("one.size=" + x._1.size + " two.size=" + x._2.size + " " + debug)
      
      var smallIter = smaller.iterator
      for (k <- bigger){
        var distance = 0.0
        
        while (smallIter.hasNext && nextTwo._1 < k._1){
          nextTwo = smallIter.next
          println("nextTwo._1=" + nextTwo._1 + " k._1=" + k._1)
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
    })
  }

  def writeValues(filtered: RDD[(Int, Seq[CaratRate])], caratFreq:String, app: String) {
    val values = filtered.collect()
    var all = new ArrayBuffer[CaratRate]
    for (k <- values)
      all++= k._2
  
    val fileName = caratFreq + app + ".csv"
    val fw = new FileWriter(fileName)
    
    for (k <- all) {
      /*println("uid=" + k.uuid + " rate=" + k.rate() + " timediff=" + k.timeDiff +
        " batterydiff=" + k.batteryDiff +
        " events1=" + k.events1.mkString(";") +
        " events2=" + k.events2.mkString(";") +
        " apps=" + k.getAllApps().mkString(";"))
*/
      fw.write(k.uuid + ", " + k.rate() +
        ", " + k.events1.mkString(";") + ", " +
        k.events2.mkString(";") + ", " +
        k.getAllApps().mkString(";") + "\n")
    }
    fw.close()
  }

  def writeProbs(filtered: RDD[(Int, TreeMap[Double, Double])], caratFreq: String, app: String) {
    // there are x treemaps. We need to flatten them but include the uuid.
    val values = filtered.flatMap(x => {
      var result = new TreeMap[Double, (Double, Int)]
      for (k <- x._2)
        result += ((k._1, (k._2, x._1)))
      result
    }).collect()

    val fileName = caratFreq + app + ".csv"
    val fw = new FileWriter(fileName)
    for (k <- values) {
      //println("uuid=" + k._2._2 + ", OS=" + "iOS" + ", model=" + "4S" + ", " + k._1 + ", " + k._2._1)
      fw.write(k._2._2 + ", OS=" + "iOS" + ", model=" + "4S" + ", " + k._1 + ", " + k._2._1 + "\n")
    }
    fw.close()
  }

  def writeDistance(distance: Double, caratFreq: String, app: String) {
    val fileName = caratFreq + app + ".csv"
    val fw = new FileWriter(fileName)
      fw.write("OS=" + "iOS" + ", model=" + "4S" + ", " + distance+ "\n")
    fw.close()
  }
}
