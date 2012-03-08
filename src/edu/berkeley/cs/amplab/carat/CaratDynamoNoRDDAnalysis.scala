package edu.berkeley.cs.amplab.carat

import spark._
import spark.SparkContext._
import spark.timeseries._
import java.util.concurrent.Semaphore
import scala.collection.mutable.ArrayBuffer
import scala.collection.Seq
import scala.collection.immutable.Set
import scala.collection.immutable.HashSet
import scala.collection.immutable.TreeMap
import collection.JavaConversions._
import com.amazonaws.services.dynamodb.model.AttributeValue
import java.io.File
import java.text.SimpleDateFormat
import java.io.ByteArrayOutputStream
import com.amazonaws.services.dynamodb.model.Key
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileInputStream
import java.io.FileWriter
import java.io.FileOutputStream
import edu.berkeley.cs.amplab.carat.dynamodb.DynamoAnalysisUtil
import scala.collection.immutable.TreeSet
import edu.berkeley.cs.amplab.carat.dynamodb.DynamoDbDecoder
import scala.actors.scheduler.ResizableThreadPoolScheduler
import scala.collection.mutable.HashMap
import com.esotericsoftware.kryo.Kryo

/**
 * Do the exact same thing as in CaratDynamoDataToPlots, but do not collect() and write plot files and run plotting in the end.
 *
 * @author Eemil Lagerspetz
 */

object CaratDynamoNoRDDAnalysis {

  // How many concurrent plotting operations are allowed to run at once.
  val CONCURRENT_PLOTS = 100
  // How many clients do we need to consider data reliable?
  val ENOUGH_USERS = 5

  /*lazy val scheduler = {
    scala.util.Properties.setProp("actors.corePoolSize", CONCURRENT_PLOTS + "")
    val s = new ResizableThreadPoolScheduler(false)
    s.start()
    s
  }*/

  var DEBUG = false
  val DECIMALS = 3
  // Isolate from the plotting.
  val tmpdir = "/mnt/TimeSeriesSpark-osmodel/spark-temp-plots/"

  var userLimit = Int.MaxValue

  /**
   * Main program entry point.
   */
  def main(args: Array[String]) {
    var master = "local[16]"
    if (args != null && args.length >= 1) {
      master = args(0)
    }
    if (args != null && args.length > 1)
      userLimit = args(1).toInt
    
    val start = DynamoAnalysisUtil.start()

    // turn off INFO logging for spark:
    System.setProperty("hadoop.root.logger", "WARN,console")
    // This is misspelled in the spark jar log4j.properties:
    System.setProperty("log4j.threshhold", "WARN")
    // Include correct spelling to make sure
    System.setProperty("log4j.threshold", "WARN")

    // turn on ProbUtil debug logging
    //System.setProperty("log4j.category.spark.timeseries.ProbUtil.threshold", "DEBUG")
    //System.setProperty("log4j.appender.spark.timeseries.ProbUtil.threshold", "DEBUG")

    // Fix Spark running out of space on AWS.
    System.setProperty("spark.local.dir", tmpdir)

    //System.setProperty("spark.kryo.registrator", classOf[CaratRateRegistrator].getName)
    val sc = TimeSeriesSpark.init(master, "default", "CaratDynamoDataSpeedTest")
    // getRates
    val allRates = CaratDynamoDataToPlots.getRates(sc)
    if (allRates != null) {
      // analyze data
      analyzeRateData(allRates)
      // do not save rates in this version.
    }
    DynamoAnalysisUtil.finish(start)
  }

  /**
   * Main analysis function. Called on the entire collected set of CaratRates.
   */
  def analyzeRateData(inputRates: RDD[CaratRate]) = {
    
    // make everything non-rdd from now on
    var allRates = inputRates.collect()
    // determine oses and models that appear in accepted data and use those
    val uuidToOsAndModel = new scala.collection.mutable.HashMap[String, (String, String)]
    uuidToOsAndModel ++= allRates.map(x => { (x.uuid, (x.os, x.model)) })
    
    var uuidArray = uuidToOsAndModel.keySet.toArray.sortWith((s, t) => {
      s < t
    })
    
    if (userLimit < uuidArray.length){
      println("Running analysis for %s users.".format(userLimit))
      // limit data to these users:
      uuidArray = uuidArray.slice(0, userLimit)
      allRates = inputRates.filter(x=>{
        uuidArray.contains(x.uuid)
      }).collect()
      
    }else{
      println("Running analysis for all users.")
    }

    val oses = uuidToOsAndModel.map(_._2._1).toSet
    val models = uuidToOsAndModel.map(_._2._2).toSet

    println("uuIds with data: " + uuidToOsAndModel.keySet.mkString(", "))
    println("oses with data: " + oses.mkString(", "))
    println("models with data: " + models.mkString(", "))
    
    /**
     * uuid distributions, xmax, ev and evNeg
     * FIXME: With many users, this is a lot of data to keep in memory.
     * Consider changing the algorithm and using RDDs.
     */
    var distsWithUuid = new TreeMap[String, Array[(Double, Double)]]
    var distsWithoutUuid = new TreeMap[String, Array[(Double, Double)]]
    /* xmax, ev, evNeg */
    var parametersByUuid = new TreeMap[String, (Double, Double, Double)]
    /* evDistances*/
    var evDistanceByUuid = new TreeMap[String, Double]

    var appsByUuid = new TreeMap[String, Set[String]]

    println("Calculating aPriori.")
    val aPrioriDistribution = DynamoAnalysisUtil.getApriori(allRates)
    println("Calculated aPriori.")
    if (aPrioriDistribution.size == 0)
      println("WARN: a priori dist is empty!")
    else
      println("a priori dist:\n" + aPrioriDistribution.mkString("\n"))

    var allApps = allRates.flatMap(_.allApps).toSet
    println("AllApps (with daemons): " + allApps)
    val DAEMONS_LIST_GLOBBED = DynamoAnalysisUtil.daemons_globbed(allApps)
    allApps --= DAEMONS_LIST_GLOBBED
    println("AllApps (no daemons): " + allApps)

    println("Number of samples: %s users: %s apps: %s".format(allRates.size,uuidArray.size,allApps.size))
    
    for (os <- oses) {
      // can be done in parallel, independent of anything else
        val fromOs = allRates.filter(_.os == os)
        val notFromOs = allRates.filter(_.os != os)
        // no distance check, not bug or hog
        val ret = plotDists("iOS " + os, "Other versions", fromOs, notFromOs, aPrioriDistribution, false, null, null, null, 0, 0, null)
    }

    for (model <- models) {
      // can be done in parallel, independent of anything else
        val fromModel = allRates.filter(_.model == model)
        val notFromModel = allRates.filter(_.model != model)
        // no distance check, not bug or hog
        val ret = plotDists(model, "Other models", fromModel, notFromModel, aPrioriDistribution, false, null, null, null, 0, 0, null)
    }

    /** Calculate correlation for each model and os version with all rates */
    val (osCorrelations, modelCorrelations) = correlation("All", allRates, aPrioriDistribution, models, oses)

    //scheduler.execute({
    //var allHogs = new HashSet[String]
    //var allBugs = new HashSet[String]

    /* Hogs: Consider all apps except daemons. */
    for (app <- allApps) {
      oneApp(uuidArray, allRates, app, aPrioriDistribution, oses, models)
    }

    /*val globalNonHogs = allApps -- allHogs
    println("Non-daemon non-hogs: " + globalNonHogs)
    println("All hogs: " + allHogs)
    println("All bugs: " + allBugs)*/
    //})

    /* uuid stuff */

    for (i <- 0 until uuidArray.length) {
      // these are independent until JScores.
        val uuid = uuidArray(i)
        val fromUuid = allRates.filter(_.uuid == uuid) //.cache()

        var uuidApps = fromUuid.flatMap(_.allApps).toSet
        uuidApps --= DAEMONS_LIST_GLOBBED

        if (uuidApps.size > 0)
          similarApps(allRates, aPrioriDistribution, i, uuidApps)
        /* cache these because they will be used numberOfApps times */
        val notFromUuid = allRates.filter(_.uuid != uuid) //.cache()
        // no distance check, not bug or hog
        val (xmax, probDist, probDistNeg, ev, evNeg, evDistance) = DynamoAnalysisUtil.getDistanceAndDistributionsUnBucketed(fromUuid, notFromUuid, aPrioriDistribution)
        if (probDist != null && probDistNeg != null) {
          distsWithUuid += ((uuid, probDist))
          distsWithoutUuid += ((uuid, probDistNeg))
          parametersByUuid += ((uuid, (xmax, ev, evNeg)))
          evDistanceByUuid += ((uuid, evDistance))
        }
        appsByUuid += ((uuid, uuidApps))
    }

    // need to collect uuid stuff here:
    plotJScores(distsWithUuid, distsWithoutUuid, parametersByUuid, evDistanceByUuid, appsByUuid)

    println("Calculated global correlations: osCorrelations=%s modelCorrelations=%s".format(osCorrelations, modelCorrelations))
  }
  
  def oneApp(uuidArray:Array[String], allRates: Array[edu.berkeley.cs.amplab.carat.CaratRate], app: String,
      aPrioriDistribution: scala.collection.immutable.HashMap[Double,Double],
      oses: scala.collection.immutable.Set[String], models: scala.collection.immutable.Set[String]){
    
      val filtered = allRates.filter(_.allApps.contains(app))
      val filteredNeg = allRates.filter(!_.allApps.contains(app))

      // skip if counts are too low:
      val fCountStart = DynamoAnalysisUtil.start
      val usersWith = filtered.map(_.uuid).toSet.size

      if (usersWith >= ENOUGH_USERS) {
        val usersWithout = filteredNeg.map(_.uuid).toSet.size
        DynamoAnalysisUtil.finish(fCountStart, "clientCount")
        if (usersWithout >= ENOUGH_USERS) {
          if (plotDists("Hog " + app + " running", app + " not running", filtered, filteredNeg, aPrioriDistribution, true, filtered, oses, models, usersWith, usersWithout, null)) {
            // this is a hog

            //allHogs += app
          } else {
            // not a hog. is it a bug for anyone?
            for (i <- 0 until uuidArray.length) {
              val uuid = uuidArray(i)
              /* Bugs: Only consider apps reported from this uuId. Only consider apps not known to be hogs. */
              val appFromUuid = filtered.filter(_.uuid == uuid) //.cache()
              val appNotFromUuid = filtered.filter(_.uuid != uuid) //.cache()
              if (plotDists("Bug " + app + " running on client " + i, app + " running on other clients", appFromUuid, appNotFromUuid, aPrioriDistribution, true,
                filtered, oses, models, 0, 0, uuid)) {
                //allBugs += app
              }
            }
          }
        } else {
          println("Skipped app " + app + " for too few points in: without: %s < thresh=%s".format(usersWithout, ENOUGH_USERS))
        }
      } else {
        println("Skipped app " + app + " for too few points in: with: %s < thresh=%s".format(usersWith, ENOUGH_USERS))
      }
  }

  def correlation(name: String, rates: Array[CaratRate], aPriori: scala.collection.immutable.HashMap[Double,Double], models: Set[String], oses: Set[String]) = {
    var modelCorrelations = new scala.collection.immutable.HashMap[String, Double]
    var osCorrelations = new scala.collection.immutable.HashMap[String, Double]

    val rateEvs = ProbUtil.normalize(DynamoAnalysisUtil.mapToRateEv(aPriori, rates).toMap)
    if (rateEvs != null) {
      for (model <- models) {
        /* correlation with this model */
        val rateModels = rates.map(x => {
          if (x.model == model)
            (x, 1.0)
          else
            (x, 0.0)
        }).toMap
        val norm = ProbUtil.normalize(rateModels)
        if (norm != null) {
          val corr = rateEvs.map(x => {
            x._2 * norm.getOrElse(x._1, 0.0)
          }).sum
          modelCorrelations += ((model, corr))
        } else
          println("ERROR: zero stddev for %s: %s".format(model, rateModels.map(x => { (x._1.model, x._2) })))
      }

      for (os <- oses) {
        /* correlation with this OS */
        val rateOses = rates.map(x => {
          if (x.os == os)
            (x, 1.0)
          else
            (x, 0.0)
        }).toMap
        val norm = ProbUtil.normalize(rateOses)
        if (norm != null) {
          val corr = rateEvs.map(x => {
            x._2 * norm.getOrElse(x._1, 0.0)
          }).sum
          osCorrelations += ((os, corr))
        } else
          println("ERROR: zero stddev for %s: %s".format(os, rateOses.map(x => { (x._1.os, x._2) })))
      }

      for (k <- modelCorrelations)
        println("%s and %s correlated with %s".format(name, k._1, k._2))
      for (k <- osCorrelations)
        println("%s and %s correlated with %s".format(name, k._1, k._2))
    } else
      println("ERROR: Rates had a zero stddev, something is wrong!")

    (osCorrelations, modelCorrelations)
  }

  /**
   * Calculate similar apps for device `uuid` based on all rate measurements and apps reported on the device.
   * Write them to DynamoDb.
   */
  def similarApps(all: Array[CaratRate], aPrioriDistribution: scala.collection.immutable.HashMap[Double,Double], i: Int, uuidApps: Set[String]) {
    val sCount = similarityCount(uuidApps.size)
    printf("SimilarApps client=%s sCount=%s uuidApps.size=%s\n", i, sCount, uuidApps.size)
    val similar = all.filter(_.allApps.intersect(uuidApps).size >= sCount)
    val dissimilar = all.filter(_.allApps.intersect(uuidApps).size < sCount)
    //printf("SimilarApps similar.count=%s dissimilar.count=%s\n",similar.count(), dissimilar.count())
    // no distance check, not bug or hog
    plotDists("Similar to client " + i, "Not similar to client " + i, similar, dissimilar, aPrioriDistribution, false, null, null, null, 0, 0, null)
  }

  /* Generate a gnuplot-readable plot file of the bucketed distribution.
   * Create folders plots/data plots/plotfiles
   * Save it as "plots/data/titleWith-titleWithout".txt.
   * Also generate a plotfile called plots/plotfiles/titleWith-titleWithout.gnuplot
   */

  def plotDists(title: String, titleNeg: String,
    one: Array[CaratRate], two: Array[CaratRate], aPrioriDistribution: scala.collection.immutable.HashMap[Double,Double], isBugOrHog: Boolean,
    filtered: Array[CaratRate], oses: Set[String], models: Set[String], usersWith: Int, usersWithout: Int, uuid: String) = {
    var hasSamples = true
    if (usersWith == 0 && usersWithout == 0) {
      hasSamples = one.take(1) match {
        case Array(t) => true
        case _ => false
      }
      hasSamples = two.take(1) match {
        case Array(t) => hasSamples && true
        case _ => false
      }
    }
    if (hasSamples) {
      val (xmax, probDist, probDistNeg, ev, evNeg, evDistance /*, usersWith, usersWithout*/ ) = DynamoAnalysisUtil.getDistanceAndDistributionsUnBucketed(one, two, aPrioriDistribution)
      if (probDist != null && probDistNeg != null && (!isBugOrHog || evDistance > 0)) {
        if (evDistance > 0) {
          var imprHr = (100.0 / evNeg - 100.0 / ev) / 3600.0
          val imprD = (imprHr / 24.0).toInt
          imprHr -= imprD * 24.0
          printf("%s evWith=%s evWithout=%s evDistance=%s improvement=%s days %s hours (%s vs %s users)\n", title, ev, evNeg, evDistance, imprD, imprHr, usersWith, usersWithout)
        } else {
          printf("%s evWith=%s evWithout=%s evDistance=%s (%s vs %s users)\n", title, ev, evNeg, evDistance, usersWith, usersWithout)
        }
        if (isBugOrHog && filtered != null) {
          val (osCorrelations, modelCorrelations) = correlation(title, filtered, aPrioriDistribution, models, oses)
          plot(title, titleNeg, xmax, probDist, probDistNeg, ev, evNeg, evDistance, osCorrelations, modelCorrelations, usersWith, usersWithout, uuid)
        } else
          plot(title, titleNeg, xmax, probDist, probDistNeg, ev, evNeg, evDistance, null, null, usersWith, usersWithout, uuid)
      }
      isBugOrHog && evDistance > 0
    } else
      false
  }

  def plot(title: String, titleNeg: String, xmax: Double, distWith: Array[(Double, Double)],
    distWithout: Array[(Double, Double)],
    ev: Double, evNeg: Double, evDistance: Double,
    osCorrelations: Map[String, Double], modelCorrelations: Map[String, Double],
    usersWith: Int, usersWithout: Int, uuid: String) {
    println("Calculated %s vs %s xmax=%s ev=%s evWithout=%s evDistance=%s osCorrelations=%s modelCorrelations=%s uuid=%s".format(
        title, titleNeg, xmax, ev, evNeg, evDistance, osCorrelations, modelCorrelations, uuid))
  }

  /**
   * The J-Score is the % of people with worse = higher energy use.
   * therefore, it is the size of the set of evDistances that are higher than mine,
   * compared to the size of the user base.
   * Note that the server side multiplies the JScore by 100, and we store it here
   * as a fraction.
   */
  def plotJScores(distsWithUuid: TreeMap[String, Array[(Double, Double)]],
    distsWithoutUuid: TreeMap[String, Array[(Double, Double)]],
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
        println("Calculated Profile for %s xmax=%s ev=%s evWithout=%s jscore=%s apps=%s".format(k, xmax, ev, evNeg, jscore, apps.size))
      else
        printf("Error: Could not plot jscore, because: distWith=%s distWithout=%s apps=%s\n", distWith, distWithout, apps)
    }
  }
}
