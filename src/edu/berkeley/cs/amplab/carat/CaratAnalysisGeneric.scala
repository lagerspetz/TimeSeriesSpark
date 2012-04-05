package edu.berkeley.cs.amplab.carat

import spark._
import spark.SparkContext._
import spark.timeseries._
import edu.berkeley.cs.amplab.carat.dynamodb.DynamoAnalysisUtil
import scala.collection.immutable.Set
import scala.collection.immutable.TreeMap
import scala.collection.mutable.Map
import collection.JavaConversions._

/**
 * Do the exact same thing as in CaratDynamoDataToPlots, but do not collect() and write plot files and run plotting in the end.
 *
 * @author Eemil Lagerspetz
 */

object CaratAnalysisGeneric {
  var removeD: ( /*daemonSet:*/ Set[String]) => Unit = null
  var action: ( /*nature:*/ String, /*keyValue1:*/ String, /*keyValue2:*/ String, String, String, Array[CaratRate], Array[CaratRate], Map[Double, Double], Boolean, Array[CaratRate], Set[String], Set[String], TreeMap[String, (Double, Double)], Int, Int, String) => Boolean = null
  var skipped: ( /*nature:*/ String, /*keyValue1:*/ String, /*keyValue2:*/ String, /*title:*/ String) => Unit = null
  var jsFunction: ( /*allRates:*/ RDD[CaratRate], /* aPrioriDistribution: */ Map[Double, Double], /*distsWithUuid:*/ TreeMap[String, Array[(Double, Double)]], /*distsWithoutUuid:*/ TreeMap[String, Array[(Double, Double)]], /*parametersByUuid:*/ TreeMap[String, (Double, Double, Double)], /*evDistanceByUuid:*/ TreeMap[String, Double], /*appsByUuid:*/ TreeMap[String, Set[String]], /*uuidToOsAndModel:*/ scala.collection.mutable.HashMap[String, (String, String)], /*decimals:*/ Int) => Unit = null
  var corrFunction: ( /*name:*/ String, /*osCorrelations:*/ scala.collection.immutable.Map[String, Double], /*modelCorrelations:*/ scala.collection.immutable.Map[String, Double], /*userCorrelations:*/ scala.collection.immutable.Map[String, Double], /*usersWith:*/ Int, /*usersWithout:*/ Int, /*uuid:*/ String) => Unit = null

  var ENOUGH_USERS = 5
  var DECIMALS = 3

  var userLimit = Int.MaxValue

  /**
   * Main program entry point.
   */
  def genericAnalysis(master: String, tmpDir: String, clients: Int, CLIENT_THRESHOLD: Int, DECIMALS: Int,
    removeDaemonsFunction: ( /*daemonSet:*/ Set[String]) => Unit,
    actionFunction:  (/*nature:*/ String, /*keyValue1:*/ String, /*keyValue2:*/ String, String, String, Array[CaratRate], Array[CaratRate], Map[Double, Double], Boolean, Array[CaratRate], Set[String], Set[String], TreeMap[String, (Double, Double)], Int, Int, String) => Boolean,
    skippedFunction: (/*nature:*/ String, /*keyValue1:*/ String, /*keyValue2:*/ String, String) => Unit,
    JScoreFunction: ( /*allRates:*/ RDD[CaratRate], /* aPrioriDistribution: */ Map[Double, Double], /*distsWithUuid:*/ TreeMap[String, Array[(Double, Double)]], /*distsWithoutUuid:*/ TreeMap[String, Array[(Double, Double)]], /*parametersByUuid:*/ TreeMap[String, (Double, Double, Double)], /*evDistanceByUuid:*/ TreeMap[String, Double], /*appsByUuid:*/ TreeMap[String, Set[String]], /*uuidToOsAndModel:*/ scala.collection.mutable.HashMap[String, (String, String)], /*decimals:*/ Int) => Unit,
    correlationFunction: ( /*name:*/ String, /*osCorrelations:*/ scala.collection.immutable.Map[String, Double], /*modelCorrelations:*/ scala.collection.immutable.Map[String, Double], /*userCorrelations:*/ scala.collection.immutable.Map[String, Double], /*usersWith:*/ Int, /*usersWithout:*/ Int, /*uuid:*/ String) => Unit) {
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

    removeD = removeDaemonsFunction
    action = actionFunction
    skipped = skippedFunction
    jsFunction = JScoreFunction
    corrFunction = correlationFunction
    ENOUGH_USERS = CLIENT_THRESHOLD
    userLimit = clients

    // Fix Spark running out of space on AWS.
    System.setProperty("spark.local.dir", tmpDir)

    //System.setProperty("spark.kryo.registrator", classOf[CaratRateRegistrator].getName)
    val sc = TimeSeriesSpark.init(master, "default", "CaratDynamoDataSpeedTest")
    // getRates
    val allRates = DynamoAnalysisUtil.getRates(sc, tmpDir)
    if (allRates != null) {
      // analyze data
      analyzeRateData(allRates)
      // do not save rates in this version. Saving will be done by another program.
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
    /* We do not know the final OS for an uuid. This takes care of that.
     Rates are sorted chronologically, so this yields the latest os and model. */
    for (k <- allRates) {
      val old = uuidToOsAndModel.getOrElse(k.uuid, (k.os, k.model))
      uuidToOsAndModel += ((k.uuid, old))
    }

    var uuidArray = uuidToOsAndModel.keySet.toArray.sortWith((s, t) => {
      s < t
    })

    if (userLimit < uuidArray.length) {
      println("Running analysis for %s users.".format(userLimit))
      // limit data to these users:
      uuidArray = uuidArray.slice(0, userLimit)
      allRates = inputRates.filter(x => {
        uuidArray.contains(x.uuid)
      }).collect()

    } else {
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
    /* avg samples and apps */
    var totalsByUuid = new TreeMap[String, (Double, Double)]
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
    removeD(DAEMONS_LIST_GLOBBED)

    /* uuid stuff */

    for (i <- 0 until uuidArray.length) {
      // these are independent until JScores.
      val uuid = uuidArray(i)
      // Take care here to use samples for the _latest_ os
      val fromUuid = allRates.filter(uuidOsFilter(uuidToOsAndModel, uuid, _))

      var uuidApps = fromUuid.flatMap(_.allApps).toSet
      uuidApps --= DAEMONS_LIST_GLOBBED
      if (uuidApps.size > 0)
        similarApps(allRates, aPrioriDistribution, uuid, i, uuidApps)
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

      /* avg apps: */
      //val uApps = fromUuid.map(_.allApps.size)
      //val avgApps = uApps.sum * 1.0 / uApps.length
      val totalSamples = fromUuid.length * 1.0
      totalsByUuid += ((uuid, (totalSamples, uuidApps.size)))
      appsByUuid += ((uuid, uuidApps))
    }

    for (os <- oses) {
      // can be done in parallel, independent of anything else
      val fromOs = allRates.filter(_.os == os)
      val notFromOs = allRates.filter(_.os != os)
      // no distance check, not bug or hog
      val ret = action("os", os, null, "iOS " + os, "Other versions", fromOs, notFromOs, aPrioriDistribution, false, null, null, null, null, 0, 0, null)
      //val ret = plotDists("iOS " + os, "Other versions", fromOs, notFromOs, aPrioriDistribution, false, null, null, null, null, 0, 0, null)
    }

    for (model <- models) {
      // can be done in parallel, independent of anything else
      val fromModel = allRates.filter(_.model == model)
      val notFromModel = allRates.filter(_.model != model)
      // no distance check, not bug or hog
      val ret = action("model", model, null, model, "Other models", fromModel, notFromModel, aPrioriDistribution, false, null, null, null, null, 0, 0, null)
    }

    /** Calculate correlation for each model and os version with all rates */
    val (osCorrelations, modelCorrelations, userCorrelations) = DynamoAnalysisUtil.correlation("All", allRates, aPrioriDistribution, models, oses, totalsByUuid)

    // need to collect uuid stuff here:
    jsFunction(inputRates, aPrioriDistribution, distsWithUuid, distsWithoutUuid, parametersByUuid, evDistanceByUuid, appsByUuid, uuidToOsAndModel, DECIMALS)
    corrFunction("All", osCorrelations, modelCorrelations, userCorrelations, 0, 0, null)

    //scheduler.execute({
    //var allHogs = new HashSet[String]
    //var allBugs = new HashSet[String]

    /* Hogs: Consider all apps except daemons. */
    for (app <- allApps) {
      oneApp(uuidArray, allRates, app, aPrioriDistribution, oses, models, parametersByUuid, totalsByUuid, uuidToOsAndModel)
    }

    /*val globalNonHogs = allApps -- allHogs
    println("Non-daemon non-hogs: " + globalNonHogs)
    println("All hogs: " + allHogs)
    println("All bugs: " + allBugs)*/
    //})
    //println("Calculated global correlations: osCorrelations=%s modelCorrelations=%s".format(osCorrelations, modelCorrelations))
  }

  /**
   * Calculate similar apps for device `uuid` based on all rate measurements and apps reported on the device.
   * Write them to DynamoDb.
   */
  def similarApps(all: Array[CaratRate], aPrioriDistribution: Map[Double, Double], uuid: String, i: Int, uuidApps: Set[String]) {
    val sCount = similarityCount(uuidApps.size)
    printf("SimilarApps client=%s sCount=%s uuidApps.size=%s\n", i, sCount, uuidApps.size)
    val similar = all.filter(_.allApps.intersect(uuidApps).size >= sCount)
    val dissimilar = all.filter(_.allApps.intersect(uuidApps).size < sCount)
    //printf("SimilarApps similar.count=%s dissimilar.count=%s\n",similar.count(), dissimilar.count())
    // no distance check, not bug or hog
    action("similar", uuid, null, "Similar to client " + i, "Not similar to client " + i, similar, dissimilar, aPrioriDistribution, false, null, null, null, null, 0, 0, null)
  }

  def oneApp(uuidArray: Array[String], allRates: Array[edu.berkeley.cs.amplab.carat.CaratRate], app: String,
    aPrioriDistribution: Map[Double, Double],
    oses: scala.collection.immutable.Set[String], models: scala.collection.immutable.Set[String],
    parametersByUuid: scala.collection.immutable.TreeMap[String, (Double, Double, Double)],
    totalsByUuid: scala.collection.immutable.TreeMap[String, (Double, Double)],
    uuidToOsAndModel: scala.collection.mutable.HashMap[String, (String, String)]) {
    val filtered = allRates.filter(_.allApps.contains(app))
    val filteredNeg = allRates.filter(!_.allApps.contains(app))

    /* Consider only apps that have rates from ENOUGH_USERS client devices.
     * Require that the reference distribution also have rates from ENOUGH_USERS client devices.
     * TODO: Multiple thresholds
     */
    val fCountStart = DynamoAnalysisUtil.start
    val usersWith = filtered.map(_.uuid).toSet.size

    if (usersWith >= ENOUGH_USERS) {
      val usersWithout = filteredNeg.map(_.uuid).toSet.size
      DynamoAnalysisUtil.finish(fCountStart, "clientCount")
      if (usersWithout >= ENOUGH_USERS) {
        if (action("hog", app, null, "Hog " + app + " running", app + " not running", filtered, filteredNeg, aPrioriDistribution, true, filtered, oses, models, totalsByUuid, usersWith, usersWithout, null)) {
          // this is a hog

          //allHogs += app
        } else {
          // not a hog. is it a bug for anyone?
          for (i <- 0 until uuidArray.length) {
            val uuid = uuidArray(i)
            /* Bugs: Only consider apps reported from this uuId. Only consider apps not known to be hogs. 
             * Only consider samples from the latest OS of the uuId. */
            val appFromUuid = filtered.filter(uuidOsFilter(uuidToOsAndModel, uuid, _))
            val appNotFromUuid = filtered.filter(_.uuid != uuid) //.cache()
            /**
             * TODO: Require BUG_REFERENCE client devices in the reference distribution.
             */

            var stuff = uuid
            if (appFromUuid.length > 0) {
              val r = appFromUuid.head
              val (cxmax, cev, cevNeg) = parametersByUuid.getOrElse(uuid, (0.0, 0.0, 0.0))
              val (totalSamples, totalApps) = totalsByUuid.getOrElse(uuid, (0.0, 0.0))
              stuff += "\n%s running %s\nClient ev=%s total samples=%s apps=%s".format(
                r.model, r.os, cev, totalSamples, totalApps)
            }
            if (action("bug", uuid, app, "Bug " + app + " running on client " + i, app + " running on other clients", appFromUuid, appNotFromUuid, aPrioriDistribution, true,
              filtered, oses, models, totalsByUuid, 0, 0, stuff)) {
              //allBugs += app
            }
          }
        }
      } else {
        skipped("hog", app, null, "Hog " + app + " running")
        println("Skipped app " + app + " for too few points in: without: %s < thresh=%s".format(usersWithout, ENOUGH_USERS))
      }
    } else {
      skipped("hog", app, null, "Hog " + app + " running")
      println("Skipped app " + app + " for too few points in: with: %s < thresh=%s".format(usersWith, ENOUGH_USERS))
    }
  }
  
  def uuidOsFilter(uuidToOsAndModel: scala.collection.mutable.HashMap[String, (String, String)], uuid:String, x: CaratRate) = {
    x.uuid == uuid && x.os == uuidToOsAndModel.getOrElse(x.uuid, ("", ""))._1
  }
}
