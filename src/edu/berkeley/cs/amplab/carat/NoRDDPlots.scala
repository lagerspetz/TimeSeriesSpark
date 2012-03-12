package edu.berkeley.cs.amplab.carat

import spark._
import spark.SparkContext._
import spark.timeseries._
import edu.berkeley.cs.amplab.carat.dynamodb.DynamoAnalysisUtil
import edu.berkeley.cs.amplab.carat.dynamodb.DynamoDbDecoder
import edu.berkeley.cs.amplab.carat.plot.PlotUtil
import scala.collection.immutable.Set
import scala.collection.immutable.TreeMap
import scala.collection.mutable.HashMap
import collection.JavaConversions._
import java.text.SimpleDateFormat

/**
 * Do the exact same thing as in CaratDynamoDataToPlots, but do not collect() and write plot files and run plotting in the end.
 *
 * @author Eemil Lagerspetz
 */

object NoRDDPlots {

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

  var plotDirectory: String = null

  /**
   * Main program entry point.
   */
  def main(args: Array[String]) {
    var master = "local[16]"
    if (args != null && args.length >= 1) {
      master = args(0)
    }
    if (args != null && args.length > 1)
      plotDirectory = args(1)

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
      val ret = plotDists("iOS " + os, "Other versions", fromOs, notFromOs, aPrioriDistribution, false, null, null, null, null, 0, 0, null)
    }

    for (model <- models) {
      // can be done in parallel, independent of anything else
      val fromModel = allRates.filter(_.model == model)
      val notFromModel = allRates.filter(_.model != model)
      // no distance check, not bug or hog
      val ret = plotDists(model, "Other models", fromModel, notFromModel, aPrioriDistribution, false, null, null, null, null, 0, 0, null)
    }

    /** Calculate correlation for each model and os version with all rates */
    val (osCorrelations, modelCorrelations, userCorrelations) = correlation("All", allRates, aPrioriDistribution, models, oses, totalsByUuid)
    

    // need to collect uuid stuff here:
    PlotUtil.plotJScores(distsWithUuid, distsWithoutUuid, parametersByUuid, evDistanceByUuid, appsByUuid, DECIMALS)
    PlotUtil.writeCorrelationFile("All", osCorrelations, modelCorrelations, userCorrelations, 0, 0)

    //scheduler.execute({
    //var allHogs = new HashSet[String]
    //var allBugs = new HashSet[String]

    /* Hogs: Consider all apps except daemons. */
    for (app <- allApps) {
      oneApp(uuidArray, allRates, app,aPrioriDistribution, oses, models, parametersByUuid, totalsByUuid)
    }

    /*val globalNonHogs = allApps -- allHogs
    println("Non-daemon non-hogs: " + globalNonHogs)
    println("All hogs: " + allHogs)
    println("All bugs: " + allBugs)*/
    //})
    //println("Calculated global correlations: osCorrelations=%s modelCorrelations=%s".format(osCorrelations, modelCorrelations))
  }

  def correlation(name: String, rates: Array[CaratRate],
      aPriori: scala.collection.immutable.HashMap[Double, Double],
      models: Set[String], oses: Set[String],
      totalsByUuid: TreeMap[String, (Double, Double)]) = {
    var modelCorrelations = new scala.collection.immutable.HashMap[String, Double]
    var osCorrelations = new scala.collection.immutable.HashMap[String, Double]
    var userCorrelations = new scala.collection.immutable.HashMap[String, Double]

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
      
      {
        val rateTotals = rates.map(x => {
          (x, totalsByUuid.getOrElse(x.uuid, (0.0, 0.0)))
        }).toMap
        val normSamples = ProbUtil.normalize(rateTotals.map(x => {(x._1, x._2._1)}))
        if (normSamples != null) {
          val corr = rateEvs.map(x => {
            x._2 * normSamples.getOrElse(x._1, 0.0)
          }).sum
          userCorrelations += (("Samples", corr))
        } else
          println("ERROR: zero stddev for %s: %s".format("Samples", rateTotals.map(x => { (x._1.uuid, x._2) })))
      }
      
        {
        val rateTotals = rates.map(x => {
          (x, totalsByUuid.getOrElse(x.uuid, (0.0, 0.0)))
        }).toMap
        val normApps = ProbUtil.normalize(rateTotals.map(x => {(x._1, x._2._2)}))
        if (normApps != null) {
          val corr = rateEvs.map(x => {
            x._2 * normApps.getOrElse(x._1, 0.0)
          }).sum
          userCorrelations += (("Apps", corr))
        } else
          println("ERROR: zero stddev for %s: %s".format("Apps", rateTotals.map(x => { (x._1.uuid, x._2) })))
      }

      for (k <- modelCorrelations)
        println("%s and %s correlated with %s".format(name, k._1, k._2))
      for (k <- osCorrelations)
        println("%s and %s correlated with %s".format(name, k._1, k._2))
        for (k <- userCorrelations)
          println("%s and %s correlated with %s".format(name, k._1, k._2))
    } else
      println("ERROR: Rates had a zero stddev, something is wrong!")

    (osCorrelations, modelCorrelations, userCorrelations)
  }

  /**
   * Calculate similar apps for device `uuid` based on all rate measurements and apps reported on the device.
   * Write them to DynamoDb.
   */
  def similarApps(all: Array[CaratRate], aPrioriDistribution: scala.collection.immutable.HashMap[Double, Double], i: Int, uuidApps: Set[String]) {
    val sCount = similarityCount(uuidApps.size)
    printf("SimilarApps client=%s sCount=%s uuidApps.size=%s\n", i, sCount, uuidApps.size)
    val similar = all.filter(_.allApps.intersect(uuidApps).size >= sCount)
    val dissimilar = all.filter(_.allApps.intersect(uuidApps).size < sCount)
    //printf("SimilarApps similar.count=%s dissimilar.count=%s\n",similar.count(), dissimilar.count())
    // no distance check, not bug or hog
    plotDists("Similar to client " + i, "Not similar to client " + i, similar, dissimilar, aPrioriDistribution, false, null, null, null, null, 0, 0, null)
  }

  /* Generate a gnuplot-readable plot file of the bucketed distribution.
   * Create folders plots/data plots/plotfiles
   * Save it as "plots/data/titleWith-titleWithout".txt.
   * Also generate a plotfile called plots/plotfiles/titleWith-titleWithout.gnuplot
   */

  def plotDists(title: String, titleNeg: String,
    one: Array[CaratRate], two: Array[CaratRate], aPrioriDistribution: scala.collection.immutable.HashMap[Double, Double], isBugOrHog: Boolean,
    filtered: Array[CaratRate], oses: Set[String], models: Set[String], totalsByUuid: TreeMap[String, (Double, Double)], usersWith: Int, usersWithout: Int, uuid: String) = {
    var hasSamples = one.length > 0 && two.length > 0
    if (hasSamples) {
      val (xmax, probDist, probDistNeg, ev, evNeg, evDistance /*, usersWith, usersWithout*/ ) = DynamoAnalysisUtil.getDistanceAndDistributionsUnBucketed(one, two, aPrioriDistribution)
      if (probDist != null && probDistNeg != null && (!isBugOrHog || evDistance > 0)) {
        if (evDistance > 0) {
          var imprHr = (100.0 / evNeg - 100.0 / ev) / 3600.0
          val imprD = (imprHr / 24.0).toInt
          imprHr -= imprD * 24.0
          printf("%s evWith=%s evWithout=%s evDistance=%s improvement=%s days %s hours (%s vs %s users)\n", title, ev, evNeg, evDistance, imprD, imprHr, usersWith, usersWithout)
        }
        if (isBugOrHog && filtered != null) {
          val (osCorrelations, modelCorrelations, userCorrelations) = correlation(title, filtered, aPrioriDistribution, models, oses, totalsByUuid)
          PlotUtil.plot(title, titleNeg, xmax, probDist, probDistNeg, ev, evNeg, evDistance, osCorrelations, modelCorrelations, userCorrelations, usersWith, usersWithout, uuid, DECIMALS)
        } else
          PlotUtil.plot(title, titleNeg, xmax, probDist, probDistNeg, ev, evNeg, evDistance, null, null, null, usersWith, usersWithout, uuid, DECIMALS)
      } else {
        printf("Not %s evWith=%s evWithout=%s evDistance=%s (%s vs %s users) %s\n", title, ev, evNeg, evDistance, usersWith, usersWithout, uuid)
      }
      isBugOrHog && evDistance > 0
    } else
      false
  }
  
    def oneApp(uuidArray:Array[String], allRates: Array[edu.berkeley.cs.amplab.carat.CaratRate], app: String,
      aPrioriDistribution: scala.collection.immutable.HashMap[Double,Double],
      oses: scala.collection.immutable.Set[String], models: scala.collection.immutable.Set[String],
      parametersByUuid: scala.collection.immutable.TreeMap[String,(Double, Double, Double)],
      totalsByUuid: scala.collection.immutable.TreeMap[String,(Double, Double)]){
      val filtered = allRates.filter(_.allApps.contains(app))
      val filteredNeg = allRates.filter(!_.allApps.contains(app))

      /* Consider only apps that have rates from ENOUGH_USERS client devices.
       * Require that the reference distribution also have rates from ENOUGH_USERS client devices.
       *  
       */
      val fCountStart = DynamoAnalysisUtil.start
      val usersWith = filtered.map(_.uuid).toSet.size

      if (usersWith >= ENOUGH_USERS) {
        val usersWithout = filteredNeg.map(_.uuid).toSet.size
        DynamoAnalysisUtil.finish(fCountStart, "clientCount")
        if (usersWithout >= ENOUGH_USERS) {
          if (plotDists("Hog " + app + " running", app + " not running", filtered, filteredNeg, aPrioriDistribution, true, filtered, oses, models, totalsByUuid, usersWith, usersWithout, null)) {
            // this is a hog

            //allHogs += app
          } else {
            // not a hog. is it a bug for anyone?
            for (i <- 0 until uuidArray.length) {
              val uuid = uuidArray(i)
              /* Bugs: Only consider apps reported from this uuId. Only consider apps not known to be hogs. */
              val appFromUuid = filtered.filter(_.uuid == uuid) //.cache()
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
              if (plotDists("Bug " + app + " running on client " + i, app + " running on other clients", appFromUuid, appNotFromUuid, aPrioriDistribution, true,
                filtered, oses, models, totalsByUuid, 0, 0, stuff)) {
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
}
