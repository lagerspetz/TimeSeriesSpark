package edu.berkeley.cs.amplab.carat

import spark._
import spark.SparkContext._
import spark.timeseries._
import edu.berkeley.cs.amplab.carat.plot.PlotUtil
import edu.berkeley.cs.amplab.carat.dynamodb.DynamoAnalysisUtil
import scala.collection.immutable.TreeMap
import scala.collection.mutable.HashMap
import collection.JavaConversions._

/**
 * Do the exact same thing as in CaratDynamoDataToPlots, but do not collect() and write plot files and run plotting in the end.
 *
 * @author Eemil Lagerspetz
 */

object CaratDynamoNoRDDAnalysis {

  // How many clients do we need to consider data reliable?
  val ENOUGH_USERS = 5

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
    CaratAnalysisGeneric.genericAnalysis(master, tmpdir, userLimit, ENOUGH_USERS, DECIMALS,
        x => {}, printDists, printJScores, globalCorrelations)
  }

  /* Generate a gnuplot-readable plot file of the bucketed distribution.
   * Create folders plots/data plots/plotfiles
   * Save it as "plots/data/titleWith-titleWithout".txt.
   * Also generate a plotfile called plots/plotfiles/titleWith-titleWithout.gnuplot
   */

  def printDists(title: String, titleNeg: String,
    one: Array[CaratRate], two: Array[CaratRate], aPrioriDistribution: scala.collection.immutable.HashMap[Double, Double], isBugOrHog: Boolean,
    filtered: Array[CaratRate], oses: Set[String], models: Set[String], 
    totalsByUuid: scala.collection.immutable.TreeMap[String,(Double, Double)], usersWith: Int, usersWithout: Int, uuid: String) = {
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
          val (osCorrelations, modelCorrelations, userCorrelations) = DynamoAnalysisUtil.correlation(title, filtered, aPrioriDistribution, models, oses, totalsByUuid)
          print(title, titleNeg, xmax, probDist, probDistNeg, ev, evNeg, evDistance, osCorrelations, modelCorrelations, userCorrelations, usersWith, usersWithout, uuid)
        } else
          print(title, titleNeg, xmax, probDist, probDistNeg, ev, evNeg, evDistance, null, null, null, usersWith, usersWithout, uuid)
      }
      isBugOrHog && evDistance > 0
    } else
      false
  }

  def print(title: String, titleNeg: String, xmax: Double, distWith: Array[(Double, Double)], 
    distWithout: Array[(Double, Double)], 
    ev: Double, evNeg: Double, evDistance: Double, 
    osCorrelations: Map[String, Double], modelCorrelations: Map[String, Double], userCorrelations: Map[String, Double], 
    usersWith: Int, usersWithout: Int, uuid: String) {
    println("Calculated %s vs %s xmax=%s ev=%s evWithout=%s evDistance=%s osCorrelations=%s modelCorrelations=%s userCorrelations=%s uuid=%s".format(
      title, titleNeg, xmax, ev, evNeg, evDistance, osCorrelations, modelCorrelations, userCorrelations, uuid))
  }

  /**
   * The J-Score is the % of people with worse = higher energy use.
   * therefore, it is the size of the set of evDistances that are higher than mine,
   * compared to the size of the user base.
   * Note that the server side multiplies the JScore by 100, and we store it here
   * as a fraction.
   */
  
  
  def printJScores(distsWithUuid: TreeMap[String, Array[(Double, Double)]], 
    distsWithoutUuid: TreeMap[String, Array[(Double, Double)]], 
    parametersByUuid: TreeMap[String, (Double, Double, Double)], 
    evDistanceByUuid: TreeMap[String, Double], 
    appsByUuid: TreeMap[String, Set[String]], 
    uuidToOsAndModel: scala.collection.mutable.HashMap[String, (String, String)], 
    decimals:Int) {
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
          ProbUtil.nDecimal(dists.filter(_ > temp).size * 1.0 / dists.size, decimals)
      }
      val distWith = distsWithUuid.get(k).getOrElse(null)
      val distWithout = distsWithoutUuid.get(k).getOrElse(null)
      val apps = appsByUuid.get(k).getOrElse(null)
      if (distWith != null && distWithout != null && apps != null) {
        val (os, model) = uuidToOsAndModel.getOrElse(k, ("", ""))
        println("Calculated Profile for %s %s running %s xmax=%s ev=%s evWithout=%s jscore=%s apps=%s".format(k, model, os, xmax, ev, evNeg, jscore, apps.size))
      } else
        printf("Error: Could not plot jscore, because: distWith=%s distWithout=%s apps=%s\n", distWith, distWithout, apps)
    }
  }

  def globalCorrelations(name: String,
    osCorrelations: Map[String, Double],
    modelCorrelations: Map[String, Double],
    userCorrelations: Map[String, Double],
    usersWith: Int, usersWithout: Int, uuid: String = null) {
    println("Calculated global correlations: osCorrelations=%s modelCorrelations=%s userCorrelations=%s".format(osCorrelations, modelCorrelations, userCorrelations))
  }
}
