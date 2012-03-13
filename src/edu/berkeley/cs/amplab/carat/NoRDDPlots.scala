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
 * Plot using an RDD only for the initial gathering of the rates.
 *
 * @author Eemil Lagerspetz
 */

object NoRDDPlots {
  // How many clients do we need to consider data reliable?
  val ENOUGH_USERS = 5
  val DECIMALS = 3
  // Isolate from the analysis to db.
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
    CaratAnalysisGeneric.genericAnalysis(master, tmpdir, ENOUGH_USERS, DECIMALS,
        x => {}, plotDists,PlotUtil.plotJScores,PlotUtil.writeCorrelationFile)
        
    DynamoAnalysisUtil.finish(start)
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
          val (osCorrelations, modelCorrelations, userCorrelations) = DynamoAnalysisUtil.correlation(title, filtered, aPrioriDistribution, models, oses, totalsByUuid)
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
}
