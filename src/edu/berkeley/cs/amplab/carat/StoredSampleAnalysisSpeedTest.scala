package edu.berkeley.cs.amplab.carat

import spark._
import spark.SparkContext._
import spark.timeseries._
import edu.berkeley.cs.amplab.carat.plot.PlotUtil
import edu.berkeley.cs.amplab.carat.dynamodb.DynamoAnalysisUtil
import scala.collection.immutable.TreeMap
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import collection.JavaConversions._

/**
 * Run Carat Analysis without storing any data, just speed testing the implementation.
 * Uses the newer `StoredSampleAnalysisGeneric` for the analysis.
 *
 * @author Eemil Lagerspetz
 */

object StoredSampleAnalysisSpeedTest {

  // How many clients do we need to consider data reliable?
  val ENOUGH_USERS = 5

  var DEBUG = false
  val DECIMALS = 3
  // Isolate from the plotting.
  val tmpdir = "spark-temp-plots/"

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
    StoredSampleAnalysisGeneric.genericAnalysis(master, tmpdir, userLimit, ENOUGH_USERS, DECIMALS,
      x => {}, PrintStats.printDists, PrintStats.printSkipped, PrintStats.printJScores, PrintStats.globalCorrelations)
  }
}
