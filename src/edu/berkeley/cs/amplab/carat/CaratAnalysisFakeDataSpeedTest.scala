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
import scala.collection.mutable.Buffer

/**
 * Run Carat Analysis without storing any data, just speed testing the implementation.
 * Uses the newer `StoredSampleAnalysisGeneric` for the analysis.
 *
 * @author Eemil Lagerspetz
 */

object CaratAnalysisFakeDataSpeedTest {

  // How many clients do we need to consider data reliable?
  val ENOUGH_USERS = 5

  var DEBUG = false
  val DECIMALS = 3
  // Isolate from the plotting.
  val tmpdir = "/mnt/TimeSeriesSpark-unstable/spark-temp-plots/"

  val fakeRegs = "fake-regs.dat"
  val fakeSamples = "fake-samples.dat"

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

    // Fix Spark running out of space on AWS.
    System.setProperty("spark.local.dir", tmpdir)

    val sc = TimeSeriesSpark.init(master, "default", "CaratDynamoAnalysis")
    // Open stored fake regs and samples 
    val regs = sc.objectFile[(String, String, String, Double)](fakeRegs)
    val samples = sc.objectFile[(String, String, String, String, String, Buffer[String], scala.collection.immutable.Map[String, (String, Object)])](fakeSamples)
    // analyze them and time the whole thing
    StoredSampleAnalysisGeneric.genericAnalysisForSamples(sc, regs, samples, userLimit, ENOUGH_USERS, DECIMALS,
      x => {}, PrintStats.printDists, PrintStats.printSkipped, PrintStats.printJScores, PrintStats.globalCorrelations)
    DynamoAnalysisUtil.finish(start)
  }
}

