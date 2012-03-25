package edu.berkeley.cs.amplab.carat.tools

import spark._
import spark.SparkContext._
import edu.berkeley.cs.amplab.carat.dynamodb.DynamoAnalysisUtil
import spark.timeseries.TimeSeriesSpark
import edu.berkeley.cs.amplab.carat.CaratRate

object APrioriRateAnalysis extends App {
  val tmpdir = "/mnt/TimeSeriesSpark-unstable/spark-temp-plots/"

  val start = DynamoAnalysisUtil.start()

  var master = "local[1]"
  if (args != null && args.length >= 1) {
    master = args(0)
  }

  // turn off INFO logging for spark:
  System.setProperty("hadoop.root.logger", "WARN,console")
  // This is misspelled in the spark jar log4j.properties:
  System.setProperty("log4j.threshhold", "WARN")
  // Include correct spelling to make sure
  System.setProperty("log4j.threshold", "WARN")

  // Fix Spark running out of space on AWS.
  System.setProperty("spark.local.dir", tmpdir)

  //System.setProperty("spark.kryo.registrator", classOf[CaratRateRegistrator].getName)

  val sc = TimeSeriesSpark.init(master, "default", "CaratDynamoDataToPlots")

  val rfile = "apriori-rates.rdd"
  val f = new java.io.File(rfile)

  val rdd = {
    if (!f.exists()) {
      val allRates = DynamoAnalysisUtil.getRates(sc, tmpdir)
      saveApriori(sc, allRates)
    } else {
      loadApriori(sc)
    }
  }

  val coll = rdd.collect().sortWith((x, y) => {
    if (x.uuid < y.uuid)
      true
    else if (y.uuid == x.uuid)
      x.time2 < y.time2
    else
      false
  })

  println("A Priori has %d rates. They are (chronological):".format(coll.size))
  for (k <- coll)
    println(k)

  def saveApriori(sc: SparkContext, rdd: RDD[CaratRate]) = {
    rdd.saveAsObjectFile("all-rates.rdd")
    val pointRates = rdd.filter(!_.isRateRange())
    pointRates.saveAsObjectFile(rfile)
    pointRates
  }

  def loadApriori(sc: SparkContext) = {
    sc.objectFile[CaratRate](rfile)
  }
}