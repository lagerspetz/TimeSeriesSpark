package edu.berkeley.cs.amplab.carat.tools

import spark._
import spark.SparkContext._
import spark.timeseries.TimeSeriesSpark
import edu.berkeley.cs.amplab.carat.CaratRate
import edu.berkeley.cs.amplab.carat.dynamodb.DynamoAnalysisUtil
import scala.collection.mutable.Buffer
import collection.JavaConversions._

object SamplesToText extends App {

  /**
   * Define a utility function to print regs and samples:
   */
  def printRegs(rfile: String, rfile2: String) {
    val f = new java.io.File(tmpdir + rfile)

    val (regRdd, sampleRdd) = {
      if (!f.exists()) {
        DynamoAnalysisUtil.getSamples(sc, tmpdir, true)
      } else {
        (sc.objectFile[(String, String, String, Double)](rfile),
          sc.objectFile[(String, String, String, String, String, Buffer[String], scala.collection.immutable.Map[String, (String, Object)])](rfile2))
      }
    }

    {
      println("There are a total of %d regs. They are:".format(regRdd.count()))
      val regStrs = regRdd.map(x => {
        "%s %s %s %s".format(x._1, x._4, x._2, x._3)
      }).collect().sorted

      for (str <- regStrs)
        println(str)
    }

    {
      println("There are a total of %d samples. They are:".format(sampleRdd.count()))
      val sampleStrs = sampleRdd.map(x => {
        // print others first
        var str1 = "%s %s %s %s %s apps=%s".format(x._1, x._2, x._3, x._4, x._5, x._6.mkString(","))
        // then print features:
        for ((feature, (fType, value)) <- x._7) {
          if (fType == "S" || fType == "N") {
            str1 += " %s=%s".format(feature, value)
          } else if (fType == "SS" || fType == "NS") {
            val cVal = value.asInstanceOf[java.util.List[String]]
            str1 += " %s=%s".format(feature, cVal.mkString(","))
          }
        }
        str1
      }).collect().sorted

      for (str <- sampleStrs)
        println(str)
    }
  }

  // Start program:

  val tmpdir = "/mnt/TimeSeriesSpark-unstable/spark-temp-plots/"
  val samplefile = "cached-samples.dat"
  val regfile = "cached-regs.dat"

  val start = DynamoAnalysisUtil.start()

  var master = "local[2]"
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

  val sc = TimeSeriesSpark.init(master, "default", this.getClass().getName())

  printRegs(regfile, samplefile)

  DynamoAnalysisUtil.finish(start)
}
  