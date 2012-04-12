package edu.berkeley.cs.amplab.carat.tools

import spark._
import spark.SparkContext._
import spark.timeseries.TimeSeriesSpark
import edu.berkeley.cs.amplab.carat.CaratRate
import edu.berkeley.cs.amplab.carat.dynamodb.DynamoAnalysisUtil
import scala.collection.mutable.Buffer
import collection.JavaConversions._

object GenerateSamples extends App {

  // Constant for how much to add to each timestamp per multiplication
  val MONTH = 3600.0 * 24 * 31

  /**
   * Define a utility function to get regs and samples to start with:
   */
  def getRealSamples(rfile: String, rfile2: String) = {
    val f = new java.io.File(tmpdir + rfile)

    // return old samples/regs if the file is there, else contents of DynamoDB
    if (!f.exists()) {
      DynamoAnalysisUtil.getSamples(sc, tmpdir, true)
    } else {
      (sc.objectFile[(String, String, String, Double)](rfile),
        sc.objectFile[(String, String, String, String, String, Buffer[String], scala.collection.immutable.Map[String, (String, Object)])](rfile2))
    }
  }
  
  /**
   * Create fake regs by taking the original regs and adding numbers to the end of each uuid, and incrementing each timestamp by a month.
   * This is done `mul` times, and the result is an RDD of regs `mul`+1 times the size of the original.
   */

  def fabricateRegs(mul: Int, regs: RDD[(String, String, String, Double)]) = {
    var fakeRegs = regs
    fakeRegs ++= regs.map(x => {
      (x._1 + "" + 1, x._2, x._3, x._4 + MONTH * 1)
    })

    for (i <- 2 until mul + 1) {
      fakeRegs ++= regs.map(x => {
        (x._1 + "" + i, x._2, x._3, x._4 + MONTH * i)
      })
    }
    fakeRegs
  }

  /**
   * Create fake samples by taking the original samples and adding numbers to the end of each uuid, and incrementing each timestamp by a month.
   * This is done `mul` times, and the result is an RDD of samples `mul`+1 times the size of the original.
   */
  
  def fabricateSamples(mul: Int, samples: RDD[(String, String, String, String, String, Buffer[String], scala.collection.immutable.Map[String, (String, Object)])]) = {
    var fakeSamples = samples
    fakeSamples ++= samples.map(x => {
     (x._1+""+1, (x._2.toDouble + MONTH).toString, x._3, x._4, x._5, x._6, x._7)
    })

    for (i <- 2 until mul + 1) {
      fakeSamples ++= samples.map(x => {
        (x._1+""+i, (x._2.toDouble + i*MONTH).toString, x._3, x._4, x._5, x._6, x._7)
      })
    }
    fakeSamples
  }

  // Start program:

  val tmpdir = "/mnt/TimeSeriesSpark-unstable/spark-temp-plots/"
  val samplefile = "cached-samples.dat"
  val regfile = "cached-regs.dat"

  val fakesamples = "fake-samples.dat"
  val fakeregs = "fake-regs.dat"

  val start = DynamoAnalysisUtil.start()

  var master = "local[2]"
   var n = 20
  if (args != null && args.length >= 1) {
    master = args(0)
  }
  
  if (args != null && args.length >= 2) {
    n = args(1).toInt
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

  val (realRegs, realSamples) = getRealSamples(regfile, samplefile)
  
  // multiply n times to get fake samples, and save them to a file:
  val fakeRegs = fabricateRegs(n, realRegs)
  val fakeSamples = fabricateSamples(n, realSamples)
  // delete old fake sample folders:
  val rem = Runtime.getRuntime().exec(Array("/bin/rm", "-rf", fakeregs))
  rem.waitFor()
  val rem2 = Runtime.getRuntime().exec(Array("/bin/rm", "-rf", fakesamples))
  rem2.waitFor()
  fakeRegs.saveAsObjectFile(fakeregs)
  fakeSamples.saveAsObjectFile(fakesamples)
  
  DynamoAnalysisUtil.finish(start)
}
  