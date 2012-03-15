package edu.berkeley.cs.amplab.carat.plot

import spark.timeseries._
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
import edu.berkeley.cs.amplab.carat.CaratRate
import spark.RDD

/**
 * Do the exact same thing as in CaratDynamoDataToPlots, but do not collect() and write plot files and run plotting in the end.
 *
 * @author Eemil Lagerspetz
 */

object PlotUtil {

  val dfs = "yyyy-MM-dd"
  val df = new SimpleDateFormat(dfs)
  val dateString = "plots-" + df.format(System.currentTimeMillis())

  val DATA_DIR = "data"
  val PLOTS = "plots"
  val PLOTFILES = "plotfiles"

  val Bug = "Bug"
  val Hog = "Hog"
  val Sim = "Sim"
  val Pro = "Pro"

  val BUGS = "bugs"
  val HOGS = "hogs"
  val SIM = "similarApps"
  val UUIDS = "uuIds"

  def plot(plotDirectory:String, title: String, titleNeg: String, xmax: Double, distWith: Array[(Double, Double)],
    distWithout: Array[(Double, Double)],
    ev: Double, evNeg: Double, evDistance: Double,
    osCorrelations: Map[String, Double], modelCorrelations: Map[String, Double],
    userCorrelations: Map[String, Double],
    usersWith: Int, usersWithout: Int, uuid: String, decimals: Int,
    apps: Seq[String] = null) {
    plotSerial(plotDirectory, title, titleNeg, xmax, distWith, distWithout, ev, evNeg, evDistance, osCorrelations, modelCorrelations,
      userCorrelations,
      usersWith, usersWithout, uuid, decimals, apps)
  }

  /**
   * The J-Score is the % of people with worse = higher energy use.
   * therefore, it is the size of the set of evDistances that are higher than mine,
   * compared to the size of the user base.
   * Note that the server side multiplies the JScore by 100, and we store it here
   * as a fraction.
   */
  def plotJScores(plotDirectory:String, 
      allRates:RDD[CaratRate], aPrioriDistribution: scala.collection.mutable.Map[Double, Double],
      distsWithUuid: TreeMap[String, Array[(Double, Double)]],
    distsWithoutUuid: TreeMap[String, Array[(Double, Double)]],
    parametersByUuid: TreeMap[String, (Double, Double, Double)],
    evDistanceByUuid: TreeMap[String, Double],
    appsByUuid: TreeMap[String, Set[String]],
    uuidToOsAndModel: scala.collection.mutable.HashMap[String, (String, String)],
    decimals: Int) {
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
      if (distWith != null && distWithout != null && apps != null)
        plot(plotDirectory, "Profile for " + k, "Other users", xmax, distWith, distWithout, ev, evNeg, jscore, null, null, null, 0, 0, k, decimals, apps.toSeq)
      else
        printf("Error: Could not plot jscore, because: distWith=%s distWithout=%s apps=%s\n", distWith, distWithout, apps)
    }
  }

  def plotSerial(plotDirectory:String, title: String, titleNeg: String, xmax: Double, distWith: Array[(Double, Double)],
    distWithout: Array[(Double, Double)],
    ev: Double, evNeg: Double, evDistance: Double,
    osCorrelations: Map[String, Double], modelCorrelations: Map[String, Double],
    userCorrelations: Map[String, Double],
    usersWith: Int, usersWithout: Int, uuid: String, decimals: Int,
    apps: Seq[String] = null) {
    println("Plotting %s vs %s xmax=%s ev=%s evWithout=%s evDistance=%s osCorrelations=%s modelCorrelations=%s uuid=%s".format(
      title, titleNeg, xmax, ev, evNeg, evDistance, osCorrelations, modelCorrelations, uuid))
    plotRegular(plotDirectory, title, titleNeg, xmax, distWith, distWithout, ev, evNeg, evDistance, osCorrelations, modelCorrelations,
      userCorrelations,
      usersWith, usersWithout, uuid, decimals, apps)
    plotBucketed(plotDirectory, title, titleNeg, xmax, distWith, distWithout, ev, evNeg, evDistance, decimals)
    plotLogBucketed(plotDirectory, title, titleNeg, xmax, distWith, distWithout, ev, evNeg, evDistance, decimals)
  }
  
  def plotRegular(plotDirectory:String, title: String, titleNeg: String, xmax: Double, distWith: Array[(Double, Double)],
    distWithout: Array[(Double, Double)],
    ev: Double, evNeg: Double, evDistance: Double,
    osCorrelations: Map[String, Double], modelCorrelations: Map[String, Double],
    userCorrelations: Map[String, Double],
    usersWith: Int, usersWithout: Int, uuid: String, decimals: Int,
    apps: Seq[String] = null) {
    var fixedTitle = title
    if (title.startsWith("Hog "))
      fixedTitle = title.substring(4)
    else if (title.startsWith("Bug "))
      fixedTitle = title.substring(4)
    // bump up accuracy here so that not everything gets blurred
    val evTitle = fixedTitle + " (EV=" + ProbUtil.nDecimal(ev, decimals + 1) + ")"
    val evTitleNeg = titleNeg + " (EV=" + ProbUtil.nDecimal(evNeg, decimals + 1) + ")"
    plotFile(plotDirectory, dateString, title, evTitle, evTitleNeg, xmax)
    writeData(dateString, evTitle, distWith)
    writeData(dateString, evTitleNeg, distWithout)
    if (osCorrelations != null) {
      var stuff = uuid + "\nevWith=%s\nevWithout=%s".format(ev, evNeg)
      writeCorrelationFile(plotDirectory, title, osCorrelations, modelCorrelations, userCorrelations, usersWith, usersWithout, stuff)
    }
    plotData(dateString, title)
  }
  
  def plotBucketed(plotDirectory:String, title: String, titleNeg: String, xmax: Double, distWithReg: Array[(Double, Double)],
    distWithoutReg: Array[(Double, Double)],
    ev: Double, evNeg: Double, evDistance: Double, decimals: Int) {
    val buckets = 100
    val (xmax,distWith,distWithout) = ProbUtil.bucketDistributionsByX(distWithReg, distWithoutReg, buckets)
    var fixedTitle = title
    if (title.startsWith("Hog "))
      fixedTitle = title.substring(4)
    else if (title.startsWith("Bug "))
      fixedTitle = title.substring(4)
    // bump up accuracy here so that not everything gets blurred
    val evTitle = fixedTitle + "B (EV=" + ProbUtil.nDecimal(ev, decimals + 1) + ")"
    val evTitleNeg = titleNeg + "B (EV=" + ProbUtil.nDecimal(evNeg, decimals + 1) + ")"
    plotFile(plotDirectory, dateString, title +"bucketed", evTitle, evTitleNeg, xmax, true)
    writeDataBucketed(dateString, evTitle, distWith, xmax, buckets)
    writeDataBucketed(dateString, evTitleNeg, distWithout, xmax, buckets)

    plotData(dateString, title +"bucketed")
  }
  
  def plotLogBucketed(plotDirectory:String, title: String, titleNeg: String, xmax: Double, distWithReg: Array[(Double, Double)],
    distWithoutReg: Array[(Double, Double)],
    ev: Double, evNeg: Double, evDistance: Double, decimals:Int) {
    val smallestBucket = 0.0001
    val buckets = 100
    val (distWith,distWithout) = ProbUtil.logBucketDists(distWithReg, distWithoutReg, xmax, buckets, smallestBucket)
    var fixedTitle = title
    if (title.startsWith("Hog "))
      fixedTitle = title.substring(4)
    else if (title.startsWith("Bug "))
      fixedTitle = title.substring(4)
    // bump up accuracy here so that not everything gets blurred
    val evTitle = fixedTitle + "LB (EV=" + ProbUtil.nDecimal(ev, decimals + 1) + ")"
    val evTitleNeg = titleNeg + "LB (EV=" + ProbUtil.nDecimal(evNeg, decimals + 1) + ")"
    plotFile(plotDirectory, dateString, title +"logbucketed", evTitle, evTitleNeg, xmax)
     
    val logBase = ProbUtil.getLogBase(buckets, smallestBucket, xmax)
    writeDataLogBucketed(dateString, evTitle, distWith, xmax, buckets, logBase)
    writeDataLogBucketed(dateString, evTitleNeg, distWithout, xmax, buckets, logBase)

    plotData(dateString, title +"logbucketed")
  }

  def plotFile(plotDirectory:String, dir: String, name: String, t1: String, t2: String, xmax: Double, bucketed:Boolean = false) = {
    val pdir = dir + "/" + PLOTS + "/"
    val gdir = dir + "/" + PLOTFILES + "/"
    val ddir = dir + "/" + DATA_DIR + "/"
    var f = new File(pdir)
    if (!f.isDirectory() && !f.mkdirs())
      println("Failed to create " + f + " for plots!")
    else {
      f = new File(gdir)
      if (!f.isDirectory() && !f.mkdirs())
        println("Failed to create " + f + " for plots!")
      else {
        f = new File(ddir)
        if (!f.isDirectory() && !f.mkdirs())
          println("Failed to create " + f + " for plots!")
        else {
          val plotfile = new java.io.FileWriter(gdir + name + ".gnuplot")
          plotfile.write("set term postscript eps enhanced color 'Helvetica' 32\nset xtics out\n" +
            "set size 1.93,1.1\n" +
            {
              if (bucketed)
                ""
              else
                "set logscale x\nset xtics 0.0005,2," + (xmax + 0.001) + "\n"
            } +
            "set xrange [0.0005:" + (xmax + 0.001) + "]\n" +
            "set xlabel \"Battery drain % / s\"\n" +
            "set ylabel \"Probability\"\n")
          if (plotDirectory != null)
            plotfile.write("set output \"" + plotDirectory + "/" + assignSubDir(plotDirectory, name) + name + ".eps\"\n")
          else
            plotfile.write("set output \"" + pdir + name + ".eps\"\n")
          plotfile.write("plot \"" + ddir + t1 + ".txt\" using 1:2 with linespoints lt rgb \"#f3b14d\" ps 3 lw 5 title \"" + t1.replace("~", "\\\\~").replace("_", "\\\\_") +
            "\", " +
            "\"" + ddir + t2 + ".txt\" using 1:2 with linespoints lt rgb \"#007777\" ps 3 lw 5 title \"" + t2.replace("~", "\\\\~").replace("_", "\\\\_")
            + "\"\n")
          plotfile.close
          true
        }
      }
    }
  }

  def assignSubDir(plotDirectory:String, name: String) = {
    val p = new File(plotDirectory)
    if (!p.isDirectory() && !p.mkdirs()) {
      ""
    } else {
      val dir = name.substring(0, 3) match {
        case Bug => { BUGS }
        case Hog => { HOGS }
        case Pro => { UUIDS }
        case Sim => { SIM }
        case _ => ""
      }
      if (dir.length() > 0) {
        val d = new File(p, dir)
        if (!d.isDirectory() && !d.mkdirs())
          ""
        else
          dir + "/"
      } else
        ""
    }
  }

  def writeData(dir: String, name: String, dist: Array[(Double, Double)]) {
    val ddir = dir + "/" + DATA_DIR + "/"
    var f = new File(ddir)
    if (!f.isDirectory() && !f.mkdirs())
      println("Failed to create " + f + " for plots!")
    else {
      val datafile = new java.io.FileWriter(ddir + name + ".txt")

      val dataPairs = dist.sortWith((x, y) => {
        x._1 < y._1
      })

      for (k <- dataPairs)
        datafile.write(k._1 + " " + k._2 + "\n")
      datafile.close
    }
  }
  
  def writeDataBucketed(dir: String, name: String, dist: TreeMap[Int, Double], xmax:Double, buckets:Int) {
    val ddir = dir + "/" + DATA_DIR + "/"
    var f = new File(ddir)
    if (!f.isDirectory() && !f.mkdirs())
      println("Failed to create " + f + " for plots!")
    else {
      val datafile = new java.io.FileWriter(ddir + name + ".txt")
      
      for (k <- dist){
        val x = (k._1 + 0.5)/buckets * xmax
        datafile.write(x + " " + k._2 +"\n")
      }
      datafile.close
    }
  }
  
  def writeDataLogBucketed(dir: String, name: String, dist: TreeMap[Int, Double], xmax:Double, buckets:Int, logBase:Double) {
    val ddir = dir + "/" + DATA_DIR + "/"
    var f = new File(ddir)
    if (!f.isDirectory() && !f.mkdirs())
      println("Failed to create " + f + " for plots!")
    else {
      val datafile = new java.io.FileWriter(ddir + name + ".txt")

      for (k <- dist) {
        val bucketStart = {
          if (k._1 == 0)
            0.0
          else
            xmax / (math.pow(logBase, buckets - k._1))
        }
        val bucketEnd = xmax / (math.pow(logBase, buckets - k._1 - 1))
        val x = (bucketEnd + bucketStart) / 2
        //println("Bucket from %s to %s".format(bucketStart, bucketEnd))
        println("wrote %s %s".format(x, k._2))
        datafile.write(x + " " + k._2 + "\n")
      }
      datafile.close
    }
  }

  def writeCorrelationFile(plotDirectory:String, name: String,
    osCorrelations: Map[String, Double],
    modelCorrelations: Map[String, Double],
    userCorrelations: Map[String, Double],
    usersWith: Int, usersWithout: Int, uuid: String = null) {
    val pdir = dateString + "/" + PLOTS + "/"
    var path = pdir + name + "-correlation.txt"
    if (plotDirectory != null)
      path = plotDirectory + "/" + assignSubDir(plotDirectory, name) + name + "-correlation.txt"

    var datafile: java.io.FileWriter = null

    if (usersWith != 0 || usersWithout != 0) {
      if (datafile == null) datafile = new java.io.FileWriter(path)
      datafile.write("%s users with\n%s users without\n".format(usersWith, usersWithout))
    }
    if (uuid != null) {
      if (datafile == null) datafile = new java.io.FileWriter(path)
      datafile.write("UUID: %s\n".format(uuid))
    }

    if (modelCorrelations.size > 0 || osCorrelations.size > 0 || userCorrelations.size > 0) {
      if (datafile == null) datafile = new java.io.FileWriter(path)
      if (osCorrelations.size > 0) {
        val arr = osCorrelations.toArray.sortWith((x, y) => { math.abs(x._2) < math.abs(y._2) })
        datafile.write("Correlation with OS versions:\n")
        for (k <- arr) {
          datafile.write(k._2 + " " + k._1 + "\n")
        }
      }

      if (modelCorrelations.size > 0) {
        val mArr = modelCorrelations.toArray.sortWith((x, y) => { math.abs(x._2) < math.abs(y._2) })
        datafile.write("Correlation with device models:\n")
        for (k <- mArr) {
          datafile.write(k._2 + " " + k._1 + "\n")
        }
      }

      if (userCorrelations.size > 0) {
        val uArr = userCorrelations.toArray.sortWith((x, y) => { math.abs(x._2) < math.abs(y._2) })
        datafile.write("Correlation with:\n")
        for (k <- uArr) {
          datafile.write(k._2 + " " + k._1 + "\n")
        }
      }
      datafile.close
    }
  }

  def plotData(dir: String, title: String) {
    val gdir = dir + "/" + PLOTFILES + "/"
    val f = new File(gdir)
    if (!f.isDirectory() && !f.mkdirs())
      println("Failed to create " + f + " for plots!")
    else {
      val temp = Runtime.getRuntime().exec(Array("gnuplot", gdir + title + ".gnuplot"))
      val err_read = new java.io.BufferedReader(new java.io.InputStreamReader(temp.getErrorStream()))
      var line = err_read.readLine()
      while (line != null) {
        println(line)
        line = err_read.readLine()
      }
      temp.waitFor()
    }
  }

  def plotSamples(title: String, data: TreeMap[String, TreeSet[Double]]) {
    println("Plotting samples.")
    writeSampleData(dateString, title, data)
  }

  def writeSampleData(dir: String, name: String, data: TreeMap[String, TreeSet[Double]]) {
    val ddir = dir + "/" + DATA_DIR + "/"
    var f = new File(ddir)
    if (!f.isDirectory() && !f.mkdirs())
      println("Failed to create " + f + " for plots!")
    else {
      val datafile = new java.io.FileWriter(ddir + name + ".txt")
      val arr = data.toArray[(String, TreeSet[Double])]
      val ret = arr.sortWith((x, y) => {
        x._2.size > y._2.size
      })
      for (k <- ret)
        for (j <- k._2)
          datafile.write(k._1 + " " + j + "\n")
      datafile.close
    }
  }
}
