package spark.timeseries.examples

import spark._
import spark.SparkContext._
import spark.timeseries._
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import java.util.Date
import java.io.FileWriter

/**
 * A program for matching the energy drain of a Monsoon Power Monitor measurement file
 * with an Android `dumpsys battery` and `date` script. To be extended to match the
 * output of Carat and Power Monitor. Produces data files of the format
 * {{{
 * date time, battery percentage
 * }}}
 * For example:
 * {{{
 * 2011-12-30 09:40:23.9876, 99
 * 2011-12-30 09:41, 98
 * 2011-12-30 09:51:23.77, 97
 * 2011-12-30 10:51:23.90, 80
 * }}}
 *
 * Usage:
 * {{{
 *  BatterLevel.main(Array("dumpsys-script.txt","powermon-file.pt4","1250")
 * }}}
 * The results will be called dumpsys-script-data.txt and powermon-file-data.txt.
 */

object DumpsysCaratRates {

  /**
   * Main program entry point. Parses the files given on the command line and saves results as
   * `dumpsys-script-battery-level.txt` and `powermon-file-battery-level.txt`.
   */

  def getAppstr(d: Date, startdate: Date) = {
    val bej = new java.util.Date(startdate.getTime() + 30 * 60 * 1000)
    val idle2 = new java.util.Date(bej.getTime() + 30 * 60 * 1000)
    val appstore = new java.util.Date(idle2.getTime() + 30 * 60 * 1000)
    val yle = new java.util.Date(appstore.getTime() + 15 * 60 * 1000)
    val skype = new java.util.Date(yle.getTime() + 30 * 60 * 1000)
    val ktdict = new java.util.Date(skype.getTime() + 15 * 60 * 1000)
    val idle3 = new java.util.Date(ktdict.getTime() + 30 * 60 * 1000)
    val carat = new java.util.Date(idle3.getTime() + 30 * 60 * 1000)
    val angry = new java.util.Date(carat.getTime() + 5 * 60 * 1000)
    val idle4 = new java.util.Date(angry.getTime() + 15 * 60 * 1000)
    val appsIdle = "Safari, Mail"
    var appsStr = appsIdle

    if (d.before(bej) ||
      (d.before(appstore) && d.after(idle2)) ||
      (d.before(carat) && d.after(idle3)) ||
      d.after(idle4)) {
      appsStr = appsIdle
    } else if (d.after(bej) && d.before(idle2)) {
      appsStr = appsIdle + ", Bejeweled Blitz"
    } else if (d.after(appstore) && d.before(yle)) {
      appsStr = appsIdle + ", App Store"
    } else if (d.after(yle) && d.before(skype)) {
      appsStr = appsIdle + ", Yle Areena"
    } else if (d.after(skype) && d.before(ktdict)) {
      appsStr = appsIdle + ", Skype"
    } else if (d.after(ktdict) && d.before(idle3)) {
      appsStr = appsIdle + ", KTDict"
    } else if (d.after(carat) && d.before(angry)) {
      appsStr = appsIdle + ", Carat"
    } else if (d.after(angry) && d.before(idle4)) {
      appsStr = appsIdle + ", Angry Birds"
    }

    appsStr
  }

  def dumpsys(sc: SparkContext, dumpsysFile: String, uid: Int) = {
    var dot = dumpsysFile.lastIndexOf('.')

    if (dot < 1)
      dot = dumpsysFile.length

    val dumpsysData = dumpsysFile.substring(0, dot) + "-carat-data.txt"

    val (timezone, startdate, startbattery, values) =
      BatteryLevel.handleDumpsysFile(sc, dumpsysFile)

    val dfs = "EEE MMM dd HH:mm:ss zzz yyyy"
    val out = new SimpleDateFormat(dfs)
    out.setTimeZone(timezone)
    val fw = new FileWriter(dumpsysData)

    var eventsStr = "batteryStatusChanged"

    var first = true
    for (k <- values) {
      if (first) {
        eventsStr += " unplugged"
        first = false
      } else {
        eventsStr = "batteryStatusChanged"
        fw.write("\n")
      }
      val appsStr = getAppstr(k._1, startdate)

      if (k == values.last)
        eventsStr += " unplugged"
      //Mon Dec 26 09:43:36 PST 2011, 46, 99, batteryStatusChanged unplugged, Safari, Mail
      fw.write((out format k._1.getTime) + ", " + uid + ", " +
        k._2 + ", " + eventsStr + ", " + appsStr)
      //println((out format k._1.getTime)+", " + k._2)
    }
    fw.close()

    (timezone, startdate, startbattery)
  }

  def powerMon(sc: SparkContext, powerMonFile: String, startdate: java.util.Date, startbattery: Int, batteryCapacity: Int,
    timezone: java.util.TimeZone, uid: Int) {
    var dot = powerMonFile.lastIndexOf('.')
    if (dot < 1)
      dot = powerMonFile.length

    val powerMonData = powerMonFile.substring(0, dot) + "-carat-data.txt"

    val powerMonitorValues =
      BatteryLevel.handlePowerMonitorFile(sc, startdate,
        startbattery, powerMonFile, batteryCapacity)

    val dfs = "EEE MMM dd HH:mm:ss zzz yyyy"
    val out = new SimpleDateFormat(dfs)
    out.setTimeZone(timezone)
    val fw2 = new FileWriter(powerMonData)

    var eventsStr = "batteryStatusChanged"

    var first = true
    for (k <- powerMonitorValues) {
      if (first) {
        eventsStr += " unplugged"
        first = false
      } else {
        eventsStr = "batteryStatusChanged"
        fw2.write("\n")
      }
      val d = out.parse(out.format(k._1))
      val appsStr = getAppstr(d, startdate)
      fw2.write((out format k._1) + ", " + uid + ", " +
        k._2 + ", " + eventsStr + ", " + appsStr)
    }
    fw2.close()
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: BatteryLevel master dumpsys-script.txt powermon-file.csv apps.txt batterycapacity\n" +
        "Example: BatteryLevel local[1] dumpsys-script.txt powermon-file.csv apps.txt 1250")
      return
    }
    val uid = 46

    val sc = new SparkContext(args(0), "BatteryLevel")
    val dumpsysFile = args(1)
    val powerMonFile = args(2)
    val eventsFile = args(3)
    val batteryCapacity = args(4) toInt

    val (timezone, startdate, startbattery) = dumpsys(sc, dumpsysFile, uid)

    println("Startdate: " + startdate)

    powerMon(sc, powerMonFile, startdate, startbattery, batteryCapacity, timezone, uid)

    System.exit(0)
  }
}
