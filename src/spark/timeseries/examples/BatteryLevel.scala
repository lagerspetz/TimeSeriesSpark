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

object BatteryLevel {

  def handleDumpsysFile(sc: SparkContext, dumpsysFile: String) = {
    val file = sc.textFile(dumpsysFile)
    val mapped = file.filter(x => { x.contains("level:") || (x matches ".*[0-9]{2}:[0-9]{2}.*") }).collect()
    var values = new ArrayBuffer[(java.util.Date, Int)]

    val formatString = "EEE MMM dd HH:mm:ss zzz yyyy"
    val df = new SimpleDateFormat(formatString)
    var startdate: Date = null
    var battery = 0
    var startbattery = 0
    println("File size: " + file.count())
    println("mapped size: " + mapped.length)
    for (k <- mapped) {
      if (k contains "level:")
        battery = k.split(" ").last toInt
      else {
        values += ((df.parse(k), battery))
        if (startdate == null) {
          startdate = values.last._1
          startbattery = battery
        }
      }
    }
    (df.getTimeZone(), startdate, startbattery, values)
  }

  def handlePowerMonitorFile(sc: SparkContext, startDate: Date, startBattery: Int, powerMonitorFile: String, batteryCapacity: Int, aux: Boolean = false) = {
    val file = sc.textFile(powerMonitorFile)
    val mapped = file.map(TimeSeriesSpark.genericMapper(_, ",")).collect()

    var values = new ArrayBuffer[(Long, Double)]

    val span = 60 * 5000

    // for every line
    var prev = 0.0
    var sumA = 0.0
    var sumV = 0.0 // if we ever want mWh
    var count = 0

    // for every span lines
    var startTime = 0.0

    for (k <- mapped) {
      //println("Line: " + k.mkString(", "))
      if (k.length > 2 || (aux && k.length > 3)) {
        count += 1
        if (aux) {
          sumA += k(4)
          sumV += k(5)
        } else {
          sumA += k(2)
          sumV += k(3)
        }
        if (count == span) {
          var usage = (k(1) - startTime) * sumA / span / 3600
          if (aux)
            usage = (k(1) - startTime) * sumA * sumV / 4 / span / 3600
          values += ((startDate.getTime + (k(1) * 1000) toLong, (batteryCapacity - prev - usage) / batteryCapacity * startBattery))
          count = 0
          startTime = k(1)
          sumA = 0.0
          sumV = 0.0
          prev = prev + usage
        }
      } else if (k.length == 2 || (aux && k.length == 3)) {
        count += 1
        if (aux) {
          sumA += k(1)
          sumV += k(2)
        } else {
          sumA += k(1)
        }
        //sumV += k(3)
        if (count == span) {
          var usage = (k(0) - startTime) * sumA / span / 3600
          if (aux)
            usage = (k(0) - startTime) * sumA * sumV / 4 / span / 3600
          values += ((startDate.getTime + (k(0) * 1000) toLong, (batteryCapacity - prev - usage) / batteryCapacity * startBattery))
          count = 0
          startTime = k(0)
          sumA = 0.0
          sumV = 0.0
          prev = prev + usage
        }
      }
    }

    if (count < span && count != 0 && (mapped.last.length > 2 || (aux && mapped.last.length > 3))) {
      val k = mapped.last
      var usage = (k(1) - startTime) * sumA / span / 3600
      if (aux)
        usage = (k(1) - startTime) * sumA * sumV / 4 / span / 3600
      values += ((startDate.getTime + (k(1) * 1000) toLong, (batteryCapacity - prev - usage) / batteryCapacity * startBattery))
    } else if (count < span && count != 0 && (mapped.last.length == 2 || (aux && mapped.last.length == 3))) {
      val k = mapped.last
      var usage = (k(0) - startTime) * sumA / span / 3600
      if (aux)
        usage = (k(0) - startTime) * sumA * sumV / 4 / span / 3600
      values += ((startDate.getTime + (k(0) * 1000) toLong, (batteryCapacity - prev - usage) / batteryCapacity * startBattery))
    }
    values
  }
  
  def auxPowerMonFileOnly(sc: SparkContext,powerMonFile: String, startDate: Date, startBattery: Int, batteryCapacity: Int) = {
    var dot = powerMonFile.lastIndexOf('.')
    if (dot < 1)
      dot = powerMonFile.length
      
    val powerMonData = powerMonFile.substring(0, dot) + "-battery-level.txt"

    val formatString2 = "yyyy-MM-dd HH:mm:ss.S"
    val out = new SimpleDateFormat(formatString2)

    println("Startdate: " + startDate)
    val powerMonitorValues = handlePowerMonitorFile(sc, startDate, startBattery, powerMonFile, batteryCapacity, true)

    val fw2 = new FileWriter(powerMonData)
    var first = true
    for (k <- powerMonitorValues) {
      if (first) {
        first = false
      } else {
        fw2.write("\n")
      }
      fw2.write((out format k._1) + ", " + k._2)
      //println((out format k._1)+", " + k._2)
    }
    fw2.close()
  }

  /**
   * Main program entry point. Parses the files given on the command line and saves results as
   * `dumpsys-script-battery-level.txt` and `powermon-file-battery-level.txt`.
   */
  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: BatteryLevel master dumpsys-script.txt powermon-file.csv batterycapacity\n" +
        "Example: BatteryLevel local[1] dumpsys-script.txt powermon-file.csv 1250")
      return
    }
    val sc = new SparkContext(args(0), "BatteryLevel")
    val dumpsysFile = args(1)
    val powerMonFile = args(2)
    val batteryCapacity = args(3) toInt

    var dot = dumpsysFile.lastIndexOf('.')
    if (dot < 1)
      dot = dumpsysFile.length

    val dumpsysData = dumpsysFile.substring(0, dot) + "-battery-level.txt"

    dot = powerMonFile.lastIndexOf('.')
    if (dot < 1)
      dot = powerMonFile.length

    val powerMonData = powerMonFile.substring(0, dot) + "-battery-level.txt"

    val (timezone, startdate, startbattery, values) = handleDumpsysFile(sc, dumpsysFile)

    val formatString2 = "yyyy-MM-dd HH:mm:ss.S"
    val out = new SimpleDateFormat(formatString2)
    out.setTimeZone(timezone)
    val fw = new FileWriter(dumpsysData)

    var first = true
    for (k <- values) {
      if (first) {
        first = false
      } else {
        fw.write("\n")
      }
      fw.write((out format k._1.getTime) + ", " + k._2)
      //println((out format k._1.getTime)+", " + k._2)
    }
    fw.close()

    println("Startdate: " + startdate)
    val powerMonitorValues = handlePowerMonitorFile(sc, startdate, startbattery, powerMonFile, batteryCapacity)

    val fw2 = new FileWriter(powerMonData)
    first = true
    for (k <- powerMonitorValues) {
      if (first) {
        first = false
      } else {
        fw2.write("\n")
      }
      fw2.write((out format k._1) + ", " + k._2)
      //println((out format k._1)+", " + k._2)
    }
    fw2.close()

    System.exit(0)
  }
}
