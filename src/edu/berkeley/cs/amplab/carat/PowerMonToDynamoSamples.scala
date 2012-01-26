package edu.berkeley.cs.amplab.carat

import spark._
import spark.SparkContext._
import spark.timeseries._
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat

object PowerMonToDynamoSamples {

  def main(args: Array[String]): Unit = {
   if (args.length < 2) {
      println("Usage: S3DataAnalysis master caratDataFile.txt\n" +
        "Example: S3DataAnalysis local[1] caratDataFile.txt")
      return
    }
    val sc = new SparkContext(args(0), "CaratDataAnalysis")
    val caratDataFile = args(1)

    convertToDynamoSamples(sc, caratDataFile)
    sys.exit(0)
  }

  def convertToDynamoSamples(sc:SparkContext, caratDataFile:String){
    registerFakeUuids()
    val samples = sc.textFile(caratDataFile).map(uidMapper)
    // parallel add to Dynamo
    samples.foreach(x => {
      DynamoDbEncoder.put(samplesTable,
          x._1, x._2, x._3, x._4, x._5, x._6)
    })
  }

  def registerFakeUuids() {
    DynamoDbEncoder.put(registrationTable,
      (regsUuid, "85"),
      (regsOs, "5.0.1"),
      (regsModel, "iPhone 4S"))
    DynamoDbEncoder.put(registrationTable,
      (regsUuid, "46"),
      (regsOs, "7.0.1RC1"),
      (regsModel, "LG Optimus 2X"))
  }

  def uidMapper(x: String) = {
    val arr = x.trim().split(",[ ]*")
    val dfs = "EEE MMM dd HH:mm:ss zzz yyyy"
    val df = new SimpleDateFormat(dfs)
    val uuId = arr(1)
    val timestamp = df.parse(arr(0)).getTime()
    val batteryLevel = arr(2).toDouble
    val events = arr(3).trim().toLowerCase().split(" ")
    val triggeredBy = events(0)
    val batteryState = {
      if (!events.contains("pluggedin"))
        "unplugged"
      else
        "charging"
    }
    var apps = new Array[String](arr.length - 4)
    Array.copy(arr, 4, apps, 0, apps.length)
    apps = apps.map("1234;"+_)

    /*
    "uuId" = String
    "timestamp" = Number
    "piList" = StringSet with each element like "pid;appname"
    "batteryState" = String
    "batteryLevel" = Number
    "triggeredBy" = String
    */
    ((sampleKey, uuId), (sampleTime, timestamp), (sampleProcesses, apps), (sampleBatteryState, batteryState),(sampleBatteryLevel, batteryLevel), (sampleEvent, triggeredBy))
  }
}
