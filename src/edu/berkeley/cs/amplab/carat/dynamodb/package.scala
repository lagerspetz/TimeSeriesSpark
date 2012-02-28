package edu.berkeley.cs.amplab.carat

import spark.RDD
import scala.collection.immutable.TreeMap
package object dynamodb {
  // Carat Website bucket
  val BUCKET_WEBSITE = "carat.website"

  // Daemons file on S3
  val DAEMON_FILE = "daemons.txt"

  val UUIDString = "uuId"

  val resultsTable = "carat.latestresults"
  val resultKey = UUIDString
  
  val similarsTable = "carat.similarusers"
  val similarKey = UUIDString
  
  val hogsTable = "carat.latestapps"
  val hogKey = "appName"

  val bugsTable = "carat.latestbugs"
  val expectedValue = "expectedValue"
  val expectedValueNeg = expectedValue+"Neg"

  val modelsTable = "carat.latestmodels"
  val modelKey = "model"

  val osTable = "carat.latestos"
  val osKey = "os"

  // For getting data:
  val registrationTable = "carat.registrations"
  val samplesTable = "carat.samples"

  val regsUuid = UUIDString
  val regsModel = "platformId"
  val regsTimestamp = "timestamp"
  val regsOs = "osVersion"

  val sampleKey = regsUuid
  val sampleTime = regsTimestamp
  val sampleProcesses = "processList"
  val sampleBatteryState = "batteryState"
  val sampleBatteryLevel = "batteryLevel"
  val sampleEvent = "triggeredBy"
}
