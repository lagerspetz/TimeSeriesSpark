package edu.berkeley.cs.amplab

import spark.RDD
import scala.collection.immutable.TreeMap

/**
 * The main analysis body is in StoredSampleAnalysisGeneric.
 * It should be used for all the live analysis, plotting, and speed testing classes.
 */
package object carat {

  val CARAT = "Carat"
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

  def similarityCount(numberOfApps: Double) = math.log(numberOfApps)

  def flatten(filtered: RDD[(String, TreeMap[Double, Double])]) = {
    // there are x treemaps. We need to flatten them but include the uuid.
    filtered.flatMap(x => {
      var result = new TreeMap[Double, Double]
      for (k <- x._2)
        result += ((k._1, k._2))
      result
    }).collect()
  }
  
  def flatten(structured: RDD[(String, Seq[CaratRate])]) = {
    // there are x treemaps. We need to flatten them but include the uuid.
    structured.flatMap(x => {x._2}).collect()
  }
}
