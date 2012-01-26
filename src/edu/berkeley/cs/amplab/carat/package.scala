package edu.berkeley.cs.amplab

import spark.RDD
import scala.collection.immutable.TreeMap
package object carat {
    
  val UUIDString = "uuId"
  
  val resultsTable = "carat.latestresults"
  val resultKey = UUIDString
    
  val appsTable = "carat.latestapps"  
  val appKey = "appName"
    
  val bugsTable = "carat.latestbugs"
    
  val modelsTable = "carat.latestmodels"
  val modelKey = "model"
  
  val osTable = "carat.latestos"
  val osKey = "os"
    
  // For getting data:
  val registrationTable = "carat.registrations"
  val samplesTable = "carat.samples"
    
  val regsUuid = "uuid"
  val regsModel = "platformId"
  val regsOs = "systemVersion"

  val sampleKey = regsUuid
  val sampleTime = "timestamp"
  val sampleProcesses = "piList"
  val sampleBatteryState = "batteryState"
  val sampleBatteryLevel = "batteryLevel"
  val sampleEvent = "triggeredBy"

  def flatten(filtered: RDD[(String, TreeMap[Double, Double])]) = {
    // there are x treemaps. We need to flatten them but include the uuid.
    filtered.flatMap(x => {
      var result = new TreeMap[Double, Double]
      for (k <- x._2)
        result += ((k._1, k._2))
      result
    }).collect()
  }
}
