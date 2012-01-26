package edu.berkeley.cs.amplab

package object carat {
  // For getting data:
  val registrationTable = "carat.registrations"
  val samplesTable = "carat.samples"
    
  val regsUuid = DynamoDbEncoder.uuId
  val regsModel = "platformId"
  val regsOs = "systemVersion"

  val sampleKey = DynamoDbEncoder.uuId
  val sampleTime = "timestamp"
  val sampleProcesses = "piList"
  val sampleBatteryState = "batteryState"
  val sampleBatteryLevel = "batteryLevel"
  val sampleEvent = "triggeredBy"

}