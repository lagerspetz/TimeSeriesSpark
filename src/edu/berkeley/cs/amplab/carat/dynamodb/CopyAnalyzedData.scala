package edu.berkeley.cs.amplab.carat.dynamodb

import edu.berkeley.cs.amplab.carat._
import com.amazonaws.services.dynamodb.model.AttributeValue
import collection.JavaConversions._

/**
 * Program to copy analyzed data of a certain uuid under osVersion 5.0 for Simulator data access.
 */
object CopyAnalyzedData {
  def main(args: Array[String]) {
    if (args != null && args.length == 1) {
      copyResultsData(args(0))
      copyOsData()
      copyModelData()
    }
  }

  def copyResultsData(uuId: String) {
    if (uuId != null) {
      println("Getting stuff from resultsTable")
      val simUuid = "4EA21A48-AF1D-4C2D-A3A7-91D7F2857A10"
      var res = DynamoDbDecoder.getItem(resultsTable, uuId)
      if (res.containsKey(resultKey)) {
        res.put(resultKey, new AttributeValue(simUuid))
      }
      println("Putting stuff to resultsTable")
      DynamoDbEncoder.putItem(resultsTable, res)
    }
  }

  def copyOsData() {
    val simOs = "5.0"
    val realOs = "5.0.1"
    println("Getting stuff from osTable")
    var x = DynamoDbDecoder.getItem(osTable, realOs)
    if (x.containsKey(osKey))
      x.put(osKey, new AttributeValue(simOs))
    println("Putting stuff to osTable")
    DynamoDbEncoder.putItem(osTable, x)
  }

  def copyModelData() {
    val simModel = "Simulator"
    val realModel = "iPhone 4S"
    println("Getting stuff from modelsTable")
    var x = DynamoDbDecoder.getItem(modelsTable, realModel)
    if (x.containsKey(modelKey))
      x.put(modelKey, new AttributeValue(simModel))
    println("Putting stuff to modelsTable")
    DynamoDbEncoder.putItem(modelsTable, x)
  }
}
