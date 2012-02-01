package edu.berkeley.cs.amplab.carat.dynamodb

import edu.berkeley.cs.amplab.carat._
import com.amazonaws.services.dynamodb.model.AttributeValue
import collection.JavaConversions._

/**
 * Program to copy analyzed data of a certain uuid under osVersion 5.0 for Simulator data access.
 */
object CopyAnalyzedData {
  def main(args: Array[String]) {
    if (args != null && args.length == 1){
      copyResultsData(args(0))
      copyOsData()
      copyModelData()
    }
  }

  def copyResultsData(uuId: String) {
    if (uuId != null) {
      val simUuid = "4EA21A48-AF1D-4C2D-A3A7-91D7F2857A10"
      var (key, res) = DynamoDbDecoder.getItems(resultsTable, uuId)
      for (x <- res) {
        if (x.containsKey(resultKey))
          x.put(resultKey, new AttributeValue(simUuid))
      }
      DynamoDbEncoder.putItems(resultsTable, res)

      while (key != null) {
        val (key2, res2) = DynamoDbDecoder.getItems(resultsTable, uuId, key)
        key = key2
        res = res2
        for (x <- res) {
          if (x.containsKey(resultKey))
            x.put(resultKey, new AttributeValue(simUuid))
        }
        DynamoDbEncoder.putItems(resultsTable, res)
      }
    }
  }

  def copyOsData() {
    val simOs = "5.0"
    val realOs = "5.0.1"

    var (key, res) = DynamoDbDecoder.getItems(osTable, realOs)
    for (x <- res) {
      if (x.containsKey(osKey))
        x.put(osKey, new AttributeValue(simOs))
    }
    DynamoDbEncoder.putItems(osTable, res)

    while (key != null) {
      val (key2, res2) = DynamoDbDecoder.getItems(osTable, realOs, key)
      key = key2
      res = res2
      for (x <- res) {
        if (x.containsKey(osKey))
          x.put(osKey, new AttributeValue(simOs))
      }
      DynamoDbEncoder.putItems(osTable, res)
    }
  }

  def copyModelData() {
    val simModel = "Simulator"
      val realModel = "iPhone 4S"

    var (key, res) = DynamoDbDecoder.getItems(modelsTable, realModel)
    for (x <- res) {
      if (x.containsKey(modelKey))
        x.put(osKey, new AttributeValue(simModel))
    }
    DynamoDbEncoder.putItems(modelsTable, res)

    while (key != null) {
      val (key2, res2) = DynamoDbDecoder.getItems(modelsTable, realModel, key)
      key = key2
      res = res2
      for (x <- res) {
        if (x.containsKey(modelKey))
          x.put(modelKey, new AttributeValue(simModel))
      }
      DynamoDbEncoder.putItems(modelsTable, res)
    }
  }
}
