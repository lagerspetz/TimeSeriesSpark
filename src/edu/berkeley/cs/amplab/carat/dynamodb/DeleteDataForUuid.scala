package edu.berkeley.cs.amplab.carat.dynamodb

import edu.berkeley.cs.amplab.carat._
import com.amazonaws.services.dynamodb.model.AttributeValue
import collection.JavaConversions._

/**
 * Program to copy analyzed data of a certain uuid under osVersion 5.0 for Simulator data access.
 */
object DeleteDataForUuid {
  def main(args: Array[String]) {
    if (args != null && args.length == 1) {
      deleteDataForUuid(args(0))
    }
  }

  def deleteDataForUuid(uuId: String) {
    if (uuId != null) {
      println("Getting stuff from "+samplesTable)
      var (key, res) = DynamoDbDecoder.getItems(samplesTable, uuId)
      for (k <- res){
        val key = k.get(sampleKey).getS()
        val time = k.get(sampleTime).getN()
        printf("key %s time %s\n", key, time)
        //DynamoDbDecoder.deleteItem(samplesTable, key, time)
      }
    }
  }
}
