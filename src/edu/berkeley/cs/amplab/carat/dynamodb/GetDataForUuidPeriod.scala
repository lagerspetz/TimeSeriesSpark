package edu.berkeley.cs.amplab.carat.dynamodb

import edu.berkeley.cs.amplab.carat._
import com.amazonaws.services.dynamodb.model.AttributeValue
import collection.JavaConversions._

/**
 * Program to delete items of a certain uuid, for removing simulator data.
 */
object GetDataForUuidPeriod {
  def main(args: Array[String]) {
    if (args != null && args.length >= 1) {
      getDataForUuid(args(0))
    }
  }

  def getDataForUuid(uuId: String) {
    if (uuId != null) {
      println("Getting stuff from "+samplesTable)
      var (key, res) = DynamoDbDecoder.getItemsAfterRangeKey(samplesTable, uuId, null)
      for (k <- res){
        val key = k.get(sampleKey).getS()
        val time = k.get(sampleTime).getN().toDouble
        val bl = k.get(sampleBatteryLevel).getN().toDouble
        val bs = k.get(sampleBatteryState).getS()
        printf("key %s time %s bl %s bs %s\n", key, time, bl, bs)
      }
    }
  }
}
