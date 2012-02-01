package edu.berkeley.cs.amplab.carat.dynamodb

import edu.berkeley.cs.amplab.carat._

import com.amazonaws.services.dynamodb.model.DescribeTableRequest
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput
import com.amazonaws.services.dynamodb.model.UpdateTableRequest
import collection.JavaConversions._

object UpdateThroughput{

 def main(args: Array[String]){
    if (args != null && args.length == 3)
      updateTableThroughput(args(0), args(1).toLong, args(2).toLong)
 }

 def updateTableThroughput(t: String, read: Long = 0L, write:Long = 0L) {
      
      val v = DynamoDbEncoder.dd.describeTable(new DescribeTableRequest().withTableName(t))
      val tp = v.getTable().getProvisionedThroughput()
      val rd = tp.getReadCapacityUnits()
      val wr = tp.getWriteCapacityUnits()
      
      var uRead = math.max(read, rd)
      var uWrite = math.max(write, wr)

      if (rd < uRead || wr < uWrite) {
        if (rd*2 < uRead){
          uRead = rd*2
          println("Warning: only increasing read cap of " + t + " to "+ uRead +" due to DynamoDb limits.")
        }
         if (wr*2 < uWrite){
          uWrite = wr*2
          println("Warning: only increasing write cap of " + t + " to "+ uWrite +" due to DynamoDb limits.")
        }
        val k = new UpdateTableRequest().withTableName(t).withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(uRead).withWriteCapacityUnits(uWrite))
        val upd = DynamoDbEncoder.dd.updateTable(k)
        // apparently only one table can be updated "simultaneously" whatever that means.
        Thread.sleep(40000)
      }else{
        println("Doing nothing, current tp for " + t +" is: " + rd + ", " + wr)
      }
  }
}
