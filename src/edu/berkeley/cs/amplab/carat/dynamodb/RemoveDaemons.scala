package edu.berkeley.cs.amplab.carat.dynamodb

import edu.berkeley.cs.amplab.carat._
import collection.JavaConversions._

object RemoveDaemons extends App {

   // Daemons file on S3
  val DAEMON_FILE = "daemons.txt"
  // Daemons list, read from S3
  val DAEMONS_LIST = CaratDynamoDataAnalysis.DAEMONS_LIST

  if (args != null && args.length >= 1 && args(0) == "DAEMONS") {
    for (k <- DAEMONS_LIST) {
      println("Deleting: " + k)
      DynamoDbDecoder.deleteItem(appsTable, k)
    }
  }
  
  val kd = DAEMONS_LIST.map(x => {
    (appKey, x)
  }).toSeq
  
  val (key, items) = DynamoDbDecoder.filterItems(bugsTable, kd:_*)
  for (k <- items){
    val uuid = k.get(resultKey).getS()
    val app = k.get(appKey).getS()
    println("Removing Bug: %s, %s".format(uuid, app))
    DynamoDbDecoder.deleteItem(bugsTable, uuid, app)
  }
}