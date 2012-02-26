package edu.berkeley.cs.amplab.carat.dynamodb

import edu.berkeley.cs.amplab.carat._
import collection.JavaConversions._

object RemoveDaemons extends App {
  // Daemons list, read from S3
  DynamoAnalysisUtil.removeDaemons(CaratDynamoDataAnalysis.DAEMONS_LIST)
}
