package edu.berkeley.cs.amplab.carat.dynamodb

import edu.berkeley.cs.amplab.carat._
import com.amazonaws.services.dynamodb.model.DescribeTableRequest
import collection.JavaConversions._

/**
 * Program to list DynamoDb tables.
 */
object DescribeTables {
  def main(args: Array[String]) {
    val tables = DynamoDbEncoder.dd.listTables().getTableNames()
    describeTables(tables: _*)
  }

  def describeTables(tables: String*) {
    var READ = 60L
    var WRITE = 60L
    for (t <- tables) {
      val v = DynamoDbEncoder.dd.describeTable(new DescribeTableRequest().withTableName(t)).getTable()
      println(v)
    }
  }
}