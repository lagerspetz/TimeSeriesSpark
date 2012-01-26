package edu.berkeley.cs.amplab.carat

import com.amazonaws.services.dynamodb.model.AttributeValue
import com.amazonaws.services.dynamodb.model.GetItemRequest
import com.amazonaws.services.dynamodb.model.Key
import com.amazonaws.services.dynamodb.model.ScanRequest
import com.amazonaws.services.dynamodb.model.QueryRequest
import collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.HashMap

object DynamoDbDecoder {
/**
 * Test program. Lists contents of tables.
 */
  def main(args: Array[String]) {
    val tables = DynamoDbEncoder.dd.listTables().getTableNames()
    S3Decoder.printList(tables)
    val it = tables.iterator()
    while (it.hasNext) {
      S3Decoder.printList(getAllItems(it.next)._2)
    }
  }
  
  def getAllItems(table:String) = {
    println("Getting all items from table " + table)
    val s = new ScanRequest(table)
    val sr = DynamoDbEncoder.dd.scan(s)
    (sr.getLastEvaluatedKey(), sr.getItems().map(getVals))
  }
  
  def getAllItems(table:String, firstKey:Key) = {
    println("Getting all items from table " + table + " starting with " + firstKey)
    val s = new ScanRequest(table)
    s.setExclusiveStartKey(firstKey)
    val sr = DynamoDbEncoder.dd.scan(s)
    (sr.getLastEvaluatedKey(), sr.getItems().map(getVals))
  }

  def getItems(table: String, keyPart: String) = {
    val q = new QueryRequest(table, new AttributeValue(keyPart))
    val sr = DynamoDbEncoder.dd.query(q)
    (sr.getLastEvaluatedKey(), sr.getItems().map(getVals))
  }

  def getItems(table: String, keyPart: String, lastKey: Key) = {
    val q = new QueryRequest(table, new AttributeValue(keyPart)).withExclusiveStartKey(lastKey)
    val sr = DynamoDbEncoder.dd.query(q)
    (sr.getLastEvaluatedKey(), sr.getItems().map(getVals))
  }
  
  def getItem(table:String, keyPart:String) = {
    val key = new Key().withHashKeyElement(new AttributeValue(keyPart))
    val g = new GetItemRequest(table, key)
    getVals(DynamoDbEncoder.dd.getItem(g).getItem())
  }
  
  def getItem(table:String, keyParts:(String, String)) = {
    val key = new Key().withHashKeyElement(new AttributeValue(keyParts._1))
    .withRangeKeyElement(new AttributeValue(keyParts._2))
    val g = new GetItemRequest(table, key)
    getVals(DynamoDbEncoder.dd.getItem(g).getItem())
  }
  
   def getVals(map: java.util.Map[String, AttributeValue]) = {
     var stuff:Map[String, Any] = new HashMap[String, Any]()
    for (k <- map) {
      var num = k._2.getN()
      val nums = k._2.getNS()
      if (num != null) {
        if (num.indexOf('.') >= 0 || num.indexOf(',') >= 0)
          stuff += ((k._1, k._2.getN().toDouble))
        else
          stuff += ((k._1, k._2.getN().toLong))
      } else if (k._2.getS() != null) {
        stuff += ((k._1, k._2.getS()))
      } else if (nums != null) {
        if (nums.length > 0) {
          num = nums.get(0)
          if (num != null) {
            if (num.indexOf('.') >= 0 || num.indexOf(',') >= 0)
              stuff += ((k._1, nums.map(_.toDouble).toSeq))
            else
              stuff += ((k._1, nums.map(_.toLong).toSeq))
          }
        } else
          stuff += ((k._1, nums.map(_.toLong).toSeq))
      } else if (k._2.getSS() != null) {
        stuff += ((k._1, k._2.getSS().toSeq))
      }
    }
    stuff
  }
}
