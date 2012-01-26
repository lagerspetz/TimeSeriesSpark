package spark.timeseries.examples

import com.amazonaws.auth.BasicAWSCredentials
import java.io.File
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.services.dynamodb.model.CreateTableRequest
import com.amazonaws.services.dynamodb.model.DescribeTableRequest
import com.amazonaws.services.dynamodb.model.KeySchemaElement
import com.amazonaws.services.dynamodb.model.ScalarAttributeType
import com.amazonaws.services.dynamodb.model.KeySchema
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput
import com.amazonaws.services.dynamodb.model.PutItemRequest
import com.amazonaws.services.dynamodb.model.AttributeValue
import collection.JavaConversions._
import com.amazonaws.services.dynamodb.model.GetItemRequest
import com.amazonaws.services.dynamodb.model.Key
import com.amazonaws.services.dynamodb.model.DeleteTableRequest
import com.amazonaws.services.dynamodb.model.ScanRequest
import com.amazonaws.services.dynamodb.model.QueryRequest

object DynamoDbDecoder {

  def main(args: Array[String]) {
    val tables = DynamoDbEncoder.dd.listTables().getTableNames()
    S3Decoder.printList(tables)
    val it = tables.iterator()
    while (it.hasNext) {
      S3Decoder.printList(getAllItems(it.next)._2)
    }
  }
  
  def getAllItems(table:String):(Key, Seq[java.util.Map[String, AttributeValue]]) = {
    val s = new ScanRequest(table)
    val sr = DynamoDbEncoder.dd.scan(s)
    (sr.getLastEvaluatedKey(), sr.getItems())
  }
  
  def getAllItems(table:String, firstKey:Key) = {
    val s = new ScanRequest(table)
    s.setExclusiveStartKey(firstKey)
    val sr = DynamoDbEncoder.dd.scan(s)
    (sr.getLastEvaluatedKey(), sr.getItems())
  }

  def getItems(table: String, keyPart: String) = {
    val q = new QueryRequest(table, new AttributeValue(keyPart))
    val sr = DynamoDbEncoder.dd.query(q)
    (sr.getLastEvaluatedKey(), sr.getItems())
  }

  def getItems(table: String, keyPart: String, lastKey: Key) = {
    val q = new QueryRequest(table, new AttributeValue(keyPart)).withExclusiveStartKey(lastKey)
    val sr = DynamoDbEncoder.dd.query(q)
    (sr.getLastEvaluatedKey(), sr.getItems())
  }
  
  
  def getItem(table:String, keyPart:String) = {
    val key = new Key().withHashKeyElement(new AttributeValue(keyPart))
    val g = new GetItemRequest(table, key)
    DynamoDbEncoder.dd.getItem(g).getItem()
  }
  
  def getItem(table:String, keyParts:(String, String)) = {
    val key = new Key().withHashKeyElement(new AttributeValue(keyParts._1))
    .withRangeKeyElement(new AttributeValue(keyParts._2))
    val g = new GetItemRequest(table, key)
    DynamoDbEncoder.dd.getItem(g).getItem()
  }
}