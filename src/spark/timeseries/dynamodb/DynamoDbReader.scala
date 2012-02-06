package spark.timeseries.dynamodb

import com.amazonaws.services.dynamodb.model.AttributeValue
import com.amazonaws.services.dynamodb.model.GetItemRequest
import com.amazonaws.services.dynamodb.model.Key
import com.amazonaws.services.dynamodb.model.ScanRequest
import com.amazonaws.services.dynamodb.model.QueryRequest
import collection.JavaConversions._
import scala.collection.immutable.HashMap
import com.amazonaws.services.dynamodb.model.Condition
import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodb.model.DescribeTableRequest

/**
 * Read DynamoDb tables using /AwsCredentials.properties for the creds.
 */

object DynamoDbReader {
  val cls = DynamoDbReader.getClass()
  val res = cls.getResourceAsStream("/AwsCredentials.properties2")
  val cred = { if (res != null)
    new PropertiesCredentials(res)
  else
    new BasicAWSCredentials("NONE", "NONE") 
  }
  val dd = new AmazonDynamoDBClient(cred)
/**
 * Test program. Lists contents of tables.
 */
  def main(args: Array[String]) {
    val tables = dd.listTables().getTableNames()
    val it = tables.iterator()
    while (it.hasNext) {
      getAllItems(it.next)._2
    }
  }
  
  def countSplits(tableName:String, limit:Long) = {
    describeTable(tableName).getItemCount() / limit
  }
  
  def describeTable(tableName:String) = {
    dd.describeTable(new DescribeTableRequest().withTableName(tableName)).getTable()
  }
  
   /**
   * Generic DynamoDb loop function. Gets items from a table using keys given, and continues until the table scan is complete.
   * This function achieves a block by block read until the end of a table, regardless of throughput or manual limits.
   */
  def DynamoDbItemLoop(tableAndValueToKeyAndResults: => (Key, java.util.List[java.util.Map[String, AttributeValue]]),
    tableAndValueToKeyAndResultsContinue: Key => (Key, java.util.List[java.util.Map[String, AttributeValue]]),
    stepHandler: (Int, Key, java.util.List[java.util.Map[String, AttributeValue]]) => Unit) {
    var index = 0
    var (key, results) = tableAndValueToKeyAndResults
    println("Got: " + results.size + " results.")
    stepHandler(index, null, results)

    while (key != null) {
      index += 1
      println("Continuing from key=" + key)
      val (key2, results2) = tableAndValueToKeyAndResultsContinue(key)
      results = results2
      stepHandler(index, key, results)
      key = key2
      println("Got: " + results.size + " results.")
    }
  }

  def filterItems(table:String, vals: (String, Any)*) = {
    val s = new ScanRequest(table)
    val conds = convertToMap[Condition](vals.map(x => {
      (x._1, new Condition().withComparisonOperator("EQ").withAttributeValueList(new AttributeValue(x._2.toString())))
    }))
    s.setScanFilter(conds)
    val sr = dd.scan(s)
    (sr.getLastEvaluatedKey(), sr.getItems())
  }
  
  def getAllItems(table:String, limit:Int) = {
    println("Getting all items from table " + table)
    val s = new ScanRequest(table)
    s.setLimit(limit) 
    val sr = dd.scan(s)
    (sr.getLastEvaluatedKey(), sr.getItems())
  }
  
  def getAllItems(table:String):(Key, java.util.List[java.util.Map[String, AttributeValue]]) = {
    getAllItems(table,Int.MaxValue)
  }
  
  def getAllItems(table:String, firstKey:Key):(Key, java.util.List[java.util.Map[String, AttributeValue]]) = {
    getAllItems(table, firstKey, Int.MaxValue)
  }
  
  def getAllItems(table:String, firstKey:Key, limit:Int) = {
    println("Getting all items from table " + table + " starting with " + firstKey)
    val s = new ScanRequest(table)
    s.setExclusiveStartKey(firstKey)
    s.setLimit(limit)
    val sr = dd.scan(s)
    (sr.getLastEvaluatedKey(), sr.getItems())
  }

  def getItems(table: String, keyName:String, keyPart: String) = {
    val q = new ScanRequest(table)
    val conds = new java.util.HashMap[String, Condition]
    conds += ((keyName, new Condition().withComparisonOperator("EQ").withAttributeValueList(new AttributeValue(keyPart))))
    //q.setLimit(THROUGHPUT_LIMIT)
    val sr = dd.scan(q)
    (sr.getLastEvaluatedKey(), sr.getItems())
  }
  
  def getItemsIN(table: String, keyName:String, keyPart: String) = {
    val q = new ScanRequest(table)
    val conds = new java.util.HashMap[String, Condition]
    conds += ((keyName, new Condition().withComparisonOperator("IN").withAttributeValueList(new AttributeValue(keyPart))))
    //q.setLimit(THROUGHPUT_LIMIT)
    val sr = dd.scan(q)
    (sr.getLastEvaluatedKey(), sr.getItems())
  }
  
  def getItems(table: String, keyName:String, keyPart: String, lastKey:Key) = {
    val q = new ScanRequest(table)
    val conds = new java.util.HashMap[String, Condition]
    conds += ((keyName, new Condition().withComparisonOperator("EQ").withAttributeValueList(new AttributeValue(keyPart))))
    q.setExclusiveStartKey(lastKey)
    //q.setLimit(THROUGHPUT_LIMIT)
    val sr = dd.scan(q)
    (sr.getLastEvaluatedKey(), sr.getItems())
  }
  
  def getItemsIN(table: String, keyName:String, keyPart: String, lastKey:Key) = {
    val q = new ScanRequest(table)
    val conds = new java.util.HashMap[String, Condition]
    conds += ((keyName, new Condition().withComparisonOperator("IN").withAttributeValueList(new AttributeValue(keyPart))))
    q.setExclusiveStartKey(lastKey)
    //q.setLimit(THROUGHPUT_LIMIT)
    val sr = dd.scan(q)
    (sr.getLastEvaluatedKey(), sr.getItems())
  }

  
  def getItems(table: String, keyPart: String) = {
    val q = new QueryRequest(table, new AttributeValue(keyPart))
    //q.setLimit(THROUGHPUT_LIMIT)
    val sr = dd.query(q)
    (sr.getLastEvaluatedKey(), sr.getItems())
  }

  def getItems(table: String, keyPart: String, lastKey: Key) = {
    val q = new QueryRequest(table, new AttributeValue(keyPart)).withExclusiveStartKey(lastKey)
    //q.setLimit(THROUGHPUT_LIMIT)
    val sr = dd.query(q)
    (sr.getLastEvaluatedKey(), sr.getItems())
  }
  
  def getItem(table:String, keyPart:String) = {
    val key = new Key().withHashKeyElement(new AttributeValue(keyPart))
    val g = new GetItemRequest(table, key)
    dd.getItem(g).getItem()
  }
  
  def getItem(table:String, keyParts:(String, String)) = {
    val key = new Key().withHashKeyElement(new AttributeValue(keyParts._1))
    .withRangeKeyElement(new AttributeValue(keyParts._2))
    val g = new GetItemRequest(table, key)
    getVals(dd.getItem(g).getItem())
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
   
  /**
   * Helper function used by put(table, vals).
   * Constructs Maps to be put into a table from a variable number of (String, Any) - pairs.
   */
  def getMap(vals: (String, Any)*) = {
    val map:java.util.Map[String, AttributeValue] = new java.util.HashMap[String, AttributeValue]()
    for (k <- vals) {
        map.put(k._1, toAttributeValue(k._2))
    }
    map
  }

  def getSerializableMap(oldMap: java.util.Map[String, AttributeValue]) = {
    val map:java.util.Map[String, AttributeValue with Serializable] = new java.util.HashMap[String, AttributeValue with Serializable]()
    for (k <- oldMap) {
      var attr = new AttributeValue with Serializable
      attr.setN(k._2.getN())
      attr.setS(k._2.getS())
      attr.setNS(k._2.getNS())
      attr.setSS(k._2.getSS())
      map += ((k._1, attr))
    }
    map
  }

  /**
   * Convert Any to AttributeValue.
   */
  def toAttributeValue(thing: Any) = {
    if (thing.isInstanceOf[Double]
      || thing.isInstanceOf[Int]
      || thing.isInstanceOf[Long]
      || thing.isInstanceOf[Float]
      || thing.isInstanceOf[Short])
      new AttributeValue().withN(thing + "")
    else if (thing.isInstanceOf[Seq[String]])
      new AttributeValue().withSS(thing.asInstanceOf[Seq[String]])
    else
      new AttributeValue(thing + "")
  }
  
  def fromAttributeValue(thing:AttributeValue) = {
    val n = thing.getN()
    val s = thing.getS()
    val ns = thing.getNS()
    val ss = thing.getSS()
    (n, s, ns, ss)
  }
  
  /**
   * Convert a Seq[String, T] to Map[String, T].
   */
  def convertToMap[T](vals: Seq[(String, T)]) = {
    val map:java.util.Map[String, T] = new java.util.HashMap[String, T]()
     for (k <- vals) {
       map.put(k._1, k._2)
     }
    map
  }
}
