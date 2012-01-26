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

object DynamoDbEncoder {
  val dd = new AmazonDynamoDBClient(S3Encoder.cred)

  
  val xrange = "xrange"
  val prob = "prob"
  val probNeg = "probNeg"
  //The Cumulative distributions are not required. 
  //val probc = "probc"
  //val probcNeg = "probcNeg"
  val xmax = "xmax"
  val distanceField = "distance"
    
  
  
  
  def put(table: String, keyName: String, keyValue:String,
      //xrange1: Seq[Double],
      maxX: Double,
      prob1: Seq[(Int, Double)],prob2: Seq[(Int, Double)],
      /*probc1: Seq[(Double, Double)],probc2: Seq[(Double, Double)],*/
      distance:Double) {
    val map = new java.util.HashMap[String, AttributeValue]()
    map.put(keyName, new AttributeValue(keyValue))
    map.put(xmax, new AttributeValue().withN(maxX + ""))
    map.put(prob, new AttributeValue().withSS(prob1.map(x => {x._1 + ";" + x._2})))
    map.put(probNeg, new AttributeValue().withSS(prob2.map(x => {x._1 + ";" + x._2})))
    map.put(distanceField, new AttributeValue().withN(distance.toString()))
    println("Going to put: " + map.mkString("\n"))
    val putReq = new PutItemRequest(table, map)
    dd.putItem(putReq)
  }
  
  def putBug(table: String, keyNames: (String, String), keyValues:(String, String),
      maxX: Double, prob1: Seq[(Int, Double)],prob2: Seq[(Int, Double)],
      distance:Double) {
    val map = new java.util.HashMap[String, AttributeValue]()
    map.put(keyNames._1, new AttributeValue(keyValues._1))
    map.put(keyNames._2, new AttributeValue(keyValues._2))
    map.put(xmax, new AttributeValue().withN(maxX + ""))
    map.put(prob, new AttributeValue().withSS(prob1.map(x => {x._1 + ";" + x._2})))
    map.put(probNeg, new AttributeValue().withSS(prob2.map(x => {x._1 + ";" + x._2})))
    map.put(distanceField, new AttributeValue().withN(distance.toString()))
    println("Going to put: " + map.mkString("\n"))
    val putReq = new PutItemRequest(table, map)
    dd.putItem(putReq)
  }

  def main(args: Array[String]) {
    val tables = dd.listTables().getTableNames()
    S3Decoder.printList(tables)
    val it = tables.iterator()
    while (it.hasNext) {
      val t = it.next
      val getReq = new DescribeTableRequest()
      val desc = dd.describeTable(getReq.withTableName(t))
      println(desc.toString())
      //val item = dd.getItem(new GetItemRequest("carat.latestbugs", new Key(new AttributeValue("85")))).getItem()
      //println("Item: " + item.mkString("\n"))
    }
  }
  
  def clearTables(){
    /*var del = new DeleteTableRequest("carat.latestbugs")
    dd.deleteTable(del)
    del = new DeleteTableRequest("carat.latestapps")
    dd.deleteTable(del)
    del = new DeleteTableRequest("carat.latestresults")
    dd.deleteTable(del)*/
    //createResultsTable()
    //createBugsTable()
    //createAppsTable()
  }
  
  def createResultsTable(){
    val getKey = new KeySchemaElement()
    val ks = getKey.withAttributeName("uuid")
    ks.setAttributeType("S")
    // will only have current
    val req = new CreateTableRequest("carat.latestresults", new KeySchema(ks))
    req.setProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(30).withWriteCapacityUnits(10))
    dd.createTable(req)
  }
  
  def createOSTable(){
    val getKey = new KeySchemaElement()
    val ks = getKey.withAttributeName("os")
    ks.setAttributeType("S")
    // will only have current
    val req = new CreateTableRequest("carat.latestos", new KeySchema(ks))
    req.setProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(30).withWriteCapacityUnits(10))
    dd.createTable(req)
  }
  
  def createModelTable(){
    val getKey = new KeySchemaElement()
    val ks = getKey.withAttributeName("model")
    ks.setAttributeType("S")
    // will only have current
    val req = new CreateTableRequest("carat.latestmodels", new KeySchema(ks))
    req.setProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(30).withWriteCapacityUnits(10))
    dd.createTable(req)
  }
  
  def createBugsTable(){
    //val del = new DeleteTableRequest("carat.latestbugs")
    //dd.deleteTable(del)
    var getKey = new KeySchemaElement()
    val ks = getKey.withAttributeName("uuid")
    ks.setAttributeType("S")
    getKey = new KeySchemaElement()
    val rk = getKey.withAttributeName("appname")
    rk.setAttributeType("S")
    // will only have current
    val req = new CreateTableRequest("carat.latestbugs", new KeySchema(ks).withRangeKeyElement(rk))
    req.setProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(30).withWriteCapacityUnits(10))
    dd.createTable(req)
  }
  
  def createAppsTable(){
    val getKey = new KeySchemaElement()
    val ks = getKey.withAttributeName("appname")
    ks.setAttributeType("S")
    // will only have current
    val req = new CreateTableRequest("carat.latestapps", new KeySchema(ks))
    req.setProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(30).withWriteCapacityUnits(10))
    dd.createTable(req)
  }
}