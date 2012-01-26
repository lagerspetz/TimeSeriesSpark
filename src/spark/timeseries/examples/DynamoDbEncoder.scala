package spark.timeseries.examples

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

  // For putting data:
  
  val uuId = "uuId"
  
  val resultsTable = "carat.latestresults"
  val resultKey = uuId
    
  val appsTable = "carat.latestapps"  
  val appKey = "appName"
    
  val bugsTable = "carat.latestbugs"
    
  val modelsTable = "carat.latestmodels"
  val modelKey = "model"
  
  val osTable = "carat.latestos"
  val osKey = "os"
    
  // For getting data:
  val registrationTable = "carat.registrations"
  val samplesTable = "carat.samples"
    
  val regsUuid = DynamoDbEncoder.uuId
  val regsModel = "platformId"
  val regsOs = "systemVersion"

  val sampleKey = DynamoDbEncoder.uuId
  val sampleTime = "timestamp"
  val sampleProcesses = "piList"
  val sampleBatteryState = "batteryState"
  val sampleBatteryLevel = "batteryLevel"
  val sampleEvent = "triggeredBy"

  
  val xrange = "xrange"
  val prob = "prob"
  val probNeg = "probNeg"
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
    var del = new DeleteTableRequest(resultsTable)
    dd.deleteTable(del)
    del = new DeleteTableRequest(osTable)
    dd.deleteTable(del)
    del = new DeleteTableRequest(modelsTable)
    dd.deleteTable(del)
    del = new DeleteTableRequest(appsTable)
    dd.deleteTable(del)
    del = new DeleteTableRequest(bugsTable)
    dd.deleteTable(del)
    
    Thread.sleep(5)
    
    createResultsTable()
    createOsTable()
    createModelsTable()
    createAppsTable()
    createBugsTable()
  }
  
  def createResultsTable(){
    val getKey = new KeySchemaElement()
    val ks = getKey.withAttributeName(resultKey)
    ks.setAttributeType("S")
    // will only have current
    val req = new CreateTableRequest(resultsTable, new KeySchema(ks))
    req.setProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(30).withWriteCapacityUnits(10))
    dd.createTable(req)
  }
  
  def createOsTable(){
    val getKey = new KeySchemaElement()
    val ks = getKey.withAttributeName(osKey)
    ks.setAttributeType("S")
    // will only have current
    val req = new CreateTableRequest(osTable, new KeySchema(ks))
    req.setProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(30).withWriteCapacityUnits(10))
    dd.createTable(req)
  }
  
  def createModelsTable(){
    val getKey = new KeySchemaElement()
    val ks = getKey.withAttributeName(modelKey)
    ks.setAttributeType("S")
    // will only have current
    val req = new CreateTableRequest(modelsTable, new KeySchema(ks))
    req.setProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(30).withWriteCapacityUnits(10))
    dd.createTable(req)
  }
  
  def createBugsTable(){
    var getKey = new KeySchemaElement()
    val ks = getKey.withAttributeName(resultKey)
    ks.setAttributeType("S")
    getKey = new KeySchemaElement()
    val rk = getKey.withAttributeName(appKey)
    rk.setAttributeType("S")
    // will only have current
    val req = new CreateTableRequest(bugsTable, new KeySchema(ks).withRangeKeyElement(rk))
    req.setProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(30).withWriteCapacityUnits(10))
    dd.createTable(req)
  }
  
  def createAppsTable(){
    val getKey = new KeySchemaElement()
    val ks = getKey.withAttributeName(appKey)
    ks.setAttributeType("S")
    // will only have current
    val req = new CreateTableRequest(appsTable, new KeySchema(ks))
    req.setProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(30).withWriteCapacityUnits(10))
    dd.createTable(req)
  }
}