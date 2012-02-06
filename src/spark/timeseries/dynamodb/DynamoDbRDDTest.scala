package spark.timeseries.dynamodb

import spark._
import spark.SparkContext._
import collection.JavaConversions._

object DynamoDbRDDTest extends App {

  val t = "carat.samples"

  val sc = new SparkContext("local", "DynamoDbRDDTest")
  val testRDD = new DynamoDbRDD(sc, t, ("uuId", "timestamp"), 2)
  val cc = testRDD.collect()
  for (x <- cc)
    println("Key: " + x._1 + " value: " + x._2.mkString(" "))
  
}