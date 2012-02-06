package spark.timeseries.dynamodb

import collection.JavaConversions._
import com.amazonaws.services.dynamodb.model.AttributeValue
import com.amazonaws.services.dynamodb.model.Key
import scala.collection.mutable.ArrayBuffer
import spark._
import spark.SparkContext._

/** A Spark split class that wraps a DynamoDB table split. */
class DynamoDbSplit(rddId: Int, idx: Int,
    s: Seq[(String, java.util.Map[String, AttributeValue with Serializable])],
    sk:Key, kn: (String, String))
extends Split with Serializable {
  
  val keyName1 = kn._1
  val keyName2 = kn._2
  
  val (startKey1, startKey2) = {
    if (sk == null)
      (null, null)
    else{
      val hash = DynamoDbReader.fromAttributeValue(sk.getHashKeyElement())._2
      val attr = 
      DynamoDbReader.fromAttributeValue(sk.getRangeKeyElement())
      if (attr._1 != null)
        (hash, attr._1.toDouble)
      else
        (hash, attr._2)
    }
  }

  val values = s
  //println("Created split with values:" + values.mkString(" "))

  override def hashCode(): Int = (41 * (41 + rddId) + idx).toInt

  override val index = idx
  
  var iteratorIndex = 0
  
  override def toString() = "DynamoDbSplit " + rddId + ", " + index + ", " + startKey1 + ", "+ startKey2 + ", " + {
    if (values != null)
      values.size+""
      else
        "null"
  }
}

/**
 * An RDD that reads a DynamoDB HashKey table in key order.
 */
class DynamoDbRDD(
  sc: SparkContext,
  tableName: String, keyNames:(String, String),
  minSplits: Int) extends RDD[(String, java.util.Map[String, AttributeValue with Serializable])](sc) {
  
  val LIMIT = 100
  
  @transient val splits_ : Array[Split] = {
    var array = new ArrayBuffer[Split]
    val items = (DynamoDbReader.countSplits(tableName, LIMIT)).toInt
    DynamoDbReader.DynamoDbItemLoop(DynamoDbReader.getAllItems(tableName, LIMIT),
        DynamoDbReader.getAllItems(tableName, _, LIMIT),
        constructSplit(_,_,_,array,keyNames))
    
    //println("Made splits:"+array.mkString("\n"))
    array.toArray[Split]
  }

  def constructSplit(splitIdx: Int, startKey: Key, list: java.util.List[java.util.Map[String, AttributeValue]], splits:ArrayBuffer[Split], keyNames:(String, String)) {
    
    val listM = list.map(x => {
      (x.get(keyNames._1).getS(), DynamoDbReader.getSerializableMap(x))
    })
    splits.append(new DynamoDbSplit(id, splitIdx, listM, startKey, keyNames))
  }

  override def splits = splits_

  override def compute(theSplit: Split) = new Iterator[(String, java.util.Map[String, AttributeValue with Serializable])] {
    val split = theSplit.asInstanceOf[DynamoDbSplit]
    val idx = split.index
    val startKey1 = split.startKey1
    val startKey2 = split.startKey2
    println("Splits.length:"
        +  {
    if (splits != null)
      splits.length+""
      else
        "null"
  })
    //if (split.values == null) {
    val values = {
      var fkey: Key = null
      if (idx == 0) {
        val (key, temp) = DynamoDbReader.getAllItems(tableName, LIMIT)
        fkey = key
        val listM = temp.map(x => {
          (x.get(keyNames._1).getS(), DynamoDbReader.getSerializableMap(x))
        })
        listM
      } else {
        if (startKey1 != null)
          fkey = new Key().withHashKeyElement(DynamoDbReader.toAttributeValue(startKey1))
        if (startKey2 != null)
          fkey = fkey.withRangeKeyElement(DynamoDbReader.toAttributeValue(startKey2))
        val (key, temp) = DynamoDbReader.getAllItems(tableName, fkey, LIMIT)
        val listM = temp.map(x => {
          (x.get(keyNames._1).getS(), DynamoDbReader.getSerializableMap(x))
        })
        listM
      }
    }
    //}
    //println("Split: "+ split)
    var iteratorIndex = 0

    override def hasNext = iteratorIndex < values.size

    override def next = {
      //println("Returning: " + values(iteratorIndex) +" split.values.size=" + values.size)
      val ret = values(iteratorIndex)
      iteratorIndex += 1
      ret
    }
  }
  
  override val dependencies: List[Dependency[_]] = Nil
}