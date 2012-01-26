package spark.timeseries.examples

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
import java.io.File
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.io.ObjectInputStream

object S3Decoder {

  var obj: com.amazonaws.services.s3.model.S3Object = null
  var in: ObjectInputStream = null
  var open = false

  def get(key: String) {
    obj = S3Encoder.s3.getObject(S3Encoder.bucket, key)
    in = new ObjectInputStream(obj.getObjectContent())
    open = true
  }

  def read() = {
    if (open) {
      try {
        in.readObject()
      } catch {
        case _ => {
          open = false
          in.close()
          null
        }
      }
    } else
      null
  }
  
  def main(args:Array[String]) {
    val objs = S3Encoder.s3.listObjects(S3Encoder.bucket)
    val list = objs.getObjectSummaries()
    printList(list)
    
    val it = list.iterator()
     while (it.hasNext) {
      var item = it.next
      get(item.getKey())
      while(in != null){
        var k = read()
        if (k != null)
          println(k)
      }
      in.close()
    }
  }

  def printList(list: java.util.List[_ <: Any]) {
    val it = list.iterator()
    while (it.hasNext) {
      var item = it.next
      println(item)
    }
  }
}