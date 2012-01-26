import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
import java.io.File
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.io.ObjectInputStream

object S3Test {
  val cred = new BasicAWSCredentials("AKIAI4GAKIECUB7URTJA", "0uhH2/GaRXyTW8PdE+3SKwIxDyHePm9SxtHRqUqE")
  val s3 = new AmazonS3Client(cred)

  def main(args: Array[String]) {
    val bn = "carat.results"
    val buckets = s3.listBuckets()
    printList(buckets)

    val it = buckets.iterator()
    while (it.hasNext) {
      val bucket = it.next
      val name = bucket.getName()
      val objs = s3.listObjects(name)
      val list = objs.getObjectSummaries()
      if (name == bn){
        val key = list.get(0).getKey()
        val obj = s3.getObject(name, key)
        val stuff = new ObjectInputStream(obj.getObjectContent()).readObject()
        printf("bucket=%s name key=%s content=%s\n", name, key, stuff)
      }
      println(name)
      println(objs.getPrefix())
      println(objs.getDelimiter())
      printList(objs.getCommonPrefixes())
      printList(list)

    }

    s3.putObject(bn, "current-85.txt", createFileFromMessage("Blarg"))
  }

  def printList(list: java.util.List[_ <: Any]) {
    val it = list.iterator()
    while (it.hasNext) {
      var item = it.next
      println(item)
    }
  }

  def createFileFromMessage(message: Object) = {
    val file = File.createTempFile("aws-java-sdk-", ".bin")
    file.deleteOnExit()

    val fos = new FileOutputStream(file)
    val out = new ObjectOutputStream(fos)
    out.writeObject(message)
    out.close()
    fos.close()
    file
  }
}