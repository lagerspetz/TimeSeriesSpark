package edu.berkeley.cs.amplab.carat

import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import java.io.File
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.io.ObjectInputStream

object S3Encoder {
  val cred = new PropertiesCredentials(S3Encoder.getClass().getResourceAsStream("/AwsCredentials.properties"))
  val s3 = new AmazonS3Client(cred)
  val defaultBucket = "carat.results"
  var tempFile = File.createTempFile("temp-appstore-icon-crawler-", ".bin")
  tempFile.deleteOnExit()
  var fos = new FileOutputStream(tempFile)
  var out = new ObjectOutputStream(fos)

  def initStream() {
    tempFile = File.createTempFile("temp-appstore-icon-crawler-", ".bin")
    tempFile.deleteOnExit()
    fos = new FileOutputStream(tempFile)
    out = new ObjectOutputStream(fos)
  }

  def write(obj: Any) {
    out.writeObject(obj)
  }
  
  def put(bucket:String, file: java.io.File) = {
    s3.putObject(bucket, file.getName(), file)
  }
  

  def put(key: String) {
    out.close()
    s3.putObject(defaultBucket, key, tempFile)
    initStream()
  }
  
  def put(bucket:String, key: String) {
    out.close()
    s3.putObject(bucket, key, tempFile)
    initStream()
  }

  def createFileFromObject(obj: Object) = {
    val file = File.createTempFile("temp-appstore-icon-crawler-", ".bin")
    file.deleteOnExit()

    val fos = new FileOutputStream(file)
    val out = new ObjectOutputStream(fos)
    out.writeObject(obj)
    out.close()
    fos.close()
    file
  }
}