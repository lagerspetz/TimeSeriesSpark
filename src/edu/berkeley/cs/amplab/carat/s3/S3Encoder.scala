package edu.berkeley.cs.amplab.carat.s3

import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import java.io.File
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.io.ObjectInputStream

object S3Encoder {
  val cred = new PropertiesCredentials(S3Encoder.getClass().getResourceAsStream("/AwsCredentials.properties"))
  var s3p:AmazonS3Client = null
  def s3() = { if (s3p == null) s3p = new AmazonS3Client(cred); s3p }
  val defaultBucket = "carat.results"
  var tempFile:File = null
  var fos:FileOutputStream = null
  var out:ObjectOutputStream = null

  def initStream() {
    tempFile = File.createTempFile("temp-appstore-icon-crawler-", ".bin")
    tempFile.deleteOnExit()
    fos = new FileOutputStream(tempFile)
    out = new ObjectOutputStream(fos)
  }

  def write(obj: Any) {
    if (out == null)
      initStream()
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