package edu.berkeley.cs.amplab.carat.s3
import com.amazonaws.services.s3.model.GetObjectRequest

object DownloadS3File extends App {
  if (args != null && args.length == 2){
    val res = S3Encoder.s3.getObject(new GetObjectRequest(args(0), args(1)), new java.io.File(args(1)))
    println(res)
  }else {
    println("Usage: DownloadS3File bucket file")}
}