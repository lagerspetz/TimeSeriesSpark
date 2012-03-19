package edu.berkeley.cs.amplab.carat.tools

import java.io.File
import edu.berkeley.cs.amplab.carat.s3.S3Decoder
import edu.berkeley.cs.amplab.carat.s3.S3Encoder

/**
 * A program for storing icons got from the App Store Icon Crawler script
 * in our S3, in bucket carat.icons. Gets icons from the /mnt/appstore-crawler/icons directory.
 */

object S3IconStorage extends App{
  /**
   * Get the jpg files stored in `dir` and put them into the S3 bucket `bucket`.
   */
  def storeIconsInS3() {
    val files = new File(dir).listFiles(new java.io.FileFilter(){
      def accept(pathname:File) = pathname.getName().endsWith(".jpg")
    })
    for (app <- files) {
      if (!S3Decoder.has(bucket, app.getName())){
        println("Storing icon for "+app.getName)
        S3Encoder.put(bucket, app)
      }
    }
  }
  
  /* Program starts below: */
  
  val bucket = "carat.icons"
  val dir = "/mnt/appstore-crawler/icons/"

  storeIconsInS3
}
