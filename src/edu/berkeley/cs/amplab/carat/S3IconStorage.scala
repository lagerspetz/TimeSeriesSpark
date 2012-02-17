package edu.berkeley.cs.amplab.carat

import spark._
import spark.SparkContext._
import spark.timeseries._
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import java.util.Date
import java.io.FileWriter
import scala.collection.Seq
import scala.collection.mutable.HashSet
import scala.collection.immutable.TreeMap
import scala.collection.immutable.SortedMap
import java.io.ObjectOutputStream
import java.io.File

/**
 * A program for storing icons got from the App Store Icon Crawler script
 * in our S3, in bucket carat.icons.
 */

object S3IconStorage {
  val bucket = "carat.icons"
  val dir = "/mnt/appstore-crawler/icons/"

  /**
   * Main program entry point. Gets icons from the /mnt/appstore-crawler/icons directory.
   */
  def main(args: Array[String]) {
    storeIconsInS3
    sys.exit(0)
  }

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
}
