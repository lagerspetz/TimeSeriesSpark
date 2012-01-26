package spark.timeseries

import spark._
import spark.SparkContext._

/**
 * Create an answer key for alerts for the tbird dataset
 * (and other supercomputer log datasets at [[http://www.cs.sandia.gov/~jrstear/logs/]]).
 * 
 */

object SuperComputerAlerts {

  def alerts(sc: SparkContext, fileName: String) = {
    val file = sc.textFile(fileName)
    val mapped = file.map(_.split(" ", 8)) // max no. fields
    mapped.filter(_(0) != "-")
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: SuperComputerAlerts master filename\n" +
        "Example: SuperComputerAlerts local[2] csv.csv")
      return
    }

    val sc = TimeSeriesSpark.init(args(0), "default")
    val hdfsDir = ""
    val alerts_oracle = alerts(sc, hdfsDir+args(1)).map(_.mkString)
    alerts_oracle.saveAsTextFile(hdfsDir+"alerts-of-" + args(1))

    var mem = Runtime.getRuntime().maxMemory() - Runtime.getRuntime().freeMemory()
    println(mem)
    
    System.exit(0)
  }
}
