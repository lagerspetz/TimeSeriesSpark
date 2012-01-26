package spark.timeseries

import spark._
import spark.SparkContext._

/**
 * A program to detect interesting non-idle portions of an energy measurement file and separate
   them into [[spark.timeseries.Run]]s. Write a file with the [[spark.timeseries.Run]]s.
   Assumes the file exists on a Hadoop DFS at hdfs://server.com:54310/user/username/fileName
   where `fileName` is given as a command line argument. Results are written to a file
   in the same HDFS directory.
 */

object HdfsRuns {

  /**
   * Maps energy measurement runs into their `(start, end, duration, total_energy)` - tuples.
   */
  def ArrayRunMapper(run: Run[Double]) = {
    var energy = 0.0
    var last = 0.0
    var duration = 0.0
    var start = 0.0
    for (k <- run.list) {
      if (last == 0) {
        last = k(1)
        start = last
      } else {
        var step = k(1) - last
        if (step > 0.000201 || step < 0.000199) {
          println("weird step from " + last + " to " + k(1) + ": " + step)
        } else {
          duration += 0.0002
          energy += 0.0002 * k(2) * k(3)
        }
        last = k(1)
      }
    }
    (start, last, duration, energy / 1000.0 / 3600.0)
  }

  /**
   * runs ArrayRunMapper on Runs detected using the specified `idleThresh` from
   * the file `fileName`.
   * 
   * @param fileName The file with energy data.
   * @param idleThresh the threshold for activity. 
   */
  def arrayTest(sc: SparkContext, fileName: String, idleThresh: String) = {
    val file = sc.textFile(fileName)
    val mapped = file.map(TimeSeriesSpark.genericMapper(_, ","))

    /* Using cache() here allows completing the entire
     * run detection using < 10G memory when without it,
     * more than 26G is used and the task does not complete.
     */

    //mapped.cache()
    // Detection of runs:

    var det = new IdleEnergyArrayDetector(idleThresh.toDouble, 2500)

    /* mapped as data, det as detector,
       half a second averages (2500 samples)
     */
    val runs = new RunRDD(mapped, det)
    val reduced = runs.map(ArrayRunMapper)
    reduced
  }

  /**
   * Program main entry point. Takes the Spark master, the file name and the idle threshold as command line arguments.
   * @param args Command line arguments. 
   */
  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: EnergyOps master filename idle_threshold\n" +
        "Example: EnergyOps local[2] csv.csv 200")
      return
    }
    val hdfsDir = ""

    val sc = TimeSeriesSpark.init(args(0), "default")

    val reduced = arrayTest(sc, hdfsDir+args(1), args(2))
    reduced.cache()
    val str = reduced.map(k => { k._1 + ", " + k._2 + ", " + k._3 + " s, " + k._4 + " Wh" })
    str.saveAsTextFile(hdfsDir+"runs-of-"+args(1)+"-"+args(2))

    var mem = Runtime.getRuntime().maxMemory() - Runtime.getRuntime().freeMemory()
    println(mem)

    System.exit(0)
  }
}
