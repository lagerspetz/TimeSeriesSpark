package spark.timeseries

import spark._
import spark.SparkContext._

/**
 * A class that shows some energy data processing facilities, 
 * such as calculation of total energy consumed during an energy measurement experiment,
 * detection of interesting peaks or periods of activity from energy data, and saving
 * statistics of these runs into a file.
 * 
 * Usage:
 * {{{
 * 
 *  val master = "local[1]"
 *  val fileName = "csv.csv"
 *  val idleThresh = 200
 *  
 *  val sc = TimeSeriesSpark.init(master, "default")
    val reduced = detectRuns(sc, fileName, idleThresh).cache()
    
    val str = reduced.map(k => { k._1 + ", " + k._2 + ", " + k._3 + " s, " + k._4 + " Wh" })
    str.saveAsTextFile("temp")
 * }}}
 */

object EnergyOps {

  /**
   * Calculate total energy of an entire measurement file using the deprecated tuple-based
   * MeasurementRunRDD class.
   */
  def totalEnergy(sc: SparkContext, mapped: spark.RDD[(Double, Double, Double)]){
    val start = System.currentTimeMillis()
    var energy = sc.accumulator(0.0)
    val en = mapped.map(x => TimeSeriesSpark.energymapper(x._1,x._2,x._3))
    for (nrj <- en) {
      energy += nrj
    }

    val end = System.currentTimeMillis()

    println("Energy: " + energy.value/5000/3600.0 +" mWh" +
      " time: " + /*(tacc.value/5000)*/11885.0664 +"s " +
      " processing time: " + (end-start) + " ms")
  } 
  
  

  def RunMapper(x: Array[(Double, Double, Double)]) = {
    var energy=0.0
    var last=0.0
    var duration = 0.0
    var start = 0.0
    for (k <- x){
      if (last == 0){
        last=k._1
        start = last
      } else {
        var step = k._1 - last
        if (step > 0.000201 || step < 0.000199){
          println("weird step from " + last + " to " + k._1 +": " + step)
        }
        else {
          duration += 0.0002
          energy+=0.0002*k._2*k._3
        }
        last = k._1
      }
    }
    (start, last, duration, energy / 1000.0 / 3600.0)
  }

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

  def tupleTest(sc: SparkContext, fileName: String, idleThresh: String) = {

    val file = sc.textFile(fileName)
    val mapped = file.map(TimeSeriesSpark.tuple3Mapper)

    /* Using cache() here allows completing the entire
     * run detection using < 10G memory when without it,
     * more than 26G is used and the task does not complete.
     */

    //mapped.cache()
    // Detection of runs:

    var det = new TimeSeriesSpark.IdleEnergyTupleDetector(idleThresh.toDouble, 2500)

    /* mapped as data, det as detector,
       half a second averages (2500 samples)
     */
    val runs = new MeasurementRunRDD(mapped, det)
    //    val runs = new RunRDD(mapped, det)
    val reduced = runs.map(RunMapper)
    reduced
  }

  /**
   * Map energy measurements into [[scala.Array]]s of [[scala.Double]]s, and
   * separate the data into measurement [[spark.timeseries.Run]]s based on the `idleThresh` threshold.
   * An average over half a second is calculated from each half a second period of the data.
   * If such an average is above `idleThresh`, values in the period and after it are considered a measurement run.
   * When the average drops below `idleThresh`, the run is considered finished. Periods of averages of less than `idleThresh` are skipped.
   * @param sc the SparkContext used to execute the task.
   * @param idleThresh The threshold to compare half a second averages against, and detect activity.
   * @param fileName data file path. Assumed to contain a text file with lines of the form
   * {{{
   * num, secs, mA, V
   * }}}
   * where num is an integer measurement number, secs is the time in seconds since measurement was started,
   * mA the current in milliAmperes, and V is the voltage in Volts.
   * Example:
   * {{{
   * 1, 0.000, 350, 3.95
   * 2, 0.002, 342, 3.96
   * ...
   * }}} 
   * 
   */
  def detectRuns(sc: SparkContext, fileName: String, idleThresh: String) = {
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
   * Main program entry point. runs `detectRuns` and saves the result as a file called "temp" in the current directory.
   */
  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: EnergyOps master filename idle_threshold\n" +
        "Example: EnergyOps local[2] csv.csv 200")
      return
    }

    val sc = TimeSeriesSpark.init(args(0), "default")
    //val reduced = tupleTest(sc, args(1), args(2))
    val reduced = detectRuns(sc, args(1), args(2))
    reduced.cache()
    val str = reduced.map(k => { k._1 + ", " + k._2 + ", " + k._3 + " s, " + k._4 + " Wh" })
    str.saveAsTextFile("temp")

    var mem = Runtime.getRuntime().maxMemory() - Runtime.getRuntime().freeMemory()
    println(mem)
    //val str = runs.map(_.mkString)
    //str.saveAsTextFile("runs-of-"+args(1)+"-"+args(2))
    //temp.saveAsTextFile("runs-of-"+args(1))
    //runsPartitions.foreach(x => println(x.map(_.mkString).mkString("[", ",", "]")))
    System.exit(0)
  }
}
