package spark.timeseries

import spark._
import spark.SparkContext._
import scala.collection.mutable.Queue
import org.jfree.data.time.Millisecond
import org.jfree.data.time.TimePeriodAnchor
import org.jfree.data.time.TimeSeries
import org.jfree.data.time.TimeSeriesCollection
import com.esotericsoftware.kryo._
import spark.KryoRegistrator
import java.util.Date
import org.eclipse.swt.SWT
import com.coconut_palm_software.xscalawt.XScalaWT._
import com.coconut_palm_software.xscalawt.XScalaWT.Assignments._
import org.eclipse.swt.layout.GridData
import org.eclipse.swt.layout.GridLayout
import org.eclipse.swt.widgets.Text
import org.eclipse.swt.events.ModifyListener
import org.eclipse.swt.events.ModifyEvent
import scala.actors.Actor._
import org.eclipse.swt.layout.FillLayout
import org.jfree.experimental.chart.swt.ChartComposite

/**
Class for testing energy data operations on MeasurementRunRDD.

@author Eemil Lagerspetz

related work: OpenTSDB: Open Time Series Database
-like ganglia, loses old data
-See what it does and what it lacks

Google for:
-Distributed Time Series Analysis
-Parallel Time Series Analysis

Data mining?
-Days instead of minutes
-From disk
-Does not scale

I want to make a General Time Series Analysis Tool
-Adapter to read OpenTSDB stuff into RDDs?
-Try to import my data to OpenTSDB and see what it loses

Mike Franklin the professor has a company that deals with stream processing

*/

object XScalaWTEnergyPlot {
  
  def plotMapper(x: Array[(Double, Double, Double)], averageLength: Int): TimeSeriesCollection = {
      var ms = System.currentTimeMillis()
      var energy=0.0
      var last=0.0
      var duration = 0.0
      var count = 0
      var start = 0.0
      
      var sum = 0.0
      var avgCount = 0
      var localTimeSeries = new TimeSeries("run " + new Date(ms))
      
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
            count += 1
            avgCount += 1
            sum += k._2*k._3
            if (avgCount == averageLength){
              localTimeSeries.add(new Millisecond(new Date(ms+count/5)), sum/averageLength)
              //println((ms+count/5) + ", " + (sum/averageLength))
              avgCount = 0
              sum = 0
            }

            duration += 0.0002
            energy+=0.0002*k._2*k._3
          }
          last = k._1
        }
      }
      
      var localTimeSeriesCollection = new TimeSeriesCollection()
      localTimeSeriesCollection.addSeries(localTimeSeries)
      localTimeSeriesCollection.setXPosition(TimePeriodAnchor.MIDDLE)
      return localTimeSeriesCollection
    }

  def main(args: Array[String]){
    if (args.length < 3){
      println("Usage: EnergyPlot master filename idle_threshold [kryo]\n"+
              "Example: EnergyPlot local[2] csv.csv 200 0")
      return
    }
    val cache = "default"
    println("init")
    val sc = TimeSeriesSpark.init(args(0), "default")
    val file = sc.textFile(args(1))
    val mapped = file.map(TimeSeriesSpark.tuple3Mapper)
    
    /* Using cache() here allows completing the entire
     * run detection using < 10G memory when without it,
     * more than 26G is used and the task does not complete.
     */
     
     
    //mapped.cache()    
    //println("mapped cached")
    // Plotting of runs:

    val title = "TimeSeriesSpark: "+args(1)
//    p.createTestData()

    var det = new TimeSeriesSpark.IdleEnergyTupleDetector(args(2).toDouble, 2500)

    /* mapped as data, det as detector,
       half a second averages (2500 samples)
     */
    val runs = new MeasurementRunRDD(mapped, det)
    runs.cache()
    println("MRRDD created")

    //p.createTestData()    
    /* Generate a plot live */
    //println("Running makePlot")

    //makePlot(runs, p, "TimeSeriesSpark: "+args(1),500)
    
    println("Start of non-lazy portion")
    var ms = System.currentTimeMillis()
    /* 500 = 10 points per second
     * (max 5 = 1000 samples, because of
     * the precision limit of 1 ms in JFreeChart)
     */
    
    // Sequence of TimeSeriesCollection
    val temp = runs.map(x => plotMapper(x, 500)).collect()

    var chart: ChartData = null
    val window = shell(title)
    window.setSize(800, 600)
    window.setLayout(new FillLayout());

    new ChartComposite(window, SWT.NONE, {
      chart = new ChartData()
      chart
    }, true)

    for (k <- temp) {
      chart.updateData(k)
    }

    //val reduced = runs.map(RunMapper)
    runEventLoop(window)
  }
}
