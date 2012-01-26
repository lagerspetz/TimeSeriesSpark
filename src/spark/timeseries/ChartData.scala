package spark.timeseries

import org.jfree.chart.plot.XYPlot
import org.jfree.data.xy.XYDataset
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer
import org.jfree.chart.labels.StandardXYToolTipGenerator
import org.jfree.chart.axis.DateAxis
import org.jfree.data.time.TimeSeriesCollection
import java.text.SimpleDateFormat
import org.jfree.chart.axis.DateTickMarkPosition
import java.text.DecimalFormat
import org.jfree.chart.axis.NumberAxis
import org.jfree.chart.ChartUtilities
import org.jfree.chart.JFreeChart
import org.jfree.data.time.TimeSeries
import org.jfree.data.time.Millisecond
import java.io.IOException
import java.util.Date
import org.jfree.data.time.TimePeriodAnchor

class ChartData(val plot: XYPlot = new XYPlot()) extends JFreeChart(plot){
  var dc = 0
  
  def updateData(data: XYDataset){
    if (data == null) // error
      return
    println("updateData dataset size=" + data.getItemCount(0))
    // lines, no shapes
    
    val localXYLineAndShapeRenderer1 = new XYLineAndShapeRenderer(true, false)
    localXYLineAndShapeRenderer1.setUseFillPaint(true)
    localXYLineAndShapeRenderer1.setBaseFillPaint(java.awt.Color.white)
    localXYLineAndShapeRenderer1.setBaseToolTipGenerator(new StandardXYToolTipGenerator("{1}: {2}", new SimpleDateFormat("mm:ss.SSS"), new DecimalFormat("0.00")))
    
    val localDateAxis1 = new DateAxis("Time");
    localDateAxis1.setRange(data.asInstanceOf[TimeSeriesCollection].getDomainBounds(true))
    localDateAxis1.setDateFormatOverride(new SimpleDateFormat("mm:ss.SSS"))
    localDateAxis1.setTickMarkPosition(DateTickMarkPosition.MIDDLE)
    if (dc > 0){
      localDateAxis1.setVisible(false);
    }else{
      plot.setRangeAxis(dc, new NumberAxis("mW"));
    }
    plot.setDomainAxis(dc, localDateAxis1);
    plot.setDataset(dc, data);
    plot.mapDatasetToDomainAxis(dc, dc);
//    localXYLineAndShapeRenderer1.setSeriesPaint(dc, Color.red);
    plot.setRenderer(dc, localXYLineAndShapeRenderer1);
    if (dc == 0){
        plot.setDomainCrosshairVisible(true);
        plot.setRangeCrosshairVisible(true);
    }
    dc+=1
  }
  
  def createTestData() = {
    var ms = System.currentTimeMillis();
    var limit = 6971127; // half of the file
    limit = 3000000; // quarter
    limit = 1500000; // 1/8
    var data = readCsvFile("data/d-wlan-71MB-dispoff-5runs-600MHz-4V.csv", ms, limit, 5000, "Dessy WLAN 71MB avg 1s");
    updateData(data)
    ms
  }
  
  def readCsvFile(filePath: String, ms: Long, limit: Long, averageLength: Long, title: String) = {
    val localTimeSeriesCollection = new TimeSeriesCollection()
    var localTimeSeries: TimeSeries = null
    try{
      localTimeSeries = parseLines(filePath, ms, limit, averageLength, title)
    } catch {
      case e: IOException => e.printStackTrace();
      case e => throw e
    }
    localTimeSeriesCollection.addSeries(localTimeSeries);
    localTimeSeriesCollection.setXPosition(TimePeriodAnchor.MIDDLE);
    localTimeSeriesCollection
  }

  def parseLines(filePath: String, ms: Long, limit: Long, averageLength: Long, title: String): TimeSeries = {
    var firstLine = true;
    var count = 0;
    var value = 0.0;
    /* JFreeChart does not support 1/5 ms granularity. Use average here: */
    var avgCount = 0;
    var sum = 0.0;
    var localTimeSeries = new TimeSeries(title);
    var file = io.Source.fromFile(filePath)
    for (line <- file.getLines()) {
      if (firstLine) {
        firstLine = false
      } else {
        val array = line split "[,\n]"
        if (count < limit) {
          if (firstLine) {
            firstLine = false
          } else {
            value = array(2).toDouble
            value *= array(3).toDouble

            count += 1
            avgCount += 1
            sum += value
            if (avgCount == averageLength) {
              //println("sum=" + sum)
              localTimeSeries.add(new Millisecond(new Date(ms + count / 5)), sum / averageLength)
              avgCount = 0
              sum = 0
            }
          }
          value = 0.0
        } else
          return localTimeSeries
      }
    }
    return localTimeSeries
  }
  
  def readCsvFileOld(ms: Long, limit: Long, averageLength: Long, title: String) = {
    println("readCsvFile")
    
    val localTimeSeriesCollection = new TimeSeriesCollection()
    var localTimeSeries = new TimeSeries(title);
    try{
      var s = new java.util.Scanner(new java.io.File("datad-wlan-71MB-dispoff-5runs-600MHz-4V.csv"))
      s.useDelimiter("[,\n]")
    var kind = 0;
    var count = 0;
    var firstLine = true;
  
    var value = 0.0;
    
    /* JFreeChart does not support 1/5 ms granularity. Use average here: */
    var avgCount = 0;
    var sum = 0.0;
        
    while((firstLine && s.hasNext()) || (s.hasNextDouble() && count < limit)){
      if (firstLine){
        s.next();
      } else{
        //double temp = s.nextDouble();
        //System.out.println("next=" + temp);
        kind match{
          case 0 => s.nextDouble()
          case 1 => s.nextDouble()
          case 2 => value = s.nextDouble()
          case 3 => value *= s.nextDouble()
          case _ => throw new Error("kind was " + kind);
        }
      }
      kind+=1
      if (kind == 4){
        if (firstLine){
          firstLine = false;
        }else{
          count+=1
          avgCount+=1
          sum += value
          if (avgCount == averageLength){
            //println("sum=" + sum)
            localTimeSeries.add(new Millisecond(new Date(ms+count/5)), sum/averageLength);
            avgCount = 0;
            sum = 0;
          }
        }
        kind = 0;
        //time = 0;
        value = 0.0
      }
    }
    s.close();
    } catch {
      case e: IOException => e.printStackTrace();
      case e => throw e
    }
    localTimeSeriesCollection.addSeries(localTimeSeries);
    localTimeSeriesCollection.setXPosition(TimePeriodAnchor.MIDDLE);
    localTimeSeriesCollection
  }
}
