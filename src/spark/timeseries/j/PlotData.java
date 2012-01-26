package spark.timeseries.j;

import java.awt.Color;
import java.awt.Dimension;
import java.util.Date;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import javax.swing.JPanel;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.DateTickMarkPosition;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.labels.StandardXYToolTipGenerator;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimePeriodAnchor;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.jfree.ui.ApplicationFrame;

public class PlotData extends ApplicationFrame
{
  private XYPlot plot = null;
  private ChartPanel panel = null;
  private int dc = 0;
  
  public PlotData(String paramString)
  {
    super(paramString);
    panel = (ChartPanel)createDemoPanel();
    panel.setPreferredSize(new Dimension(500, 270));
    panel.setMouseZoomable(true, true);
    setContentPane(panel);
  }
  
  public void updateData(XYDataset data){
    if (data == null) // error
      return;
    System.out.println("updateData dataset size=" + data.getItemCount(0));

    // lines, no shapes
    XYLineAndShapeRenderer localXYLineAndShapeRenderer1 = new XYLineAndShapeRenderer(true, false);
    localXYLineAndShapeRenderer1.setUseFillPaint(true);
    localXYLineAndShapeRenderer1.setBaseFillPaint(Color.white);
    localXYLineAndShapeRenderer1.setBaseToolTipGenerator(new StandardXYToolTipGenerator("{1}: {2}", new SimpleDateFormat("mm:ss.SSS"), new DecimalFormat("0.00")));
    
    DateAxis localDateAxis1 = new DateAxis("Time");
    localDateAxis1.setRange(((TimeSeriesCollection)data).getDomainBounds(true));
    localDateAxis1.setDateFormatOverride(new SimpleDateFormat("mm:ss.SSS"));
    localDateAxis1.setTickMarkPosition(DateTickMarkPosition.MIDDLE);
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
    dc++;
  }
  
  public long createTestData(){
    long ms = System.currentTimeMillis();
    long limit = 6971127; // half of the file
    limit = 3000000; // quarter
    limit = 1500000; // 1/8
    XYDataset data = readCsvFile(ms, limit, 5000, "Dessy WLAN 71MB avg 1s");
    updateData(data);
    return ms;
  }

  private JFreeChart createChart()
  {
    plot = new XYPlot();
    JFreeChart localJFreeChart = new JFreeChart("Power usage", plot);
    ChartUtilities.applyCurrentTheme(localJFreeChart);
    return localJFreeChart;
  }

  private static XYDataset readCsvFile(long ms, long limit, long averageLength, String title)
  {
    TimeSeries localTimeSeries = new TimeSeries(title);
    try{
    java.util.Scanner s = new java.util.Scanner(new java.io.File("data/d-wlan-71MB-dispoff-5runs-600MHz-4V.csv"));
    s.useDelimiter("[,\n]");
    int kind = 0;
    int count = 0;
    //long limit = Long.MAX_VALUE;
    boolean firstLine = true;
  
    //double time = 0;
    double val = 0.0;
    
    /* JFreeChart does not support 1/5 ms granularity. Use average here: */
    int avgCount = 0;
    double sum = 0.0;
        
    while((firstLine && s.hasNext()) || (s.hasNextDouble() && count < limit)){
      if (firstLine){
        s.next();
      } else{
        //double temp = s.nextDouble();
        //System.out.println("next=" + temp);
        switch (kind){
          case 0:
            s.nextDouble();
            break;
          case 1:
            // Time
            //time = s.nextDouble();
            s.nextDouble();
            break;
          case 2:
            val = s.nextDouble();
            break;
          case 3:
            val *= s.nextDouble();
            break;
          default:
            break;
        }
      }
      kind++;
      if (kind == 4){
        if (firstLine){
          firstLine = false;
        }else{
          count++;
          avgCount++;
          sum += val;
          if (avgCount == averageLength){
            localTimeSeries.add(new Millisecond(new Date(ms+count/5)), sum/averageLength);
            avgCount = 0;
            sum = 0;
          }
        }
        kind = 0;
        //time = 0;
        val = 0.0;
      }
    }
    s.close();
    } catch (java.io.IOException e){
      e.printStackTrace();
    }
    
    TimeSeriesCollection localTimeSeriesCollection = new TimeSeriesCollection();
    localTimeSeriesCollection.addSeries(localTimeSeries);
    localTimeSeriesCollection.setXPosition(TimePeriodAnchor.MIDDLE);
    return localTimeSeriesCollection;
  }

  public JPanel createDemoPanel()
  {
    JFreeChart localJFreeChart = createChart();
    ChartPanel localChartPanel = new ChartPanel(localJFreeChart);
    localChartPanel.setMouseWheelEnabled(true);
    return localChartPanel;
  }

  public static void main(String[] paramArrayOfString)
  {
    final PlotData instance = new PlotData("JFreeChart: Dessy WLAN 71MB");
    instance.pack();
    //RefineryUtilities.centerFrameOnScreen(instance);
    instance.setVisible(true);
    new Thread(){
      public void run(){
      try{
        sleep(2000);
        } catch (InterruptedException e){
        // no-op
        }
        //long ms = instance.createTestData();
        long ms = System.currentTimeMillis();
              try{
        sleep(5000);
        } catch (InterruptedException e){
        // no-op
        }
            long limit = 500000; // 1/24
        XYDataset data = instance.readCsvFile(ms, limit, 10000, "Dessy WLAN 71MB avg 2s");
        instance.updateData(data);
      }
    }.start();
  }
  
  /*@Override
  public void windowClosing(java.awt.event.WindowEvent event){
    // no-op
  }*/
}


