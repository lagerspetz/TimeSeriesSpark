package spark.timeseries

import com.coconut_palm_software.xscalawt.XScalaWT._
import com.coconut_palm_software.xscalawt.XScalaWT.Assignments._
import org.eclipse.swt.layout.GridData
import org.eclipse.swt.SWT
import org.eclipse.swt.layout.GridLayout
import org.eclipse.swt.widgets.Composite
import org.eclipse.swt.widgets.Listener
import org.eclipse.swt.widgets.Event
import org.eclipse.swt.graphics.Rectangle
import scala.actors.Actor._
import com.coconut_palm_software.xscalawt.XScalaWT
import org.jfree.experimental.chart.swt.ChartComposite
import org.eclipse.swt.layout.FillLayout


object SwtTest2 {
  def main(args: Array[String]) {
    var str = "data/d-wlan-71MB-dispoff-5runs-600MHz-4V.csv"
    var temp: ChartComposite = null
    var chart: ChartData = null
    var ms = System.currentTimeMillis()
    var limit = 500000 // 1/24

    val window = shell("SWT Scala JFreeChart: Dessy WLAN 71MB")
    
    window.setSize(800, 600)
    window.setLayout(new FillLayout());
    
    temp =new ChartComposite(window, SWT.NONE, {
    
    chart = new ChartData()
    val stuff = chart.readCsvFile(str, ms, limit, 10000, "Dessy WLAN 71MB avg 2s")
    chart.updateData(stuff)
    chart}, true)

    //window.pack
    actor {
      Thread.sleep(5000)
      val data = chart.readCsvFile(str, ms, limit, 1000, "Dessy WLAN 71MB avg 0.2s")
      XScalaWT.asyncExecInUIThread({
        chart.updateData(data)
      })
    }
    
    runEventLoop(window)
  }
}
