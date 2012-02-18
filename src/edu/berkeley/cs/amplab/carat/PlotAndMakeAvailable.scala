package edu.berkeley.cs.amplab.carat
import java.io.File

object PlotAndMakeAvailable extends App {
  val plotwww = "/mnt/www/plots"
    
  // rm -rf /mnt/www/plots/*.eps
  val f = new File(plotwww)
  val flist = f.listFiles(new java.io.FilenameFilter(){
    def accept(dir:File, name:String) = name.endsWith(".eps") 
  })
  for (k <- flist){
    k.delete()
  }

  val plotDir = CaratDynamoDataToPlots.plotEverything("local[2]", true, plotwww)
  // /mnt/www/treethumbnailer.sh /mnt/www/plots

  val temp = Runtime.getRuntime().exec(Array("/bin/bash", "/mnt/www/treethumbnailer.sh", "/mnt/www/plots"))
  val err_read = new java.io.BufferedReader(new java.io.InputStreamReader(temp.getErrorStream()))
  val out_read = new java.io.BufferedReader(new java.io.InputStreamReader(temp.getInputStream()))
  var line = err_read.readLine()
  var line2 = out_read.readLine()
  while (line != null || line2 != null) {
    if (line != null){
      println(line)
      line = err_read.readLine()
    }
    
    if (line2 != null){
      println(line2)
      line2 = out_read.readLine()
    }
  }
  temp.waitFor()
  sys.exit(0)
}