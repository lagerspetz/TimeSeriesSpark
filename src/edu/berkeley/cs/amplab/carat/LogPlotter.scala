package edu.berkeley.cs.amplab.carat

import scala.io.Source
import scala.collection.immutable.TreeMap
import scala.sys.process._


object LogPlotter extends App {

  val kw = "Considering"
  val dir = "data/charts/final/"
    
  var tp = ""
    
  var data = false
  
  var prob = new TreeMap[Double, Double]
  var probNeg = new TreeMap[Double, Double]
  var target = prob
  
  var fname = ""
  var nameparts=""
    
  var neg = false
  
  val lines = Source.fromFile("data/charts/carat-dynamo-data-analysis-log.txt").getLines()
  for (k <- lines){
    //println("Line: " + k)
    if (k.startsWith(kw)) {
      val tempArray = k.split(" ")
      fname = tempArray(1)
      tp = fname
      nameparts = ""
      for (k <- 2 until tempArray.length)
        nameparts += "-" + tempArray(k)
      fname += nameparts
      println("Processing "+tp + " " + nameparts.substring(1))
    }else if (k == "prob"){
      data = true
      prob = new TreeMap[Double, Double]
      probNeg = new TreeMap[Double, Double]
    }else if (data){
      val arr = k.split(" ")
      if (arr.length > 2){
        // turn data off at the end of this section
        data = false
        neg = false
        // and plot the distributions!
/*        if (nameparts.contains("Angry") ||
            nameparts.contains("BTServer") ||
            nameparts.contains("Skype") ||
            (nameparts.contains("2DEC") && tp == "jscore"))*/
        plotDistributions(prob, probNeg, fname, "With "+ nameparts.substring(1), "Without " + nameparts.substring(1), tp == "hog" || tp == "bug")
      }else if (arr.length == 1 && k == "probNeg"){
        neg = true
      }else if (arr.length == 2){
        if (neg){
          probNeg += ((arr(0).toDouble, arr(1).toDouble))
        }else{
          prob += ((arr(0).toDouble, arr(1).toDouble))
        }
      }
      // turn data off at the end of this
    }
  }

  def plotDistributions(prob: TreeMap[Double, Double], probNeg: TreeMap[Double, Double], fname: String, t1:String, t2:String, distanceCheck: Boolean = false) {
    // hour
    val mul = 3600
    
    var sum = 0.0
    var cumulative = new TreeMap[Double, Double]
    for (k <- prob){
      sum += k._2
      cumulative += ((k._1, sum))
    }
    
    sum = 0.0
    var cumulativeNeg = new TreeMap[Double, Double]
    for (k <- probNeg){
      sum += k._2
      cumulativeNeg += ((k._1, sum))
    }
    
    val xmax = math.max(prob.lastKey, probNeg.lastKey)
    
    cumulative+=((xmax, 1.0))
    cumulativeNeg+=((xmax, 1.0))
    
    val distance = CaratDynamoDataAnalysis.getDistanceNonCumulative(prob, probNeg)
    
    if (!distanceCheck || distance > 0){
    val fname1 = fname + ".txt"
    val fname2 = fname + "_neg.txt"
    
    val fname3 = fname + "c.txt"
    val fname4 = fname + "c_neg.txt"
    
    val plotfile = new java.io.FileWriter(dir + "plotfile.txt")
    val plotfilec = new java.io.FileWriter(dir + "plotfilec.txt")
    plotfile.write("set term postscript eps enhanced color 'Arial' 24\nset xtics out\n" +
      "set size 1.93,1.1\n"+
      "set logscale x\n"+
      "set xlabel \"Battery drain % / s\"\n" +
      "set ylabel \"Probability\"\n")
    plotfile.write("set output \"" + dir + fname + ".eps\"\n")
    plotfilec.write("set term postscript eps enhanced color 'Arial' 24\nset xtics out\n" +
      "set size 1.93,1.1\n"+
      "set logscale x\n"+
      "set yrange [0:1.05]\n"+
      "set key bottom Right\n"+
      "set xlabel \"Battery drain % / s\"\n" +
      "set ylabel \"Probability of drain <= x\"\n")
    plotfilec.write("set output \"" + dir + fname + "c.eps\"\n")
    
    plotfile.write("plot \"" + dir + fname1 + "\" using 1:2 with linespoints lt rgb \"#f3b14d\" lw 2 title \""+t1+"\", "+
        "\"" + dir + fname2 + "\" using 1:2 with linespoints lt rgb \"#007777\" lw 2 title \""+t2+"\"\n")
    plotfile.close
    plotfilec.write("plot \"" + dir + fname3 + "\" using 1:2 with linespoints lt rgb \"#f3b14d\" lw 2 title \""+t1+"\", "+
        "\"" + dir + fname4 + "\" using 1:2 with linespoints lt rgb \"#007777\" lw 2 title \""+t2+"\"\n")
    plotfilec.close
    val out1 = new java.io.FileWriter(dir + fname1)
    val out2 = new java.io.FileWriter(dir + fname2)

    val probDist = prob.map(x => {
      x._1 + " " + x._2
    })
    val probNegDist = probNeg.map(x => {
      x._1 + " " + x._2
    })
    
    var cumulativeDist = cumulative.map(x => {
      x._1 + " " + x._2
    })
    
     val cumulativeNegDist = cumulativeNeg.map(x => {
      x._1 + " " + x._2
    })
    
    //println(probDist.mkString("\n"))
    out1.write(probDist.mkString("\n") + "\n")
    //println(probNegDist.mkString("\n"))
    out2.write(probNegDist.mkString("\n") + "\n")
    out1.close
    out2.close
    
    val out3= new java.io.FileWriter(dir + fname3)
    val out4 = new java.io.FileWriter(dir + fname4)
    
    out3.write(cumulativeDist.mkString("\n") + "\n")
    out4.write(cumulativeNegDist.mkString("\n") + "\n")
    out3.close
    out4.close
    
    var temp = Runtime.getRuntime().exec("gnuplot " + dir + "plotfile.txt")
    var err_read = new java.io.BufferedReader(new java.io.InputStreamReader(temp.getErrorStream()))
    var line = err_read.readLine()
    while (line != null) {
      println(line)
      line = err_read.readLine()
    }
    
    temp = Runtime.getRuntime().exec("gnuplot " + dir + "plotfilec.txt")
    err_read = new java.io.BufferedReader(new java.io.InputStreamReader(temp.getErrorStream()))
    line = err_read.readLine()
    while (line != null) {
      println(line)
      line = err_read.readLine()
    }
    }
  }
}
