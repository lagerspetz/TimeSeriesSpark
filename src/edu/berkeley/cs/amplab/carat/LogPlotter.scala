package edu.berkeley.cs.amplab.carat

import scala.io.Source
import scala.collection.immutable.TreeMap
import scala.sys.process._


object LogPlotter extends App {

  val kw = "Considering"
  val dir = "data/charts/"
    
  var tp = ""
    
  var data = false
  
  var prob = new TreeMap[Double, Double]
  var probNeg = new TreeMap[Double, Double]
  var target = prob
  
  var fname = ""
  var nameparts=""
  
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
      probNeg = target
      target = prob
    }else if (data){
      val arr = k.split(" ")
      if (arr.length > 2){
        // turn data off at the end of this section
        data = false
        probNeg = target
        // and plot the distributions!
        plotDistributions(prob, probNeg, fname, "With "+ nameparts.substring(1), "Without " + nameparts.substring(1))
      }else if (arr.length == 1 && k == "probNeg"){
        prob = target
        target = probNeg
      }else if (arr.length == 2){
        target += ((arr(0).toDouble, arr(1).toDouble))
      }
      // turn data off at the end of this
    }
  }

  def plotDistributions(prob: TreeMap[Double, Double], probNeg: TreeMap[Double, Double], fname: String, t1:String, t2:String) {
    val fname1 = fname + ".txt"
    val fname2 = fname + "_neg.txt"
    // hour
    val mul = 3600
    
    val plotfile = new java.io.FileWriter(dir + "plotfile.txt")
    plotfile.write("set term postscript eps enhanced color 'Arial' 24\nset xtics out\n" +
      "set size 1.93,1.1\n"+
      "set xlabel \"Battery drain % / s\"\n" +
      "set ylabel \"Probability\"\n")
    plotfile.write("set output \"" + dir + fname + ".eps\"\n")
    plotfile.write("plot \"" + dir + fname1 + "\" using 1:2 with linespoints lt rgb \"#f3b14d\" lw 2 title \""+t1+"\", "+
        "\"" + dir + fname2 + "\" using 1:2 with linespoints lt rgb \"#007777\" lw 2 title \""+t2+"\"\n")
    plotfile.close
    val out1 = new java.io.FileWriter(dir + fname1)
    val out2 = new java.io.FileWriter(dir + fname2)

    val probDist = prob.map(x => {
      x._1 + " " + x._2
    })
    val probNegDist = probNeg.map(x => {
      x._1 + " " + x._2
    })
    //println(probDist.mkString("\n"))
    out1.write(probDist.mkString("\n") + "\n")
    //println(probNegDist.mkString("\n"))
    out2.write(probNegDist.mkString("\n") + "\n")
    out1.close
    out2.close
    val temp = Runtime.getRuntime().exec("gnuplot " + dir + "plotfile.txt")
    val err_read = new java.io.BufferedReader(new java.io.InputStreamReader(temp.getErrorStream()))
    var line = err_read.readLine()
    while (line != null) {
      println(line)
      line = err_read.readLine()
    }
  }
}