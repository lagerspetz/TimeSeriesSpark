package edu.berkeley.cs.amplab.carat

import scala.io.Source
import scala.collection.immutable.TreeMap
import scala.sys.process._


object DataPlotter extends App {

  val dir = "data/charts/"
    
  var hogs = false
  var bugs = false
  val lines = Source.fromFile("data/charts/alltables-2012-02-09.txt").getLines()
  for (k <- lines){
    //println("Line: " + k)
    if (k.endsWith(hogsTable)){
      if (!hogs)
        println("Hogs:")
      hogs = true
    }
    if (k.endsWith(bugsTable)){
      if (!hogs)
        println("Bugs:")
      hogs = false
      bugs = true
    }
    if (hogs && k.startsWith(hogKey)){
      
      // collect and plot hogs
      //println("Line:" + k + " start with: " + appKey)
      parseLine(k, "hog")
    }
    
    if (bugs && k.startsWith(hogKey)){
      
      // collect and plot hogs
      //println("Line:" + k + " start with: " + appKey)
      parseLine(k, "bug")
    }
  }

  def parseLine(k: String, t:String) = {
    val s = k.split("->").map(typeStuff(_))
    val nextKeys = s.map(_._2)
    //println("Parsed line:" + s.mkString("# "))
    var xmax = 0.0
    if (s != null) {
      if (s(s.length - 2)._2 == ("xmax")) {
        xmax = s.last._1.toDouble
      }
      if (xmax > 0) {
        var probIndex = nextKeys.indexOf("prob")+1
        var probNegIndex = nextKeys.indexOf("probNeg")+1
        val fname = t+"_" + s(1)._1
        val fname1 = fname+".txt"
        val fname2 = fname+"_neg.txt"
        val plotfile = new java.io.FileWriter(dir+"plotfile.txt")
        plotfile.write("set term postscript eps enhanced color 'Helvetica' 12\nset xtics out\n"+
    "set xlabel \"Battery drain %/s\"\n"+
    "set ylabel \"Probability\"\n")
        plotfile.write("set output \""+dir+fname+".eps\"\n")
        plotfile.write("plot \""+dir+fname1+"\" using 1:2 with linespoints, \""+dir+fname2+"\" using 1:2 with linespoints\n")
        plotfile.close
        val out1 = new java.io.FileWriter(dir+fname1)
        val out2 = new java.io.FileWriter(dir+fname2)
        
        //println(s(1)._1)
        val probDist = prob(xmax, s(probIndex)._1).map(x => {
          x._1 + " " + x._2
        })
        val probNegDist = prob(xmax, s(probNegIndex)._1).map(x => {
          x._1 + " " + x._2
        })
        //println(probDist.mkString("\n"))
        out1.write(probDist.mkString("\n")+"\n")
        //println(probNegDist.mkString("\n"))
        out2.write(probNegDist.mkString("\n")+"\n")
        out1.close
        out2.close
        val temp = Runtime.getRuntime().exec("gnuplot "+dir+"plotfile.txt")
        val err_read = new java.io.BufferedReader(new java.io.InputStreamReader(temp.getErrorStream()))
        var line = err_read.readLine()
        while (line != null){
          println(line)
          line = err_read.readLine()
        }
      }
    }
  }
  
  def typeStuff(s:String) = {
    var colon = s.indexOf(':')
    val curly = s.lastIndexOf('}')
    if (colon > 0 && curly > 0){
      var ccut = curly-2
      if (s(ccut-1) == ']' )
        ccut -= 1
      if (s(colon+2) == '[' )
        colon += 1
      if (curly+2 < s.length())
        (s.substring(colon+2, ccut).trim, s.substring(curly+2).trim)
      else
        (s.substring(colon+2, ccut).trim, null)
    }else
      (s.trim, null)
  }
  
  def prob(xmax:Double, s:String) = {
    var tm = new TreeMap[Double, Double]
    val pairs = s.split(",[ ]*")
    for (k <- pairs){
      val pair = k.split(";")
      val x = pair(0).toInt/100.0*xmax
      val y = pair(1).toDouble
      tm+= ((x, y))
    }
    tm
  }
  
  
}