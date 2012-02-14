package edu.berkeley.cs.amplab.carat
import scala.collection.immutable.TreeSet


/**
 * For a battery measurement of 75 and 75, the actual drain can be from 0 to 5 %.
 * for 75 and 70, it can be from 0 to 10%.
 */
class UniformDist(val from:Double, val to:Double) extends Serializable{
  
  def contains(x:Double) = from >= x && x <= to
  
  def getEv() = (from + to) / 2

  /**
   * Discretize the distribution to values accurate to `decimals` decimals.
   */
  def discretize(decimals: Int) = {
    var result = new TreeSet[Double]
    var mul = 1.0
    for (k <- 0 until decimals)
      mul *= 10
    val f = math.round(from * mul)
    val t = math.round(to * mul)

    for (k <- f until t)
      result += (k / mul)
    // add the end point too
    result += t/mul
    result
  }
  
  def this(batt1:Double, batt2:Double, timeStart:Double, timeEnd:Double) = {
    this((batt1 - batt2)/(timeEnd-timeStart), (batt1+5.0 - batt2)/(timeEnd-timeStart))
  }
}