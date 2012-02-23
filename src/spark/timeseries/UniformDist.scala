package edu.berkeley.cs.amplab.carat
import scala.collection.immutable.TreeSet


/**
 * For a battery measurement of 75 and 75, the actual drain can be from 0 to 5 %.
 * for 75 and 70, it can be from 0 to 10%.
 */
class UniformDist(val from:Double, val to:Double) extends Ordered[UniformDist] with Serializable{

  /**
   * @return true if x is within [from, to].
   */
  def contains(x: Double) = {
    if (from == to) {
      /* kludge for point values:
       * with 3 decimals,
       * granularity is
       * 0.001, 0.002, ...
       * so 0.0005 can be used to
       * "expand" a point value's range. */
      from - 0.0005 <= x && x < to + 0.0005
    } else
      from <= x && x < to
  }
  
  /**
   * @return true if [start, end] and [from, to] have any common points.
   */
   def overlaps(start:Double, end:Double) = {
    if (isPoint){
      start <= from && to < end
    }else
      (start <= from && from < end) ||
      (from <= start && start < to)
  }

  def probOverlap(start: Double, end: Double) = {
    if (!overlaps(start, end)) {
      0.0
    } else if (isPoint()) {
      1.0
    } else {
      val lowerBound = { if (start > from) start else from }
      assert(lowerBound >= from, "lowerBound should be within the range")
      val upperBound = { if (end < to) end else to }
      assert(upperBound <= to, "upperBound should be within the range")
      val p = (upperBound - lowerBound) * prob
      assert(p <= 1, "probOverlap should not be greater than 1:" + p )
      p
    }
  }
  
  /**
   * Get the expected value of the distribution.
   * @return (from + to) / 2
   */
  def getEv() = (from + to) / 2

  /**
   * Discretize the distribution to values accurate to `decimals` decimals.
   * This generates a lot of values with a high number of decimals, so avoid when possible.
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
  
  /**
   * Alternate constructor that takes y range endpoints and x range endpoints and creates a UniformDist.
   * @param batt1 the battery fraction [0, 1.0] at the start time
   * @param batt2 the battery fraction [0, 1.0] at the end time
   * @param timeStart the start time, in seconds
   * @param timeEnd the end time, in seconds
   * 
   * @return a new UniformDist, that represents a range of battery drain from (x% to y%) / s.
   * The range is from (batt1 - batt2) to (batt1 - batt2)+5% in (timeEnd - timeStart). 
   */
  def this(batt1:Double, batt2:Double, timeStart:Double, timeEnd:Double) = {
    this((batt1 - batt2)*100.0/(timeEnd-timeStart), (batt1+0.05 - batt2)*100.0/(timeEnd-timeStart))
  }
  
  /**
   * Return the uniform probability value,
   * `1 / (to - from)`
   */
  def prob() = {
    if (to == from)
      1.0
    else
      1.0 / (to - from)
  }
  
  /**
   * Return the uniform probability value if the x range point `at` is inside [from, to].
   * Otherwise return 0.
   *  
   */
  def probAt(at: Double) = {
    if (contains(at))
      prob
    else
      0.0
  }
  
  /**
   * 
   * @return true if to == from.
   */
  def isPoint() = to == from

  /**
   * @return -1 if this.from < that.from, -1 if this.from == that.from && this.to < that.to,
   * 0 if both to and from of the two objects are the same,
   * and 1 otherwise.
   */
  def compare(that: UniformDist) = {
    if (this.from < that.from)
      -1
    else if (this.from == that.from && this.to < that.to)
      -1
    else if (this.from == that.from && this.to == that.to)
      0
    else 1
  }
  
  /**
   * toString method that prints the from, to, and the uniform probability.
   */
  override def toString() = "UniformDist from " + from +" to " + to + " (prob=" + prob + ")"
}