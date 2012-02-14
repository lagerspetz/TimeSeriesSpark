package spark.timeseries

import scala.collection.immutable.TreeMap

/**
 * Various utilities for probability distribution processing.
 */
object ProbUtil {

  /**
   * Get the expected value of a probability distribution.
   * The EV is x*y / sum(y), where sum(y) is 1 for a probability distribution.
   */
  def getEv(values: TreeMap[Double, Double]) = {
    val m = values.map(x => {
      x._1 * x._2
    }).toSeq
    m.sum
  }

  /**
   * Debug: Print non-zero values of two sets.
   */
  def debugNonZero(one: Iterable[Double], two: Iterable[Double], kw1: String) {
    debugNonZeroOne(one, kw1)
    debugNonZeroOne(two, kw1 + "Neg")
  }

  /**
   * Debug: Print non-zero values of a set.
   */
  def debugNonZeroOne(one: Iterable[Double], kw: String) {
    val nz = one.filter(_ > 0)
    println("Nonzero " + kw + ": " + nz.mkString(" ") + " sum=" + nz.sum)
  }

  /**
   * Debug: Utility function that helps plotting distributions by outputting them one pair per line.
   */
  def plot(values: TreeMap[Double, Double], others: TreeMap[Double, Double]) {
    println("prob")
    for (k <- values)
      println(k._1 + " " + k._2)
    println("probNeg")
    for (k <- others)
      println(k._1 + " " + k._2)
  }

  /**
   * Bucket given distributions into `buckets` buckets, and return the maximum x value and the bucketed distributions.
   */
  def bucketDistributionsByX(values: TreeMap[Double, Double], others: TreeMap[Double, Double], buckets: Int, decimals: Int) = {
    var bucketed = new TreeMap[Int, Double]
    var bucketedNeg = new TreeMap[Int, Double]

    val xmax = math.max(values.last._1, others.last._1)

    for (k <- values) {
      val x = k._1 / xmax
      var bucket = (x * buckets).toInt
      if (bucket >= buckets)
        bucket = buckets - 1
      var old = bucketed.get(bucket).getOrElse(0.0)
      bucketed += ((bucket, nDecimal(old + k._2, decimals)))
    }

    for (k <- others) {
      val x = k._1 / xmax
      var bucket = (x * buckets).toInt
      if (bucket >= buckets)
        bucket = buckets - 1
      var old = bucketedNeg.get(bucket).getOrElse(0.0)
      bucketedNeg += ((bucket, nDecimal(old + k._2, decimals)))
    }

    for (k <- 0 until buckets) {
      if (!bucketed.contains(k))
        bucketed += ((k, 0.0))
      if (!bucketedNeg.contains(k))
        bucketedNeg += ((k, 0.0))
    }

    (xmax, bucketed, bucketedNeg)
  }

  /**
   * Get our own variant of the KS distance,
   * where negative values are ignored, from a regular, non-cumulative distribution.
   * The cumulative distribution values are constructed on the fly and discarded afterwards.
   */
  def getDistanceNonCumulative(one: TreeMap[Double, Double], two: TreeMap[Double, Double]) = {
    genericDistance(one, two, distance, signedReplace)
  }

  /**
   * Get a signed distance variant of the KS metric from a regular, non-cumulative distribution.
   * The cumulative distribution values are constructed on the fly and discarded afterwards.
   */
  def getDistanceAbs(one: TreeMap[Double, Double], two: TreeMap[Double, Double]) = {
    genericDistance(one, two, distance, absReplace)
  }

  /**
   * Get the weighted distance: average of x values times the distance, from a non-cumulative distribution.
   * The cumulative distribution values are constructed on the fly and discarded afterwards.
   *
   * Calculates the weighted distance: average of x values times the distance.
   */
  def getDistanceWeighted(one: TreeMap[Double, Double], two: TreeMap[Double, Double]) = {
    genericDistance(one, two, weightedDistance, absReplace)
  }

  def genericDistance(one: TreeMap[Double, Double], two: TreeMap[Double, Double], distanceFunction: (Double, Double, Double, Double) => Double, shouldReplace: (Double, Double) => Boolean) = {
    // Definitions:
    // result will be here
    var maxDistance = 0.0
    // represents previous value of distribution with a smaller starting value
    var prevTwo = (0.0, 0.0)
    // represents next value of distribution with a smaller starting value
    var nextTwo = prevTwo
    // Guess which distribution has a smaller starting value
    var smaller = one
    var bigger = two

    /* Swap if the above assignment was not the right guess: */
    if (one.size > 0 && two.size > 0) {
      if (one.head._1 > two.head._1) {
        smaller = two
        bigger = one
      }
    }

    // use these to keep the cumulative distribution current value
    var sumOne = 0.0
    var sumTwo = 0.0

    //println("one.size=" + one.size + " two.size=" + two.size)

    // advance the smaller dist manually
    var smallIter = smaller.iterator
    // and the bigger automatically
    for (k <- bigger) {
      // current value of bigger dist
      sumOne += k._2

      // advance smaller past bigger, keep prev and next
      // from either side of the current value of bigger
      while (smallIter.hasNext && nextTwo._1 <= k._1) {
        var temp = smallIter.next
        sumTwo += temp._2

        // assign cumulative dist value
        nextTwo = (temp._1, sumTwo)
        //println("nextTwo._1=" + nextTwo._1 + " k._1=" + k._1)
        if (nextTwo._1 <= k._1) {
          prevTwo = nextTwo
        }
      }

      /* now nextTwo >= k > prevTwo */

      /* (NoApp - App) gives a high positive number
         * if the app uses a more energy. This is because
         * if the app distribution is shifted to the right,
         * it has a high probability of running at a high drain rate,
         * and so its cumulative dist value is lower, and NoApp
         * has a higher value. Inverse for low energy usage. */

      val distance = {
        if (smaller == two)
          distanceFunction(prevTwo._2, sumOne, k._1, prevTwo._1)
        else
          distanceFunction(sumOne, prevTwo._2, k._1, prevTwo._1)

      }
      if (shouldReplace(maxDistance, distance))
        maxDistance = distance
    }
    maxDistance
  }

  def absReplace(max: Double, dist: Double) = math.abs(dist) > math.abs(max)

  def signedReplace(max: Double, dist: Double) = dist > max

  def distance(y1: Double, y2: Double, x1: Double, x2: Double) = y1 - y2

  def weightedDistance(y1: Double, y2: Double, x1: Double, x2: Double) = (y1 - y2) * (x1 - x2) / 2

  def nDecimal(orig: Double, decimals: Int) = {
    var mul = 1.0
    for (k <- 0 until decimals)
      mul *= 10
    math.round(orig * mul) / mul
  }

  def nInt(orig: Double, decimals: Int) = {
    var mul = 1.0
    for (k <- 0 until decimals)
      mul *= 10
    math.round(orig * mul)
  }
}