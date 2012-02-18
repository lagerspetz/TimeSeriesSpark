package spark.timeseries

import scala.collection.immutable.TreeMap
import edu.berkeley.cs.amplab.carat.UniformDist
import spark.Logging

/**
 * Various utilities for probability distribution processing.
 */
object ProbUtil extends Logging{

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
   * Get the expected value of a probability distribution.
   * The EV is x*y / sum(y), where sum(y) is 1 for a probability distribution.
   */
  def getEv(values: TreeMap[Int, Double], xmax: Double) = {
    val m = values.map(x => {
      (x._1 + 0.5) * (xmax / values.size) * x._2
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
   * Bucket given distributions into `buckets` buckets, and return the maximum x value and the bucketed distributions.
   */
  def bucketDistributionsByX(withDist: Array[UniformDist], withoutDist: Array[UniformDist], buckets: Int, decimals: Int) = {
    var bucketed = new TreeMap[Int, Double]
    var bucketedNeg = new TreeMap[Int, Double]

    var xmax = 0.0
    /* Find min and max x*/
    for (d <- withDist) {
      if (d.to > xmax)
        xmax = d.to
    }

    for (d <- withoutDist) {
      if (d.to > xmax)
        xmax = d.to
    }

    var bigtotal = 0.0
    var bigtotal2 = 0.0

    var withPoint = withDist.filter(_.isPoint).map(_.from)
    var withoutPoint = withoutDist.filter(_.isPoint).map(_.from)

    /* Iterate over buckets and put uniform values into them */
    for (k <- 0 until buckets) {
      val bucketStart = k * xmax / buckets
      val bucketEnd = bucketStart + xmax / buckets

      val count = withDist.filter(!_.isPoint()).map(_.probOverlap(bucketStart, bucketEnd)).sum
      val count2 = withoutDist.filter(!_.isPoint()).map(_.probOverlap(bucketStart, bucketEnd)).sum

      logDebug("Bucket %s from %s to %s: count1=%s count2=%s\n".format(k, bucketStart, bucketEnd, count, count2))

      bigtotal += count
      bigtotal2 += count2

      val old = bucketed.get(k).getOrElse(0.0) + count
      val old2 = bucketedNeg.get(k).getOrElse(0.0) + count2

      bucketed += ((k, old))
      bucketedNeg += ((k, old2))
    }

    /* Add point measurements */

    for (k <- withPoint) {
      val x = k / xmax
      var bucket = (x * buckets).toInt
      if (bucket >= buckets)
        bucket = buckets - 1
      var old = bucketed.get(bucket).getOrElse(0.0)
      bucketed += ((bucket, old + 1))
      bigtotal += 1
    }

    /* Add point measurements */
    for (k <- withoutPoint) {
      val x = k / xmax
      var bucket = (x * buckets).toInt
      if (bucket >= buckets)
        bucket = buckets - 1
      var old = bucketedNeg.get(bucket).getOrElse(0.0)
      bucketedNeg += ((bucket, old + 1))
      bigtotal2 += 1
    }

    var ev1 = 0.0
    var ev2 = 0.0

    /* Normalize dists */

    for (k <- 0 until buckets) {
      val norm = nDecimal(bucketed.get(k).getOrElse(0.0) / bigtotal, decimals)
      bucketed += ((k, norm))

      val norm2 = nDecimal(bucketedNeg.get(k).getOrElse(0.0) / bigtotal2, decimals)
      bucketedNeg += ((k, norm2))

      ev1 += (k + 0.5) / buckets * xmax * bucketed.get(k).getOrElse(0.0)
      ev2 += (k + 0.5) / buckets * xmax * bucketedNeg.get(k).getOrElse(0.0)

      logDebug("Norm Bucket %s: val=%s val2=%s\n".format(k, norm, norm2))
    }

    (xmax, bucketed, bucketedNeg, ev1, ev2)
  }

  def getLogBase(buckets: Int, smallestBucket: Double, xmax: Double) = math.pow(math.E, math.log(xmax / smallestBucket) / buckets)

  /**
   * Bucket given distributions into `buckets` buckets, that have log sizes
   * (smaller at the low end) and return the maximum x value and the bucketed distributions.
   *
   * Suggested parameters: buckets = 100, smallestBucket = 0.0001, decimals = 3 or 4
   *
   * For a smallest bucket upper boundary of 0.0001,
   * the maximum battery consumption that falls into
   * it would use the iPhone battery in 11.5 days.
   * This is unrealistic. A 0.0005 % /s
   * usage falls into the 87th bucket,
   * and drains the battery in 2.5 days. This is more realistic.
   * As the buckets go to the right, higher and higher usage is
   * bucketed, with a larger bucket size, making heavy usage with even a
   * high variance fall into the same bucket.
   *
   */
  def logBucketDistributionsByX(withDist: Array[UniformDist], withoutDist: Array[UniformDist], buckets: Int, smallestBucket: Double, decimals: Int) = {
    var bucketed = new TreeMap[Int, Double]
    var bucketedNeg = new TreeMap[Int, Double]

    var xmax = 0.0
    /* Find min and max x*/
    for (d <- withDist) {
      if (d.to > xmax)
        xmax = d.to
    }

    for (d <- withoutDist) {
      if (d.to > xmax)
        xmax = d.to
    }

    /* xmax / (logBase^buckets) > smallestBucket
     * <=> logBase^buckets * smallestBucket < xmax
     * <=> logBase^buckets < xmax / smallestBucket
     * log (logbase) * buckets < log (xmax/smallestBucket)
     * logbase < e^(log(xmax/smallestBucket) / buckets)
     */

    val logbase = getLogBase(buckets, smallestBucket, xmax)

    /**
     * TODO: Finish this...
     */

    var bigtotal = 0.0
    var bigtotal2 = 0.0

    /* Iterate over buckets and add fractions of ranges that fall into them */
    for (k <- 0 until buckets) {
      val bucketStart = {
        if (k == 0)
          0.0
        else
          xmax / (math.pow(logbase, buckets - k))
      }
      val bucketEnd = xmax / (math.pow(logbase, buckets - k - 1))

      val count = withDist.filter(!_.isPoint()).map(_.probOverlap(bucketStart, bucketEnd)).sum

      val count2 = withoutDist.filter(!_.isPoint()).map(_.probOverlap(bucketStart, bucketEnd)).sum

      bigtotal += count
      bigtotal2 += count2

      val old = bucketed.get(k).getOrElse(0.0) + count
      val old2 = bucketedNeg.get(k).getOrElse(0.0) + count2

      logDebug("Bucket %s from %s to %s: count1=%s count2=%s\n".format(k, bucketStart, bucketEnd, old, old2))

      bucketed += ((k, old))
      bucketedNeg += ((k, old2))
    }

    /* Add point values */

    var withPoint = withDist.filter(_.isPoint).map(_.from)
    var withoutPoint = withoutDist.filter(_.isPoint).map(_.from)

    for (k <- withPoint) {
      val bucketDouble = 100 - math.log(xmax / k) / math.log(logbase)
      val bucket = {
        if (bucketDouble >= buckets)
          buckets - 1
        else if (bucketDouble < 0)
          0
        else
          bucketDouble.toInt
      }
      var old = bucketed.get(bucket).getOrElse(0.0)
      logDebug("With Point value %s bucket %s count %s\n".format(k, bucket, old + 1))
      bucketed += ((bucket, old + 1))
      bigtotal += 1
    }

    for (k <- withoutPoint) {
      val bucketDouble = 100 - math.log(xmax / k) / math.log(logbase)
      val bucket = {
        if (bucketDouble >= buckets)
          buckets - 1
        else if (bucketDouble < 0)
          0
        else
          bucketDouble.toInt
      }
      var old = bucketedNeg.get(bucket).getOrElse(0.0)
      logDebug("Without Point value %s bucket %s count %s\n".format(k, bucket, old + 1))
      bucketedNeg += ((bucket, old + 1))
      bigtotal2 += 1
    }

    /* Normalize dists: */

    var ev1 = 0.0
    var ev2 = 0.0

    var checksum1 = 0.0
    var checksum2 = 0.0

    for (k <- 0 until buckets) {
      val bucketStart = {
        if (k == 0)
          0.0
        else
          xmax / (math.pow(logbase, buckets - k))
      }
      val bucketEnd = xmax / (math.pow(logbase, buckets - k - 1))
      var old = bucketed.get(k).getOrElse(0.0)
      val norm = old / bigtotal
      bucketed += ((k, nDecimal(norm, decimals)))

      old = bucketedNeg.get(k).getOrElse(0.0)
      val norm2 = old / bigtotal2
      bucketedNeg += ((k, nDecimal(norm2, decimals)))
      checksum1 += norm
      checksum2 += norm2
      logDebug("Norm Bucket %s: val=%4.4f val2=%4.4f\n".format(k, norm, norm2))
      ev1 += (bucketEnd - bucketStart) / 2 * bucketed.get(k).getOrElse(0.0)
      ev2 += (bucketEnd - bucketStart) / 2 * bucketedNeg.get(k).getOrElse(0.0)
    }

    if (bigtotal > 0) {
      val contSum = bucketed.map(_._2).sum
      assert(contSum <= 1.01 && contSum >= 0.99, "Continuous value \"with\" distribution should sum up to 1 when normalized: " + contSum)
    }

    if (bigtotal2 > 0) {
      val contSum = bucketedNeg.map(_._2).sum
      assert(contSum <= 1.01 && contSum >= 0.99, "Continuous value \"without\" distribution should sum up to 1 when normalized: " + contSum)
    }

    val (sane, sanitySum1) = sanityCheck(bucketed)
    if (!sane)
      throw new Error("Bucketed sums up to " + sanitySum1 + "!")
    val (sane2, sanitySum2) = sanityCheck(bucketedNeg)
    if (!sane2)
      throw new Error("BucketedNeg sums up to " + sanitySum2 + "!")

    (xmax, bucketed, bucketedNeg, ev1, ev2)
  }

  /** Sanity check prob distribution */
  def sanityCheck(bucketed: TreeMap[Int, Double]) = {
    val sum = bucketed.map(_._2).sum
    (sum >= 0.99 && sum <= 1.01, sum)
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