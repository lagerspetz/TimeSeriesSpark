package edu.berkeley.cs.amplab.carat.dynamodb

/**
 * Helper class to allow sorting the RDD of observations.
 * Use with Sample.
 * Example usage:
 * {{{
 * val samples: RDD[(SampleKey, Sample)] = observations.map(x => {
 *   val k = new SampleKey(x._1, x._2.toDouble)
 *   val v = new Sample(x._3, x._4, x._5, x._6, x._7)
 * (k, v)
 * })
 * }}}
 */
class SampleKey(val uuid: String, val time: Double) extends Ordered[SampleKey] with Serializable {

  def compare(that: SampleKey) = {
    if (this.uuid < that.uuid)
      -1
    else if (this.uuid == that.uuid) {
      if (this.time < that.time)
        -1
      else
        1
    } else 1
  }
}
