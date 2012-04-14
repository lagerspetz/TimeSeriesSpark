package edu.berkeley.cs.amplab.carat.dynamodb

/**
 * Class that represents the optional data contained in a Carat sample.
 * Use with SampleKey. Example usage:
 * {{{
 * val samples: RDD[(SampleKey, Sample)] = observations.map(x => {
 *   val k = new SampleKey(x._1, x._2.toDouble)
 *   val v = new Sample(x._3, x._4, x._5, x._6, x._7)
 *   (k, v)
 * })
 * }}}
 */
class Sample(val battery: String, val event: String, val state: String, val apps: scala.collection.mutable.Buffer[String], val features: scala.collection.immutable.Map[String, (String, java.lang.Object)]) extends Serializable {

}