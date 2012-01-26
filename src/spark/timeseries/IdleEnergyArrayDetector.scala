package spark.timeseries
import scala.collection.mutable.Queue


/**
 * Detect periods of activity from a data file of [[scala.Array]]s of [[scala.Double]]s, where each [[scala.Array]] consists of time, current, and voltage values.
 *
 * Detection of activity is based on the average power (in mW) being above
 * a given threshold `idle_thresh`. The average is calculated over
 * a period of `windowLength` Arrays.
 *   @param idle_thresh The power threshold above which values are considered activity.
 *   @param windowLength The number of Arrays that averages are calculated over, and compared to `idle_thresh`.
 */

class IdleEnergyArrayDetector(idle_thresh: Double, windowLength: Int) extends RunDetector[Double] {
  var q = new Queue[Array[Double]]()
  var sum = 0.0
  var readyDelay = 0
  /**
   * Resets the internal queue and sum, along with readyDelay.
   */
  def reset() {
    q = new Queue[Array[Double]]()
    sum = 0.0
    readyDelay = 0
  }

  /**
   * Returns whether we are in a run.
   *
   *  @return true when `sum / q.size >= idle_thresh` and `q.size >= windowLength`.
   */
  def inRun(): Boolean = {
    if (q.size < windowLength)
      false
    else
      sum / q.size >= idle_thresh
  }

  /**
   * Returns true when values from [[spark.timeseries.TimeSeriesSpark.IdleArrayDetector.inRun()]] can be trusted.
   *
   * @return true when `readyDelay == 0` and `q.size >= windowLength`
   */
  override def ready() = { readyDelay == 0 && q.size >= windowLength }

  /**
   * Appends `sample` to the internal queue and updates `sum`.
   *
   * Also dequeues the first element if the queue if the queue is full,
   * and decreases `readyDelay` by one if it is greater than zero.
   */
  def update(sample: Array[Double]) {
    if (sample.size < 4) {
      println("Weird sample: " + sample.mkString)
      return
    }
    q += sample
    if (q.size > windowLength) {
      var prev = q.dequeue()
      sum -= prev(2) * prev(3)
    }
    sum += sample(2) * sample(3)
    if (readyDelay > 0)
      readyDelay -= 1
  }

  /**
   * Returns the internal queue.
   */
  def prepend() = q

  /**
   * Call to inform the detector of proceeding to a new split.
   *
   * Sets `readyDelay` to `windowLength` so that `windowLength` samples are put into the queue
   * before `ready()` returns true again. This is to ensure that a run in the beginning of a
   * next split will be detected.
   */
  override def splitChanged() {
    readyDelay = windowLength
  }
}
