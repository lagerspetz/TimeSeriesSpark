package spark.timeseries
import scala.collection.mutable.Queue

/**
 * trait for detecting periods/buckets/segments of activity in a data file.
 * In case all lines are considered activity, use the default
 * `splitChanged()` and `ready()` implementations,
 * and make inRun() return false once to start a new period/bucket/segment. 
 *
 * Takes as parameter the type of data items on each line.
 * Considers each line a [[scala.Array]] of the data type T.
 */

trait RunDetector[T] extends Serializable {
  
  /** Call to re-use instance as a clean one. */
  def reset()
  
  /**
   * Returns true when subsequent calls to inRun() will
   * provide meaningful information.
   * Default implementation: always return true.
   */
  def ready() = true
  
  /**
   * if ready(), returns true when a measurement run is detected, and false
   * when not detected.
   */
  def inRun(): Boolean
  
  /**
   * Call to inform the detector of moving to a new split of the RDD.
   * Causes the detector to return false for ready() until enough information
   * is gathered from the new split via update().
   * Default implementation: no-op.
   */
  def splitChanged() {}
  
  /**
   * Call to give each data element to the detector.
   * Improves detection of runs. Enough update() calls should result in
   * ready() returning true.
   * @param item the next line of data.
   */
  def update(item: Array[T])
  
  /**
   * Returns a portion of data to be prepended to the current detected run.
   * Call this after the first time inRun() returns true, i.e., a new run
   * is detected.
   * @return a Queue of data items that are part of the beginning of the current run.
   */
  def prepend(): Queue[Array[T]]
}
