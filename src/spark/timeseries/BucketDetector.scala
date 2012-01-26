package spark.timeseries

/**
 * Put consecutive items into buckets based on their characteristics.
 * This can be used to for example put log messages to buckets based on their source or time.
 * It is used in the example program [[spark.timeseries.BucketLogsByHour]].
 * 
 * Call `update()` to signal change of bucket. 
 */
trait BucketDetector[T] extends Serializable{
  /**
   * Return true when the sample before this one was the last of a run.
   * @param sample a single sample of data type T. 
   * @return true if `sample` belongs to a new run, and false otherwise.
   */
  def update(sample: T): Boolean
  
  /**
   * Resets the detector so that it considers the next sample independent of previous ones.
   */
  def reset()
}
