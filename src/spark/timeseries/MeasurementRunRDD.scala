package spark.timeseries

import spark._
import spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue

/**
Time Series RDD class. Splits up a parent RDD containing a series of measurement data into meaningful partitions such as measurement runs,
activity peaks, etc. Splitting is done by a an user input class class.

General idea:
compute() will be called on each split of the previous RDD.
one run of compute should detect runs that have not begun in previous splits.
This can be done in the following manner:

* Each time we read a bit to the next split, so continued runs can be
recorded until their end.

1. If we are in the first split, record all runs.
2. else skip the first run, since it has been read by a previous compute()
3. supply data to the detector until ready()
4. if the detector says we are in a run, and we are not to skip it, save it until no longer in a run
5. keep processing until no longer in a run, or until in the next split since starting, or until
there are no more splits to process.

X. Do we want to support runs that can overlap each other?
   The user may want to look at parts of a combined run
   separately, for example because the duration that a GPS
   sensor is on overlaps with the duration of heavy processing.

@author Eemil Lagerspetz,Matei Zaharia
*/

trait MeasurementRunDetector[T] extends Serializable{
  /** Call to re-use instance as a clean one. */
  def reset()
  /** Returns true when subsequent calls to inRun() will
      provide meaningful information. */
  def ready(): Boolean
  /** if ready(), returns true when a measurement run is detected, and false
      when not detected. */
  def inRun(): Boolean
  /** Call to inform the detector of moving to a new split of the RDD.
      Causes the detector to return false for ready() until enough information
      is gathered from the new split via update(). */
  def splitChanged()
  /** Call to give each data element to the detector.
      Improves detection of runs. Enough update() calls should result in
      ready() returning true. */
  def update(item: T)
  /** Returns a portion of data to be prepended to the current detected run.
      Call this after the first time inRun() returns true, i.e., a new run
      is detected. */
  def prepend(): Queue[T]
}

/**
  Represents an RDD that spans all measurement runs in the data,
  detected by a user-defined RunDetector.
  TODO: Is there a synchronization problem since we only have one RunDetector?
  FIXME: Possible issue with runs being skipped.
*/
@deprecated
class MeasurementRunRDD[T: ClassManifest]
(prev: RDD[T], detector: MeasurementRunDetector[T])
extends RDD[Array[T]](prev.context) {
  val prevSplits = prev.splits

  override def splits = prev.splits

  override val dependencies = List(new OneToOneDependency(prev))

  override def compute(split: Split): Iterator[Array[T]] = {
    /* The detector needs to be reset in the beginning, since
       a single instance could be used for all invocations of
       compute()
     */
    detector.reset()

    // starting index, i.e. which split we are processing
    val startIdx = split.index
    /* whether values are being recorded as a measurement run.
     * false means values are being ignored.
     */

    var inRun = false
    // points to the current split being processed
    var idx = startIdx
    // DEBUG
    var lastRun = idx
    // What is our position in the split (number of values processed)
    var posInSplit = 0
    // How many runs have been discovered since started
    var runsFound = 0
    // Whether to skip the first run because it has been detected previously
    var skipFirstRun = false
    // Iterator to current split
    var curSplitIter = prev.iterator(prevSplits(idx))
    // Return value that contains all found runs
    val runs = new ArrayBuffer[Array[T]]
    // Accumulates values for the current run
    var curRun = new ArrayBuffer[T]

    // If this is not the first split, assume we start in a run (so that we
    // don't add the same values in the previous split's output and this one's)
    if (idx > 0) {
      inRun = true
      skipFirstRun = true
    }
    //println("compute start=" +startIdx)
  
    /* index is within the RDD and
       ( (index is the "next" split and position is less than windowlength)
       or we are in a run ) */
/* new behaviour: index is within the RDD and either we are in the first split,
or we are in a run in the second split.*/
    
    while (idx < prevSplits.length && (idx == startIdx || (idx == startIdx+1 && !detector.ready()) || inRun)) {
      //println("idx=" + idx +"det.ready(): " + detector.ready() + " inRun="+inRun)
      /* How to represent posInSplit < windowLength without windowLength?
         Detectors can have a dynamic window length, so it does not make
         sense to fix it.
       */
      if (curSplitIter.hasNext) {
        val k = curSplitIter.next

        // feed each value to the detector
        detector.update(k)
        posInSplit += 1

        if (detector.ready()){
          // Check whether we're in a run
          if (detector.inRun()) {
            if (!inRun) {
              // Start a new run
              //println("New run started")
              inRun = true
              curRun ++= detector.prepend()
            } else {
              // Add the current element to the run
              curRun += k
            }
          } else {
            if (inRun) {
              // We finished the current run; add it to our sequence to return
              // We finished the current run; add it to our sequence to return
              runsFound += 1
              if (runsFound > 1 || !skipFirstRun){
                //println("run startIdx=" + startIdx + " idx="+lastRun +" to " +idx)
                lastRun=idx
                runs += curRun.toArray
              }
              curRun = new ArrayBuffer[T]
              inRun = false
            }
          }
        }
      } else {
        /* Proceed to next split */
        idx += 1
        posInSplit = 0
        if (idx < prevSplits.length)
          curSplitIter = prev.iterator(prevSplits(idx))
        detector.splitChanged()
      }
    }

    // If we reached the end of the splits and we're still in a run, add it
    if (idx == prevSplits.length && inRun) {
      runsFound += 1
      if (runsFound > 1 || !skipFirstRun)
        runs += curRun.toArray
    }
    return runs.iterator
  }
}

