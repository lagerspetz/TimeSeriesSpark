package spark.timeseries
import spark._
import spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue

/**
 * A class that represents an RDD of [[spark.timeseries.Run]]s.
 * 
 * Splits up a parent RDD containing a series of measurement data into meaningful partitions such as measurement runs,
 * activity peaks, etc.
 * Accepts a [[spark.timeseries.RunDetector]] to detect [[spark.timeseries.Run]]s from data
 * and partition the data into them.
 * 
 * Overlapping runs are currently not supported. To support them, store data in the [[spark.timeseries.RunDetector]] implementation. 
 * 
 * @author Eemil Lagerspetz,Matei Zaharia
 * 
 */
class RunRDD[T: ClassManifest]
(prev: RDD[Array[T]], detector: RunDetector[T])
extends RDD[Run[T]](prev.context) {
  val prevSplits = prev.splits

  override def splits = prev.splits

  override val dependencies = List(new OneToOneDependency(prev))

  /**
   * Computes the splits according to the [[spark.timeseries.RunDetector[T]]] given.
   * 
   */
  override def compute(split: Split): Iterator[Run[T]] = {
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
    val runs = new ArrayBuffer[Run[T]]
    // Accumulates values for the current run
    var curRun = new ArrayBuffer[Array[T]]

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
          //println("detector ready")
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
              //println("Run finished")
              // We finished the current run; add it to our sequence to return
              runsFound += 1
              if (runsFound > 1 || !skipFirstRun){
                //println("run startIdx=" + startIdx + " idx="+lastRun +" to " +idx)
                lastRun=idx
                runs += new Run[T](curRun.toArray: _*)
              }
              curRun = new ArrayBuffer[Array[T]]
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
        runs += new Run[T](curRun.toArray: _*)
    }
    //println("returning run iterator")
    return runs.iterator
  }
}

