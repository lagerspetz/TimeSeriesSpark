package spark.timeseries
import spark._
import spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue

/**
 * A class that represents an RDD of Arrays of a specified type. Each Array is a partition or "bucket" of items
 * that belong together. Usage:
 * 
 {{{
import spark._
import spark.SparkContext._
import spark.timeseries._

val sc = TimeSeriesSpark.init("local[1]", "default")
    
val file = sc.textFile("fileName")
val mapped = file.map(_.split(" ", 8))
    
var det = new HourBucketDetector()
val buckets = new BucketRDD(mapped, det)

buckets.cache()

val sizes = buckets.map(_.length).collect()

for(k <- sizes)
  println(k)
}}}

See [[spark.timeseries.BucketLogsByHour]] for a more real example.
 * 
 * Accepts a [[spark.timeseries.BucketDetector]] to assign data items into buckets.
 * 
 */
class BucketRDD[T: ClassManifest]
(prev: RDD[T], detector: BucketDetector[T])
extends RDD[Array[T]](prev.context) {
  val prevSplits = prev.splits

  override def splits = prev.splits

  override val dependencies = List(new OneToOneDependency(prev))

  /**
   * Assigns log messages to buckets, using the [[spark.timeseries.BucketDetector[T]]] given.
   */
  override def compute(split: Split): Iterator[Array[T]] = {
    /* I think the reset is not needed, since state is not carried from one compute() to the next. */
    //detector.reset()
    
    var itemCounter = 0
    var udebug = false
    // starting index, i.e. which split we are processing
    val startIdx = split.index
    
    // points to the current split being processed
    var idx = startIdx
    // which split did the last run start in?
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
    // Whether a run has been found after entering a split past the starting split
    var splitAfter = false

    // If this is not the first split, assume we start in a run (so that we
    // don't add the same values in the previous split's output and this one's)
    if (idx > 0) {
      skipFirstRun = true
    }
    while (idx < prevSplits.length && (idx == startIdx || (idx > startIdx && !splitAfter))) {

      if (curSplitIter.hasNext) {
        val k = curSplitIter.next

        // feed each value to the detector
        var bucketFinished = detector.update(k)
        posInSplit += 1

        // Did the run finish before k?
        if (bucketFinished) {
            runsFound+=1
            /* this needs to be set even when skipping, because the 1st run may have progressed into
             * another split. */
            if (idx > startIdx)
                splitAfter = true
            if (udebug) println("BucketFinished run " + runsFound + " skip=" + skipFirstRun + " splitAfter=" + splitAfter + " curRun=" + curRun.length)
            if (runsFound > 1 || !skipFirstRun){
              runs += curRun.toArray
              
              lastRun=idx
              if (udebug)
                println("Added run "+runsFound+" length="+curRun.length + " startIdx=" +
                  startIdx + " idx="+lastRun +" to " +idx + " skip=" + skipFirstRun + " splitAfter=" + splitAfter)

            }
            curRun = new ArrayBuffer[T]
            itemCounter = 0
        }
        /* Add the current element to the (possibly new) run,
         * but only if we are in the first split, or it is the first run in a new split.
         */
        if (idx == startIdx || !splitAfter){
          //println("Adding element to run because " +idx +" == " +startIdx +" or splitAfter=" +splitAfter)
          itemCounter+=1
          curRun += k
        }
      } else {
        /* Proceed to next split */
        idx += 1
        posInSplit = 0
        if (idx < prevSplits.length)
          curSplitIter = prev.iterator(prevSplits(idx))
        if (udebug) {
          println("Going to split " + idx + " splitAfter=" + splitAfter + " startIdx="
            + startIdx + " runs=" + runsFound + " skip=" + skipFirstRun)
        }
      }
    }
    
    // If we reached the end of the splits and we're still in a run, add it
    if (idx == prevSplits.length && curRun.length > 0) {
      runsFound += 1
      if (runsFound > 1 || !skipFirstRun){
        runs += curRun.toArray
        //println("Adding remaining run " + runsFound + " of length="+curRun.length + " (itemCounter=" +itemCounter+") splitAfter=" + splitAfter)
        itemCounter = 1
      }
    }
    //println("returning run iterator")
    return runs.iterator
  }
}
