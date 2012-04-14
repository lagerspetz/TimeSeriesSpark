package edu.berkeley.cs.amplab.carat.dynamodb

import com.amazonaws.services.dynamodb.model.AttributeValue
import scala.collection.immutable.HashSet
import java.io.File
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileInputStream
import java.io.FileWriter
import collection.JavaConversions._
import edu.berkeley.cs.amplab.carat.s3.S3Decoder
import scala.collection.mutable.ArrayBuffer
import spark.timeseries.UniformDist
import spark._
import spark.SparkContext._
import com.amazonaws.services.dynamodb.model.Key
import scala.collection.immutable.HashMap
import edu.berkeley.cs.amplab.carat.CaratRate
import spark.timeseries.ProbUtil
import scala.collection.immutable.TreeMap
import scala.collection.mutable.Map
import java.util.Date
import scala.collection.mutable.Buffer

object DynamoAnalysisUtil {

  // constants for battery state and sample triggers
  val MODEL_SIMULATOR = "Simulator"
  val STATE_CHARGING = "charging"
  val STATE_UNKNOWN = "unknown"
  val STATE_DISCHARGING = "unplugged"
  val TRIGGER_BATTERYLEVELCHANGED = "batterylevelchanged"
  val ABNORMAL_RATE = 0.04

  val RATE_BUFFER_SIZE = 1000

  val FRESHNESS_SECONDS = 3600 * 2

  val knownKeys = Array(sampleKey, sampleProcesses, sampleTime, sampleBatteryState, sampleBatteryLevel, sampleEvent)

  val DIST_THRESHOLD = 10

  var last_sample_write = 0.0

  var last_reg_write = 0.0

  // Daemons list, read from S3
  lazy val DAEMONS_LIST = DynamoAnalysisUtil.readS3LineSet(BUCKET_WEBSITE, DAEMON_FILE)

  def start() = System.currentTimeMillis()

  def finish(startTime: Long, message: String = null) {
    var fmt = ""
    if (message != null)
      fmt = "-%s".format(message)
    val functionStack = Thread.currentThread().getStackTrace()
    val f = {
      if (functionStack != null && functionStack.length > 0) {
        if (functionStack.length > 3)
          functionStack(2) + fmt + " from " + functionStack(3)
        else if (functionStack.length > 2)
          functionStack(2) + fmt
        else if (functionStack.length > 1)
          functionStack(1) + fmt
      } else
        "edu.berkeley.cs.amplab.carat.dynamodb.DynamoAnalysisUtil.finish" + fmt
    }
    println("Time %s: %d".format(f, (System.currentTimeMillis() - startTime)))
  }

  def readDoubleFromFile(file: String) = {
    val startTime = start
    val f = new File(file)
    if (!f.exists() && !f.createNewFile())
      throw new Error("Could not create %s for reading!".format(file))
    val rd = new BufferedReader(new InputStreamReader(new FileInputStream(f)))
    var s = rd.readLine()
    rd.close()
    finish(startTime)
    if (s != null && s.length > 0) {
      s.toDouble
    } else
      0.0
  }

  def saveDoubleToFile(d: Double, file: String) {
    val startTime = start
    val f = new File(file)
    if (!f.exists() && !f.createNewFile())
      throw new Error("Could not create %s for saving %f!".format(file, d))
    val wr = new FileWriter(file)
    wr.write(d + "\n")
    wr.close()
    finish(startTime)
  }

  def readS3LineSet(bucket: String, file: String) = {
    val startTime = start
    var r: Set[String] = new HashSet[String]
    val rd = new BufferedReader(new InputStreamReader(S3Decoder.get(bucket, file)))
    var s = rd.readLine()
    while (s != null) {
      r += s
      s = rd.readLine()
    }
    rd.close()
    println("%s/%s downloaded: %s".format(bucket, file, r))
    finish(startTime)
    r
  }

  def regSet(regs: java.util.List[java.util.Map[String, AttributeValue]]) = {
    val startTime = start
    var regSet = new HashMap[String, (String, String)]
    regSet ++= regs.map(x => {
      val uuid = { val attr = x.get(regsUuid); if (attr != null) attr.getS() else "" }
      val model = { val attr = x.get(regsModel); if (attr != null) attr.getS() else "" }
      val os = { val attr = x.get(regsOs); if (attr != null) attr.getS() else "" }
      (uuid, (model, os))
    })
    finish(startTime)
    regSet
  }

  def replaceOldRateFile(oldPath: String, newPath: String) {
    val startTime = start
    val rem = Runtime.getRuntime().exec(Array("/bin/rm", "-rf", oldPath + "-backup"))
    rem.waitFor()
    val move1 = Runtime.getRuntime().exec(Array("/bin/mv", oldPath, oldPath + "-backup"))
    move1.waitFor()
    val move2 = Runtime.getRuntime().exec(Array("/bin/mv", newPath, oldPath))
    move2.waitFor()
    finish(startTime)
  }

  /**
   * Get Rates from DynamoDB samples and registrations.
   */

  def getRates(sc: SparkContext, tmpdir: String, clean: Boolean = true) = {
    // Master RDD for all data.

    val RATES_CACHED_NEW = tmpdir + "cached-rates-new.dat"
    val RATES_CACHED = tmpdir + "cached-rates.dat"
    val LAST_SAMPLE = tmpdir + "last-sample.txt"
    val LAST_REG = tmpdir + "last-reg.txt"

    lazy val last_sample = DynamoAnalysisUtil.readDoubleFromFile(LAST_SAMPLE)
    lazy val last_reg = DynamoAnalysisUtil.readDoubleFromFile(LAST_REG)

    val nowS = System.currentTimeMillis() / 1000
    println("Now=%s lastSample=%s diff=%s".format(nowS, last_sample, nowS - last_sample))

    val oldRates: spark.RDD[CaratRate] = {
      val f = new File(RATES_CACHED)
      if (f.exists() && (!clean || (nowS - last_sample < FRESHNESS_SECONDS))) {
        sc.objectFile(RATES_CACHED)
      } else
        null
    }

    var allRates: spark.RDD[CaratRate] = oldRates

    /* TODO: FIXME: featureTracking can only be done for samples, not stored rates.
       * Obvious fix: save samples instead of Rates, then save features and Rates after those. 
      */
    var featureTracking = new scala.collection.mutable.HashMap[String, HashMap[String, ArrayBuffer[(Long, Object)]]]

    /* closure to forget uuids, models and oses after assigning them to rates.
     * This is because new rates may have new uuids, models and oses.
    */
    {
      // Unique uuIds, Oses, and Models from registrations.
      val uuidToOsAndModel = new scala.collection.mutable.HashMap[String, (String, String)]
      // UUID -> [(timestamp,os), (timestamp,model), ...]
      val uuidToOsesAndModels = new scala.collection.mutable.HashMap[String, ArrayBuffer[(Double, String, String)]]
      // TODO: Make uuids to multiple OSes actually usable
      val allModels = new scala.collection.mutable.HashSet[String]
      val allOses = new scala.collection.mutable.HashSet[String]

      if (allRates != null) {
        val devices = allRates.map(x => {
          // the latest time that they had an OS and model
          (x.uuid, (x.time2, x.os, x.model))
        }).groupByKey().collect()
        for (k <- devices) {
          val uuid = k._1
          val s = uuidToOsesAndModels.get(uuid).getOrElse(new ArrayBuffer[(Double, String, String)])
          s.addAll(k._2)
          uuidToOsesAndModels += ((uuid, s.sortWith((x, y) => {
            x._1 < y._1
          })))

          for (j <- k._2) {
            uuidToOsAndModel += ((uuid, (j._2, j._3)))
            allOses += j._2
            allModels += j._3
          }
        }
      }
      /* Only get new rates if we have no old rates, or it has been more than an hour */

      if (allRates == null || (nowS - last_sample > FRESHNESS_SECONDS)) {
        if (!clean && last_reg > 0) {
          DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.filterItemsAfter(registrationTable, regsTimestamp, last_reg + ""),
            DynamoDbDecoder.filterItemsAfter(registrationTable, regsTimestamp, last_reg + "", _),
            handleRegs(_, _, uuidToOsesAndModels, allOses, allModels))
        } else {
          DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.getAllItems(registrationTable),
            DynamoDbDecoder.getAllItems(registrationTable, _),
            handleRegs(_, _, uuidToOsesAndModels, allOses, allModels))
        }

        /* Limit attributesToGet here so that bandwidth is not used for nothing. Right now the memory attributes of samples are not considered. */
        if (!clean && last_sample > 0) {
          allRates = DynamoAnalysisUtil.DynamoDbItemLoop[CaratRate](DynamoDbDecoder.filterItemsAfter(samplesTable, sampleTime, last_sample + ""),
            DynamoDbDecoder.filterItemsAfter(samplesTable, sampleTime, last_sample + "", _),
            handleSamples(sc, _, uuidToOsesAndModels, featureTracking, _),
            true,
            allRates)
        } else {
          allRates = DynamoAnalysisUtil.DynamoDbItemLoop[CaratRate](DynamoDbDecoder.getAllItems(samplesTable),
            DynamoDbDecoder.getAllItems(samplesTable, _),
            handleSamples(sc, _, uuidToOsesAndModels, featureTracking, _),
            true,
            allRates)
        }
      }

      // we may not be interested in these actually.
      println("All uuIds: " + uuidToOsAndModel.keySet.mkString(", "))
      println("All oses: " + allOses.mkString(", "))
      println("All models: " + allModels.mkString(", "))

      printFeatures(featureTracking, uuidToOsesAndModels)
    }
    // save entire rate rdd for later:
    allRates.saveAsObjectFile(RATES_CACHED_NEW)
    DynamoAnalysisUtil.replaceOldRateFile(RATES_CACHED, RATES_CACHED_NEW)
    DynamoAnalysisUtil.saveDoubleToFile(last_sample_write, LAST_SAMPLE)
    DynamoAnalysisUtil.saveDoubleToFile(last_reg_write, LAST_REG)

    allRates
  }

  /**
   * Get Rates from DynamoDB samples and registrations.
   */

  def getRatesUnabriged(sc: SparkContext, tmpdir: String) = {

    // Master RDD for all data.
    var allRates: spark.RDD[CaratRate] = null

    /* closure to forget uuids, models and oses after assigning them to rates.
     * This is because new rates may have new uuids, models and oses.
    */
    {
      // Unique uuIds, Oses, and Models from registrations.
      val uuidToOsAndModel = new scala.collection.mutable.HashMap[String, (String, String)]
      // UUID -> [(timestamp,os), (timestamp,model), ...]
      val uuidToOsesAndModels = new scala.collection.mutable.HashMap[String, ArrayBuffer[(Double, String, String)]]
      // TODO: Make uuids to multiple OSes actually usable
      val allModels = new scala.collection.mutable.HashSet[String]
      val allOses = new scala.collection.mutable.HashSet[String]

      /* Only get new rates if we have no old rates, or it has been more than an hour */

      DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.getAllItems(registrationTable),
        DynamoDbDecoder.getAllItems(registrationTable, _),
        handleRegs(_, _, uuidToOsesAndModels, allOses, allModels))

      /* Limit attributesToGet here so that bandwidth is not used for nothing. Right now the memory attributes of samples are not considered. */

      allRates = DynamoAnalysisUtil.DynamoDbItemLoop[CaratRate](DynamoDbDecoder.getAllItems(samplesTable),
        DynamoDbDecoder.getAllItems(samplesTable, _),
        handleSamplesUnabriged(sc, _, uuidToOsesAndModels, _),
        true,
        allRates)

      println("All uuIds: " + uuidToOsAndModel.keySet.mkString(", "))
      println("All oses: " + allOses.mkString(", "))
      println("All models: " + allModels.mkString(", "))
    }

    allRates
  }

  /**
   * Handles a set of registration messages from the Carat DynamoDb.
   * uuids, oses and models are filled in.
   */
  def handleRegs(key: Key, regs: java.util.List[java.util.Map[String, AttributeValue]],
    uuidToOsesAndModels: scala.collection.mutable.HashMap[String, ArrayBuffer[(Double, String, String)]],
    oses: scala.collection.mutable.Set[String],
    models: scala.collection.mutable.Set[String]) {

    // Get last reg timestamp for set saving
    if (regs.size > 0) {
      last_reg_write = regs.last.get(regsTimestamp).getN().toDouble
    }

    for (x <- regs) {
      val uuid = { val attr = x.get(regsUuid); if (attr != null) attr.getS() else "" }
      val model = { val attr = x.get(regsModel); if (attr != null) attr.getS() else "" }
      val os = { val attr = x.get(regsOs); if (attr != null) attr.getS() else "" }
      val time = { val attr = x.get(regsTimestamp); if (attr != null) attr.getN().toDouble else 0.0 }
      val s = uuidToOsesAndModels.get(uuid).getOrElse(new ArrayBuffer[(Double, String, String)])
      // only record changes in OS.
      if (s.size <= 0 || s.last._2 != os)
        s.add((time, os, model))
      uuidToOsesAndModels += ((uuid, s.sortWith((x, y) => {
        x._1 < y._1
      })))
      models += model
      oses += os
    }
  }

  /**
   * Handles a set of registration messages as an RDD.
   * uuids, oses and models are filled in.
   */
  def handleRegs(regs: RDD[(String, String, String, Double)],
    uuidToOsesAndModels: scala.collection.mutable.HashMap[String, ArrayBuffer[(Double, String, String)]],
    oses: scala.collection.mutable.Set[String],
    models: scala.collection.mutable.Set[String]) {

    regs.foreach(x => {
      val uuid = x._1
      val model = x._2
      val os = x._3
      val time = x._4
      val s = uuidToOsesAndModels.get(uuid).getOrElse(new ArrayBuffer[(Double, String, String)])
      // only record changes in OS.
      if (s.size <= 0 || s.last._2 != os)
        s.add((time, os, model))
      uuidToOsesAndModels += ((uuid, s.sortWith((x, y) => {
        x._1 < y._1
      })))
      models += model
      oses += os
    })
  }

  /**
   * Process a bunch of samples, assumed to be in order by uuid and timestamp.
   * will return an RDD of CaratRates. Samples need not be from the same uuid.
   */
  def handleSamples(sc: SparkContext, samples: java.util.List[java.util.Map[java.lang.String, AttributeValue]],
    uuidToOsesAndModels: scala.collection.mutable.HashMap[String, ArrayBuffer[(Double, String, String)]],
    featureTracking: scala.collection.mutable.HashMap[String, HashMap[String, ArrayBuffer[(Long, Object)]]],
    rates: RDD[CaratRate]) = {

    if (samples.size > 0) {
      val lastSample = samples.last
      last_sample_write = lastSample.get(sampleTime).getN().toDouble
    }

    var rateRdd = sc.parallelize[CaratRate]({
      val mapped = samples.map(sampleMapper)
      DynamoAnalysisUtil.rateMapperPairwise(uuidToOsesAndModels, featureTracking, mapped)
    })
    if (rates != null)
      rateRdd = rateRdd.union(rates)
    rateRdd
  }

  /**
   * Process a bunch of samples, assumed to be in order by uuid and timestamp.
   * will return an RDD of CaratRates. Samples need not be from the same uuid.
   */
  def handleSamples(sc: SparkContext, allSamples: spark.RDD[(String, String, String, String, String, scala.collection.mutable.Buffer[String], scala.collection.immutable.Map[String, (String, java.lang.Object)])],
    uuidToOsesAndModels: scala.collection.mutable.HashMap[String, ArrayBuffer[(Double, String, String)]],
    featureTracking: scala.collection.mutable.HashMap[String, HashMap[String, ArrayBuffer[(Long, Object)]]]) = {

    val rateRdd = DynamoAnalysisUtil.rateMapperPairwise(sc, uuidToOsesAndModels, featureTracking, allSamples)
    rateRdd
  }

  /**
   * Process a bunch of samples, assumed to be in order by uuid and timestamp.
   * will return an RDD of CaratRates. Samples need not be from the same uuid.
   */
  def handleSamplesUnabriged(sc: SparkContext, samples: java.util.List[java.util.Map[java.lang.String, AttributeValue]],
    uuidToOsesAndModels: scala.collection.mutable.HashMap[String, ArrayBuffer[(Double, String, String)]],
    rates: RDD[CaratRate]) = {

    if (samples.size > 0) {
      val lastSample = samples.last
      last_sample_write = lastSample.get(sampleTime).getN().toDouble
    }

    var rateRdd = sc.parallelize[CaratRate]({
      val mapped = samples.map(sampleMapper)
      DynamoAnalysisUtil.rateMapperUnabriged(uuidToOsesAndModels, mapped)
    })
    if (rates != null)
      rateRdd = rateRdd.union(rates)
    rateRdd
  }

  /**
   * Process a bunch of regs, just aggregating them into an RDD.
   * will return an RDD of regs.
   */
  def accumulateRegs(sc: SparkContext, regs: java.util.List[java.util.Map[java.lang.String, AttributeValue]],
    oldRegs: RDD[(String, String, String, Double)]) = {

    // Get last reg timestamp for set saving
    if (regs.size > 0) {
      last_reg_write = regs.last.get(regsTimestamp).getN().toDouble
    }

    var regRdd = sc.parallelize[(String, String, String, Double)]({
      regs.map(x => {
        val uuid = { val attr = x.get(regsUuid); if (attr != null) attr.getS() else "" }
        val model = { val attr = x.get(regsModel); if (attr != null) attr.getS() else "" }
        val os = { val attr = x.get(regsOs); if (attr != null) attr.getS() else "" }
        val time = { val attr = x.get(regsTimestamp); if (attr != null) attr.getN().toDouble else 0.0 }
        (uuid, model, os, time)
      })
    })

    if (oldRegs != null)
      regRdd = regRdd.union(oldRegs)
    regRdd
  }

  /**
   * Process a bunch of samples, just aggregating them into an RDD.
   * will return an RDD of samples.
   */
  def accumulateSamples(sc: SparkContext, samples: java.util.List[java.util.Map[java.lang.String, AttributeValue]],
    oldSamples: RDD[(String, String, String, String, String, Buffer[String], scala.collection.immutable.Map[String, (String, Object)])]) = {

    // Get last reg timestamp for set saving
    if (samples.size > 0) {
      last_reg_write = samples.last.get(sampleTime).getN().toDouble
    }

    var sampleRdd = sc.parallelize[(String, String, String, String, String, Buffer[String], scala.collection.immutable.Map[String, (String, Object)])]({
      samples.map(sampleMapper)
    })

    if (oldSamples != null)
      sampleRdd = sampleRdd.union(oldSamples)
    sampleRdd
  }

  /**
   * Generic Carat DynamoDb loop function. Gets items from a table using keys given, and continues until the table scan is complete.
   * This function achieves a block by block read until the end of a table, regardless of throughput or manual limits.
   */
  def DynamoDbItemLoop[T](tableAndValueToKeyAndResults: => (Key, java.util.List[java.util.Map[String, AttributeValue]]),
    tableAndValueToKeyAndResultsContinue: Key => (Key, java.util.List[java.util.Map[String, AttributeValue]]),
    stepHandler: (java.util.List[java.util.Map[String, AttributeValue]], RDD[T]) => RDD[T], prefix: Boolean, dist: RDD[T]) = {
    val startTime = start
    var finished = false

    var (key, results) = tableAndValueToKeyAndResults
    println("Got: " + results.size + " results.")

    var distRet: RDD[T] = null
    distRet = stepHandler(results, dist)

    while (key != null) {
      println("Continuing from key=" + key)
      var (key2, results2) = tableAndValueToKeyAndResultsContinue(key)
      /* Add last sample here as the starting point for a Rate from it to the next retrieved one */
      if (prefix)
        results2.prepend(results.last)
      results = results2
      key = key2
      println("Got: " + results.size + " results.")

      distRet = stepHandler(results, distRet)
    }
    finish(startTime)
    distRet
  }

  /**
   * Get the contents of the DynamoDb samples and registrations tables as RDDs.
   * @param clean when true, either take all new or all old values, but do not merge.
   * @param sc the SparkContext to use for building the RDDs and saving them to files.
   * @param tmpdir the directory to use for temporary files created by Spark and also for saving the RDDs.
   */
  def getSamples(sc: SparkContext, tmpdir: String, clean: Boolean = true) = {
    val SAMPLES_CACHED_NEW = tmpdir + "cached-samples-new.dat"
    val SAMPLES_CACHED = tmpdir + "cached-samples.dat"
    val REGS_CACHED_NEW = tmpdir + "cached-regs-new.dat"
    val REGS_CACHED = tmpdir + "cached-regs.dat"
    val LAST_SAMPLE = tmpdir + "last-sample.txt"
    val LAST_REG = tmpdir + "last-reg.txt"

    lazy val last_sample = DynamoAnalysisUtil.readDoubleFromFile(LAST_SAMPLE)
    lazy val last_reg = DynamoAnalysisUtil.readDoubleFromFile(LAST_REG)

    val nowS = System.currentTimeMillis() / 1000
    println("Now=%s lastSample=%s diff=%s".format(nowS, last_sample, nowS - last_sample))

    val oldSamples: RDD[(String, String, String, String, String, Buffer[String], scala.collection.immutable.Map[String, (String, Object)])] = {
      val f = new File(SAMPLES_CACHED)
      if (f.exists() && (!clean || (nowS - last_sample < FRESHNESS_SECONDS))) {
        sc.objectFile(SAMPLES_CACHED)
      } else
        null
    }

    val oldRegs: RDD[(String, String, String, Double)] = {
      val f = new File(REGS_CACHED)
      if (f.exists() && (!clean || (nowS - last_sample < FRESHNESS_SECONDS))) {
        sc.objectFile(REGS_CACHED)
      } else
        null
    }

    // Master RDDs for all data.
    var allRegs: RDD[(String, String, String, Double)] = oldRegs
    var allSamples: RDD[(String, String, String, String, String, Buffer[String], scala.collection.immutable.Map[String, (String, Object)])] = oldSamples

    /* Only get new samples if we have no old samples, or it has been more than an hour */

    if (allSamples == null || (nowS - last_sample > FRESHNESS_SECONDS)) {
      if (!clean && last_reg > 0) {
        allRegs = DynamoAnalysisUtil.DynamoDbItemLoop[(String, String, String, Double)](DynamoDbDecoder.filterItemsAfter(registrationTable, regsTimestamp, last_reg + ""),
          DynamoDbDecoder.filterItemsAfter(registrationTable, regsTimestamp, last_reg + "", _),
          accumulateRegs(sc, _, _), false, allRegs)
      } else {
        allRegs = DynamoAnalysisUtil.DynamoDbItemLoop[(String, String, String, Double)](DynamoDbDecoder.getAllItems(registrationTable),
          DynamoDbDecoder.getAllItems(registrationTable, _),
          accumulateRegs(sc, _, _), false, allRegs)
      }

      if (!clean && last_sample > 0) {
        allSamples = DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.filterItemsAfter(samplesTable, sampleTime, last_sample + ""),
          DynamoDbDecoder.filterItemsAfter(samplesTable, sampleTime, last_sample + "", _),
          accumulateSamples(sc, _, _),
          false,
          allSamples)
      } else {
        allSamples = DynamoAnalysisUtil.DynamoDbItemLoop(DynamoDbDecoder.getAllItems(samplesTable),
          DynamoDbDecoder.getAllItems(samplesTable, _),
          accumulateSamples(sc, _, _),
          false,
          allSamples)
      }
    }

    // save entire sample and reg rdd for later:
    allSamples.saveAsObjectFile(SAMPLES_CACHED_NEW)
    DynamoAnalysisUtil.replaceOldRateFile(SAMPLES_CACHED, SAMPLES_CACHED_NEW)
    allRegs.saveAsObjectFile(REGS_CACHED_NEW)
    DynamoAnalysisUtil.replaceOldRateFile(REGS_CACHED, REGS_CACHED_NEW)
    DynamoAnalysisUtil.saveDoubleToFile(last_sample_write, LAST_SAMPLE)
    DynamoAnalysisUtil.saveDoubleToFile(last_reg_write, LAST_REG)

    (allRegs, allSamples)
  }

  /**
   * Get Rates from DynamoDB samples and registrations.
   */

  def samplesToRates(sc:SparkContext, allRegs: spark.RDD[(String, String, String, Double)],
    allSamples: spark.RDD[(String, String, String, String, String, scala.collection.mutable.Buffer[String], scala.collection.immutable.Map[String, (String, java.lang.Object)])]) = {
    // Master RDD for all data.
    var allRates: spark.RDD[CaratRate] = null

    /* TODO: FIXME: featureTracking can only be done for samples, not stored rates.
       * Obvious fix: save samples instead of Rates, then save features and Rates after those. 
      */
    var featureTracking = new scala.collection.mutable.HashMap[String, HashMap[String, ArrayBuffer[(Long, Object)]]]

    /* closure to forget uuids, models and oses after assigning them to rates.
     * This is because new rates may have new uuids, models and oses.
    */
    {
      // Unique uuIds, Oses, and Models from registrations.
      // UUID -> [(timestamp,os), (timestamp,model), ...]
      val uuidToOsesAndModels = new scala.collection.mutable.HashMap[String, ArrayBuffer[(Double, String, String)]]
      // TODO: Make uuids to multiple OSes actually usable
      val allModels = new scala.collection.mutable.HashSet[String]
      val allOses = new scala.collection.mutable.HashSet[String]
      // fill in uuidToOsesAndModels, allOses, and allModels.
      handleRegs(allRegs, uuidToOsesAndModels, allOses, allModels)
      allRates = handleSamples(sc, allSamples, uuidToOsesAndModels, featureTracking)

      // we may not be interested in these actually.
      println("All oses: " + allOses.mkString(", "))
      println("All models: " + allModels.mkString(", "))

      printFeatures(featureTracking, uuidToOsesAndModels)
    }

    allRates
  }

  def sampleMapper(x: java.util.Map[String, AttributeValue]) = {
    /* See properties in package.scala for data keys. */

    val uuid = x.get(sampleKey).getS()
    val apps = x.get(sampleProcesses).getSS().map(w => {
      if (w == null)
        ""
      else {
        val s = w.split(";")
        if (s.size > 1)
          s(1).trim
        else
          ""
      }
    })

    val time = { val attr = x.get(sampleTime); if (attr != null) attr.getN() else "" }
    val batteryState = { val attr = x.get(sampleBatteryState); if (attr != null) attr.getS() else "" }
    val batteryLevel = { val attr = x.get(sampleBatteryLevel); if (attr != null) attr.getN() else "" }
    val event = { val attr = x.get(sampleEvent); if (attr != null) attr.getS() else "" }

    // key -> (type,value) 
    var features: scala.collection.immutable.Map[String, (String, Object)] = new HashMap[String, (String, Object)]
    for (k <- x) {
      val key = k._1
      if (!knownKeys.contains(key))
        features += ((key, DynamoDbEncoder.fromAttributeValue(k._2)))
    }
    (uuid, time, batteryLevel, event, batteryState, apps, features)
  }

  /**
   * New metric: Use EV difference for hog and bug decisions.
   * TODO: Variance and its use in the decision?
   * Multimodal distributions -> EV does not match true energy usage profile?
   * m = 1 - evWith / evWithout
   *
   * m > 0 -> Hog
   * m <= 0 -> not
   */
  def evDiff(evWith: Double, evWithout: Double) = {
    if (evWith == 0 && evWithout == 0)
      0.0
    else
      1.0 - evWithout / evWith
  }

  /**
   * Map samples into CaratRates. `os` and `model` are inserted for easier later processing.
   * Consider sample pairs with non-blc endpoints rates from 0 to prevBatt - batt with uniform probability.
   */
  def rateMapperPairwiseSingleUuid(os: String, model: String, observations: Seq[(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, Seq[String], scala.collection.immutable.Map[String, (String, Object)])]) = {
    // Observations format: (uuid, time, batteryLevel, event, batteryState, apps)
    var prevD = 0.0
    var prevBatt = 0.0
    var prevEvent = ""
    var prevState = ""
    var prevApps: Seq[String] = Array[String]()
    var prevFeatures: scala.collection.immutable.HashMap[String, ArrayBuffer[(String, Object)]] = new HashMap[String, ArrayBuffer[(String, Object)]]

    var d = 0.0
    var batt = 0.0
    var event = ""
    var state = ""
    var apps: Seq[String] = Array[String]()
    var features: scala.collection.immutable.Map[String, (String, Object)] = null

    var negDrainSamples = 0
    var abandonedSamples = 0
    var chargingSamples = 0
    var zeroBLCSamples = 0
    var allZeroSamples = 0
    var pointRates = 0

    var rates = new ArrayBuffer[CaratRate]

    for (k <- observations) {
      d = k._2.toDouble
      batt = k._3.toDouble
      event = k._4.trim().toLowerCase()
      state = k._5.trim().toLowerCase()
      apps = k._6
      features = k._7

      if (state != STATE_CHARGING) {
        /* Record rates. First time fall through */
        if (prevD != 0 && prevD != d) {
          if (prevBatt - batt < 0) {
            printf("prevBatt %s batt %s for d1=%s d2=%s uuid=%s\n", prevBatt, batt, prevD, d, k._1)
            negDrainSamples += 1
          } else if (prevBatt == 0 && batt == 0) {
            /* Assume simulator, ignore */
            printf("prevBatt %s batt %s for d1=%s d2=%s uuid=%s\n", prevBatt, batt, prevD, d, k._1)
            allZeroSamples += 1
          } else {
            /* now prevBatt - batt >= 0 */
            if (prevEvent == TRIGGER_BATTERYLEVELCHANGED && event == TRIGGER_BATTERYLEVELCHANGED) {
              /* Point rate */
              for (k <- features) {
                val v = prevFeatures.getOrElse(k._1, new ArrayBuffer[(String, Object)])
                v += k._2
                prevFeatures += ((k._1, v))
              }
              println("Extra features:")
              for (k <- prevFeatures) {
                println(k._1, k._2.mkString("; "))
              }
              val r = new CaratRate(k._1, os, model, prevD, d, prevBatt, batt,
                prevEvent, event, prevApps, apps, prevFeatures.map(x => { (x._1, x._2.toSeq) }))
              if (r.rate() == 0) {
                // This should never happen
                println("RATE ERROR: BatteryLevelChanged with zero rate: " + r.toString(false))
                zeroBLCSamples += 1
              } else {
                if (considerRate(r)) {
                  rates += r
                  pointRates += 1
                } else {
                  abandonedSamples += 1
                }
              }
            } else {
              /* One endpoint not BLC, use uniform distribution rate */
              for (k <- features) {
                val v = prevFeatures.getOrElse(k._1, new ArrayBuffer[(String, Object)])
                v += k._2
                prevFeatures += ((k._1, v))
              }
              val r = new CaratRate(k._1, os, model, prevD, d, prevBatt, batt, new UniformDist(prevBatt, batt, prevD, d),
                prevEvent, event, prevApps, apps, prevFeatures.map(x => { (x._1, x._2.toSeq) }))
              if (considerRate(r)) {
                rates += r
              } else {
                println("Abandoned uniform rate with abnormally high EV: " + r.toString(false))
                abandonedSamples += 1
              }
            }
          }
        }
      } else {
        chargingSamples += 1
      }
      prevD = d
      prevBatt = batt
      prevEvent = event
      prevState = state
      prevApps = apps
      prevFeatures = new HashMap[String, ArrayBuffer[(String, Object)]]
      for (k <- features) {
        val v = prevFeatures.getOrElse(k._1, new ArrayBuffer[(String, Object)])
        v += k._2
        prevFeatures += ((k._1, v))
      }
    }

    println(nzf("Recorded %s point rates ", pointRates) + "abandoned " +
      nzf("%s all zero, ", allZeroSamples) +
      nzf("%s charging, ", chargingSamples) +
      nzf("%s negative drain, ", negDrainSamples) +
      nzf("%s > " + ABNORMAL_RATE + " drain, ", abandonedSamples) +
      nzf("%s zero drain BLC", zeroBLCSamples) + " samples.")
    rates.toSeq
  }

  /**
   * Map samples into CaratRates. `os` and `model` are inserted for easier later processing.
   * Consider sample pairs with non-blc endpoints rates from 0 to prevBatt - batt with uniform probability.
   */
  def rateMapperPairwise(uuidToOsesAndModels: scala.collection.mutable.HashMap[String, ArrayBuffer[(Double, String, String)]],
    featureTracking: scala.collection.mutable.HashMap[String, HashMap[String, ArrayBuffer[(Long, Object)]]],
    observations: Seq[(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, Seq[String], scala.collection.immutable.Map[String, (String, Object)])]) = {
    // Observations format: (uuid, time, batteryLevel, event, batteryState, apps)
    val startTime = start
    var prevUuid = ""
    var prevD = 0.0
    var prevBatt = 0.0
    var prevEvent = ""
    var prevState = ""
    var prevApps: Seq[String] = Array[String]()
    var prevFeatures: scala.collection.immutable.HashMap[String, ArrayBuffer[(String, Object)]] = new HashMap[String, ArrayBuffer[(String, Object)]]

    // Store a map of feature name to its value history for each uuid in featureTracking.

    var uuid = ""
    var d = 0.0
    var batt = 0.0
    var event = ""
    var state = ""
    var apps: Seq[String] = Array[String]()
    var features: scala.collection.immutable.Map[String, (String, Object)] = null

    var negDrainSamples = 0
    var abandonedSamples = 0
    var chargingSamples = 0
    var zeroBLCSamples = 0
    var allZeroSamples = 0
    var pointRates = 0

    var rates = new ArrayBuffer[CaratRate]
    val obsSorted = observations.sortWith((x, y) => {
      // order lexicographically by uuid
      if (x._1 < y._1)
        true
      else if (x._1 > y._1)
        false
      else if (x._1 == y._1 && x._2 < y._2)
        true
      else if (x._1 == y._1 && x._2 > y._2)
        false
      else
        true
      // if time and uuid are the same, sort first parameter first
    })
    for (k <- obsSorted) {
      uuid = k._1
      d = k._2.toDouble
      val list = uuidToOsesAndModels.get(k._1).getOrElse(new ArrayBuffer[(Double, String, String)])

      var os = ""
      var model = ""
      var last = list.head
      // if all registrations are somehow later than the measurement:
      if (last._1 > d) {
        os = last._2
        model = last._3
      } else {
        // some registrations are before the measurement
        for (k <- list) {
          if (last._1 < d && k._1 <= d) {
            os = last._2
            model = last._3
          }
          last = k
        }
        if (os == "") {
          /* no interval found for d.
           * Probably all regs are before the measurement.
           * In this case, take the last os/model.
           */
          os = last._2
          model = last._3
        }
      }
      // Now we have the right os and model.

      batt = k._3.toDouble
      event = k._4.trim().toLowerCase()
      state = k._5.trim().toLowerCase()
      apps = k._6
      features = k._7

      /* Do this early not to censor any values out of the history.
       */

      var old = featureTracking.get(uuid).getOrElse(new HashMap[String, ArrayBuffer[(Long, Object)]])

      var bl = old.getOrElse("BatteryLevel", new ArrayBuffer[(Long, Object)])
      if (bl.length <= 0 || bl.last._2 != batt) {
        bl += ((d.toLong, new java.lang.Double(batt)))
        old += (("BatteryLevel", bl))
      }
      val bs = old.getOrElse("BatteryState", new ArrayBuffer[(Long, Object)])
      if (bs.length <= 0 || bs.last._2 != state) {
        bs += ((d.toLong, state))
        old += (("BatteryState", bs))
      }
      val ev = old.getOrElse("Event", new ArrayBuffer[(Long, Object)])
      if (ev.length <= 0 || ev.last._2 != event) {
        ev += ((d.toLong, event))
        old += (("Event", ev))
      }
      val om = old.getOrElse("Model", new ArrayBuffer[(Long, Object)])
      if (om.length <= 0 || om.last._2 != model) {
        om += ((d.toLong, model))
        old += (("Model", om))
      }
      val od = old.getOrElse("OS", new ArrayBuffer[(Long, Object)])
      if (od.length <= 0 || od.last._2 != os) {
        od += ((d.toLong, os))
        old += (("OS", od))
      }
      val oldApps = old.getOrElse("Apps", new ArrayBuffer[(Long, Object)])
      if (oldApps.length <= 0 || oldApps.last._2 != apps) {
        oldApps += ((d.toLong, apps.sorted))
        old += (("Apps", oldApps))
      }
      /* Handle extra features: */
      for ((feature, (fType, value)) <- features) {
        val oldFeature = old.getOrElse(feature, new ArrayBuffer[(Long, Object)])
        if (oldFeature.length <= 0 || oldFeature.last._2 != value) {
          oldFeature += ((d.toLong, value))
          old += ((feature, oldFeature))
        }
      }

      // Fixme: does == comparison of Scala Arrays or sets work properly? (Features, Apps)
      featureTracking.put(uuid, old)

      if (model != MODEL_SIMULATOR) {
        if (state != STATE_CHARGING && state != STATE_UNKNOWN) {
          /* Record rates. First time fall through.
           * Note: same date or different uuid does not result
           * in discard of the sample as a starting point for a rate.
           * However, we cannot have a rate across UUIDs or the same timestamp.
           */
          if (prevD != 0 && prevD != d && prevUuid == uuid) {
            if (prevBatt - batt < 0) {
              printf("prevBatt %s batt %s for d1=%s d2=%s uuid=%s\n", prevBatt, batt, prevD, d, k._1)
              negDrainSamples += 1
            } else if (prevBatt == 0 && batt == 0) {
              /* Assume simulator, ignore */
              printf("prevBatt %s batt %s for d1=%s d2=%s uuid=%s\n", prevBatt, batt, prevD, d, k._1)
              allZeroSamples += 1
            } else {

              /* now prevBatt - batt >= 0 */
              if (prevEvent == TRIGGER_BATTERYLEVELCHANGED && event == TRIGGER_BATTERYLEVELCHANGED) {
                /* Point rate */
                for (k <- features) {
                  val v = prevFeatures.getOrElse(k._1, new ArrayBuffer[(String, Object)])
                  v += k._2
                  prevFeatures += ((k._1, v))
                }
                println("Extra features:")
                for (k <- prevFeatures) {
                  println(k._1, k._2.mkString("; "))
                }
                val r = new CaratRate(k._1, os, model, prevD, d, prevBatt, batt,
                  prevEvent, event, prevApps, apps, prevFeatures.map(x => { (x._1, x._2.toSeq) }))
                if (r.rate() == 0) {
                  // This should never happen
                  println("RATE ERROR: BatteryLevelChanged with zero rate: " + r.toString(false))
                  zeroBLCSamples += 1
                } else {
                  if (considerRate(r)) {
                    rates += r
                    pointRates += 1
                  } else {
                    abandonedSamples += 1
                  }
                }
              } else {
                /* One endpoint not BLC, use uniform distribution rate */
                println("Extra features:")
                for (k <- prevFeatures) {
                  println(k._1, k._2.mkString("; "))
                }
                for (k <- features) {
                  val v = prevFeatures.getOrElse(k._1, new ArrayBuffer[(String, Object)])
                  v += k._2
                  prevFeatures += ((k._1, v))
                }
                val r = new CaratRate(k._1, os, model, prevD, d, prevBatt, batt, new UniformDist(prevBatt, batt, prevD, d),
                  prevEvent, event, prevApps, apps, prevFeatures.map(x => { (x._1, x._2.toSeq) }))
                if (considerRate(r)) {
                  rates += r
                } else {
                  println("Abandoned uniform rate with abnormally high EV: " + r.toString(false))
                  abandonedSamples += 1
                }
              }
            }
          }
        } else {
          chargingSamples += 1
          // do not use charging samples as even starting points.
          prevD = 0
        }
      } else {
        // simulator samples also reset prevD
        prevD = 0
      }
      prevUuid = uuid
      prevD = d
      prevBatt = batt
      prevEvent = event
      prevState = state
      prevApps = apps
      prevFeatures = new HashMap[String, ArrayBuffer[(String, Object)]]
      for (k <- features) {
        val v = prevFeatures.getOrElse(k._1, new ArrayBuffer[(String, Object)])
        v += k._2
        prevFeatures += ((k._1, v))
      }
    }

    println(nzf("Recorded %s point rates ", pointRates) + "abandoned " +
      nzf("%s all zero, ", allZeroSamples) +
      nzf("%s charging, ", chargingSamples) +
      nzf("%s negative drain, ", negDrainSamples) +
      nzf("%s > " + ABNORMAL_RATE + " drain, ", abandonedSamples) +
      nzf("%s zero drain BLC", zeroBLCSamples) + " samples.")
    finish(startTime)
    rates.toSeq
  }

  /**
   * Map samples into CaratRates. `os` and `model` are inserted for easier later processing.
   * Consider sample pairs with non-blc endpoints rates from 0 to prevBatt - batt with uniform probability.
   */
  def rateMapperPairwise(sc: SparkContext, uuidToOsesAndModels: scala.collection.mutable.HashMap[String, ArrayBuffer[(Double, String, String)]],
    featureTracking: scala.collection.mutable.HashMap[String, HashMap[String, ArrayBuffer[(Long, Object)]]],
    observations: RDD[(String, String, String, String, String, scala.collection.mutable.Buffer[String], scala.collection.immutable.Map[String, (String, java.lang.Object)])]) = {
    // Observations format: (uuid, time, batteryLevel, event, batteryState, apps, other features)
    val startTime = start
    var prevUuid = ""
    var prevD = 0.0
    var prevBatt = 0.0
    var prevEvent = ""
    var prevState = ""
    var prevApps: Seq[String] = Array[String]()
    var prevFeatures: scala.collection.immutable.HashMap[String, ArrayBuffer[(String, Object)]] = new HashMap[String, ArrayBuffer[(String, Object)]]

    // Store a map of feature name to its value history for each uuid in featureTracking.

    var uuid = ""
    var d = 0.0
    var batt = 0.0
    var event = ""
    var state = ""
    var apps: Seq[String] = Array[String]()
    var features: scala.collection.immutable.Map[String, (String, Object)] = null

    var negDrainSamples = 0
    var abandonedSamples = 0
    var chargingSamples = 0
    var zeroBLCSamples = 0
    var allZeroSamples = 0
    var pointRates = 0

    var rates: RDD[CaratRate] = null
    var rateArray = new ArrayBuffer[CaratRate]
    val obsKeyed = observations.map(x => {
      val k = new SampleKey(x._1, x._2.toDouble)
      val v = new Sample(x._3, x._4, x._5, x._6, x._7)
      (k, v)
    })

    // sort ascending by uuid, time
    // Requires Spark git from March 2012
    val obsSorted = obsKeyed.sortByKey(true)

    for ((key, value) <- obsSorted) {
      uuid = key.uuid
      d = key.time
      val list = uuidToOsesAndModels.get(uuid).getOrElse(new ArrayBuffer[(Double, String, String)])

      var os = ""
      var model = ""
      var last = list.head
      // if all registrations are somehow later than the measurement:
      if (last._1 > d) {
        os = last._2
        model = last._3
      } else {
        // some registrations are before the measurement
        for (k <- list) {
          if (last._1 < d && k._1 <= d) {
            os = last._2
            model = last._3
          }
          last = k
        }
        if (os == "") {
          /* no interval found for d.
           * Probably all regs are before the measurement.
           * In this case, take the last os/model.
           */
          os = last._2
          model = last._3
        }
      }
      // Now we have the right os and model.

      batt = value.battery.toDouble
      event = value.event.trim().toLowerCase()
      state = value.state.trim().toLowerCase()
      apps = value.apps
      features = value.features

      /* Do this early not to censor any values out of the history.
       */

      var old = featureTracking.get(uuid).getOrElse(new HashMap[String, ArrayBuffer[(Long, Object)]])

      var bl = old.getOrElse("BatteryLevel", new ArrayBuffer[(Long, Object)])
      if (bl.length <= 0 || bl.last._2 != batt) {
        bl += ((d.toLong, new java.lang.Double(batt)))
        old += (("BatteryLevel", bl))
      }
      val bs = old.getOrElse("BatteryState", new ArrayBuffer[(Long, Object)])
      if (bs.length <= 0 || bs.last._2 != state) {
        bs += ((d.toLong, state))
        old += (("BatteryState", bs))
      }
      val ev = old.getOrElse("Event", new ArrayBuffer[(Long, Object)])
      if (ev.length <= 0 || ev.last._2 != event) {
        ev += ((d.toLong, event))
        old += (("Event", ev))
      }
      val om = old.getOrElse("Model", new ArrayBuffer[(Long, Object)])
      if (om.length <= 0 || om.last._2 != model) {
        om += ((d.toLong, model))
        old += (("Model", om))
      }
      val od = old.getOrElse("OS", new ArrayBuffer[(Long, Object)])
      if (od.length <= 0 || od.last._2 != os) {
        od += ((d.toLong, os))
        old += (("OS", od))
      }
      val oldApps = old.getOrElse("Apps", new ArrayBuffer[(Long, Object)])
      if (oldApps.length <= 0 || oldApps.last._2 != apps) {
        oldApps += ((d.toLong, apps.sorted))
        old += (("Apps", oldApps))
      }
      /* Handle extra features: */
      for ((feature, (fType, value)) <- features) {
        val oldFeature = old.getOrElse(feature, new ArrayBuffer[(Long, Object)])
        if (oldFeature.length <= 0 || oldFeature.last._2 != value) {
          oldFeature += ((d.toLong, value))
          old += ((feature, oldFeature))
        }
      }

      // Fixme: does == comparison of Scala Arrays or sets work properly? (Features, Apps)
      featureTracking.put(uuid, old)

      if (model != MODEL_SIMULATOR) {
        if (state != STATE_CHARGING && state != STATE_UNKNOWN) {
          /* Record rates. First time fall through.
           * Note: same date or different uuid does not result
           * in discard of the sample as a starting point for a rate.
           * However, we cannot have a rate across UUIDs or the same timestamp.
           */
          if (prevD != 0 && prevD != d && prevUuid == uuid) {
            if (prevBatt - batt < 0) {
              printf("prevBatt %s batt %s for d1=%s d2=%s uuid=%s\n", prevBatt, batt, prevD, d, uuid)
              negDrainSamples += 1
            } else if (prevBatt == 0 && batt == 0) {
              /* Assume simulator, ignore */
              printf("prevBatt %s batt %s for d1=%s d2=%s uuid=%s\n", prevBatt, batt, prevD, d, uuid)
              allZeroSamples += 1
            } else {

              /* now prevBatt - batt >= 0 */
              if (prevEvent == TRIGGER_BATTERYLEVELCHANGED && event == TRIGGER_BATTERYLEVELCHANGED) {
                /* Point rate */
                for (k <- features) {
                  val v = prevFeatures.getOrElse(k._1, new ArrayBuffer[(String, Object)])
                  v += k._2
                  prevFeatures += ((k._1, v))
                }
                println("Extra features:")
                for (k <- prevFeatures) {
                  println(k._1, k._2.mkString("; "))
                }
                val r = new CaratRate(uuid, os, model, prevD, d, prevBatt, batt,
                  prevEvent, event, prevApps, apps, prevFeatures.map(x => { (x._1, x._2.toSeq) }))
                if (r.rate() == 0) {
                  // This should never happen
                  println("RATE ERROR: BatteryLevelChanged with zero rate: " + r.toString(false))
                  zeroBLCSamples += 1
                } else {
                  if (considerRate(r)) {
                    rateArray += r
                    if (rateArray.size == RATE_BUFFER_SIZE) {
                      if (rates == null) {
                        rates = sc.parallelize[CaratRate](rateArray)
                      } else {
                        rates ++= sc.parallelize[CaratRate](rateArray)
                      }
                      rateArray = new ArrayBuffer[CaratRate]
                    }

                    pointRates += 1
                  } else {
                    abandonedSamples += 1
                  }
                }
              } else {
                /* One endpoint not BLC, use uniform distribution rate */
                println("Extra features:")
                for (k <- prevFeatures) {
                  println(k._1, k._2.mkString("; "))
                }
                for (k <- features) {
                  val v = prevFeatures.getOrElse(k._1, new ArrayBuffer[(String, Object)])
                  v += k._2
                  prevFeatures += ((k._1, v))
                }
                val r = new CaratRate(uuid, os, model, prevD, d, prevBatt, batt, new UniformDist(prevBatt, batt, prevD, d),
                  prevEvent, event, prevApps, apps, prevFeatures.map(x => { (x._1, x._2.toSeq) }))
                if (considerRate(r)) {
                  rateArray += r
                  if (rateArray.size == RATE_BUFFER_SIZE) {
                    if (rates == null) {
                      rates = sc.parallelize[CaratRate](rateArray)
                    } else {
                      rates ++= sc.parallelize[CaratRate](rateArray)
                    }
                    rateArray = new ArrayBuffer[CaratRate]
                  }
                } else {
                  println("Abandoned uniform rate with abnormally high EV: " + r.toString(false))
                  abandonedSamples += 1
                }
              }
            }
          }
        } else {
          chargingSamples += 1
          // do not use charging samples as even starting points.
          prevD = 0
        }
      } else {
        // simulator samples also reset prevD
        prevD = 0
      }
      prevUuid = uuid
      prevD = d
      prevBatt = batt
      prevEvent = event
      prevState = state
      prevApps = apps
      prevFeatures = new HashMap[String, ArrayBuffer[(String, Object)]]
      for (k <- features) {
        val v = prevFeatures.getOrElse(k._1, new ArrayBuffer[(String, Object)])
        v += k._2
        prevFeatures += ((k._1, v))
      }
    }

    println(nzf("Recorded %s point rates ", pointRates) + "abandoned " +
      nzf("%s all zero, ", allZeroSamples) +
      nzf("%s charging, ", chargingSamples) +
      nzf("%s negative drain, ", negDrainSamples) +
      nzf("%s > " + ABNORMAL_RATE + " drain, ", abandonedSamples) +
      nzf("%s zero drain BLC", zeroBLCSamples) + " samples.")
    finish(startTime)

    if (rateArray.size > 0) {
      if (rates == null) {
        rates = sc.parallelize[CaratRate](rateArray)
      } else {
        rates ++= sc.parallelize[CaratRate](rateArray)
      }
    }
    rates
  }

  def printFeatures(featureTracking: scala.collection.mutable.HashMap[String, HashMap[String, ArrayBuffer[(Long, Object)]]],
    uuidToOsesAndModels: scala.collection.mutable.HashMap[String, ArrayBuffer[(Double, String, String)]]) {
    var changePairs = new HashMap[String, HashSet[(Object, Object)]]
    // retrieve change pairs
    for ((uuid, featureMap) <- featureTracking) {
      for ((feature, history) <- featureMap) {
        println("Feature history for %s and %s:".format(uuid, feature))
        var prev: Object = null
        for ((time, thing) <- history) {
          if (prev != null) {
            var pairSet = changePairs.getOrElse(feature, new HashSet[(Object, Object)])
            pairSet += ((prev, thing))
            changePairs += ((feature, pairSet))
          }
          // Convert time to milliseconds and display using current locale
          println("%s %s".format(new Date(time * 1000), thing))
          prev = thing
        }
      }
    }

    for ((feature, pairSet) <- changePairs)
      for (pair <- pairSet)
        println("%s change: %s to %s".format(feature, pair._1, pair._2))

    // group by model:
    val modelFeatures = featureTracking.map(x => {
      val list = uuidToOsesAndModels.getOrElse(x._1, new ArrayBuffer[(Double, String, String)])
      var model = ""
      if (!list.isEmpty)
        model = list.last._3
      val lastFeatures = x._2.map(y => {
        (y._1, y._2.last)
      })
      (x._1 + " " + model, lastFeatures)
    })
    for ((model, features) <- modelFeatures) {
      println("Last features of %s".format(model))
      for ((feature, value) <- features)
        println("%s %s %s %s".format(model, feature, new Date(value._1 * 1000), value._2))
    }
  }

  /**
   * Map samples into CaratRates. `os` and `model` are inserted for easier later processing.
   * Consider sample pairs with non-blc endpoints rates from 0 to prevBatt - batt with uniform probability.
   */
  def rateMapperUnabriged(uuidToOsesAndModels: scala.collection.mutable.HashMap[String, ArrayBuffer[(Double, String, String)]],
    observations: Seq[(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, Seq[String], scala.collection.immutable.Map[String, (String, Object)])]) = {
    // Observations format: (uuid, time, batteryLevel, event, batteryState, apps)
    val startTime = start
    var prevUuid = ""
    var prevD = 0.0
    var prevBatt = 0.0
    var prevEvent = ""
    var prevState = ""
    var prevApps: Seq[String] = Array[String]()
    var prevFeatures: scala.collection.immutable.HashMap[String, ArrayBuffer[(String, Object)]] = new HashMap[String, ArrayBuffer[(String, Object)]]

    var uuid = ""
    var d = 0.0
    var batt = 0.0
    var event = ""
    var state = ""
    var apps: Seq[String] = Array[String]()
    var features: scala.collection.immutable.Map[String, (String, Object)] = null

    var negDrainSamples = 0
    var abandonedSamples = 0
    var chargingSamples = 0
    var zeroBLCSamples = 0
    var allZeroSamples = 0
    var pointRates = 0

    var rates = new ArrayBuffer[CaratRate]
    val obsSorted = observations.sortWith((x, y) => {
      // order lexicographically by uuid
      if (x._1 < y._1)
        true
      else if (x._1 > y._1)
        false
      else if (x._1 == y._1 && x._2 < y._2)
        true
      else if (x._1 == y._1 && x._2 > y._2)
        false
      else
        true
      // if time and uuid are the same, sort first parameter first
    })
    for (k <- obsSorted) {
      uuid = k._1
      d = k._2.toDouble
      val list = uuidToOsesAndModels.get(k._1).getOrElse(new ArrayBuffer[(Double, String, String)])

      var os = ""
      var model = ""
      var last = list.head
      // if all registrations are somehow later than the measurement:
      if (last._1 > d) {
        os = last._2
        model = last._3
      } else {
        // some registrations are before the measurement
        for (k <- list) {
          if (last._1 < d && k._1 <= d) {
            os = last._2
            model = last._3
          }
          last = k
        }
        if (os == "") {
          /* no interval found for d.
           * Probably all regs are before the measurement.
           * In this case, take the last os/model.
           */
          os = last._2
          model = last._3
        }
      }
      // Now we have the right os and model.

      batt = k._3.toDouble
      event = k._4.trim().toLowerCase()
      state = k._5.trim().toLowerCase()
      apps = k._6
      features = k._7

      if (model != MODEL_SIMULATOR) {
        if (state != STATE_CHARGING) {
          /* Record rates. First time fall through.
           * Note: same date or different uuid does not result
           * in discard of the sample as a starting point for a rate.
           * However, we cannot have a rate across UUIDs or the same timestamp.
           */
          if (prevD != 0 && prevD != d && prevUuid == uuid) {
            if (prevBatt - batt < 0) {
              printf("prevBatt %s batt %s for d1=%s d2=%s uuid=%s\n", prevBatt, batt, prevD, d, k._1)
              negDrainSamples += 1
            } else if (prevBatt == 0 && batt == 0) {
              /* Assume simulator, ignore */
              printf("prevBatt %s batt %s for d1=%s d2=%s uuid=%s\n", prevBatt, batt, prevD, d, k._1)
              allZeroSamples += 1
            } else {

              /* now prevBatt - batt >= 0 */
              if (prevEvent == TRIGGER_BATTERYLEVELCHANGED && event == TRIGGER_BATTERYLEVELCHANGED) {
                /* Point rate */
                for (k <- features) {
                  val v = prevFeatures.getOrElse(k._1, new ArrayBuffer[(String, Object)])
                  v += k._2
                  prevFeatures += ((k._1, v))
                }
                println("Extra features:")
                for (k <- prevFeatures) {
                  println(k._1, k._2.mkString("; "))
                }
                val r = new CaratRate(k._1, os, model, prevD, d, prevBatt, batt,
                  prevEvent, event, prevApps, apps, prevFeatures.map(x => { (x._1, x._2.toSeq) }))
                if (r.rate() == 0) {
                  // This should never happen
                  println("RATE ERROR: BatteryLevelChanged with zero rate: " + r.toString(false))
                  zeroBLCSamples += 1
                } else {
                  rates += r
                  pointRates += 1
                }
              } else {
                /* One endpoint not BLC, use uniform distribution rate */
                println("Extra features:")
                for (k <- prevFeatures) {
                  println(k._1, k._2.mkString("; "))
                }
                for (k <- features) {
                  val v = prevFeatures.getOrElse(k._1, new ArrayBuffer[(String, Object)])
                  v += k._2
                  prevFeatures += ((k._1, v))
                }
                val r = new CaratRate(k._1, os, model, prevD, d, prevBatt, batt, new UniformDist(prevBatt, batt, prevD, d),
                  prevEvent, event, prevApps, apps, prevFeatures.map(x => { (x._1, x._2.toSeq) }))
                rates += r
              }
            }
          }
        } else {
          chargingSamples += 1
          // do not use charging samples as even starting points.
          prevD = 0
        }
      } else {
        // simulator samples also reset prevD
        prevD = 0
      }
      prevUuid = uuid
      prevD = d
      prevBatt = batt
      prevEvent = event
      prevState = state
      prevApps = apps
      prevFeatures = new HashMap[String, ArrayBuffer[(String, Object)]]
      for (k <- features) {
        val v = prevFeatures.getOrElse(k._1, new ArrayBuffer[(String, Object)])
        v += k._2
        prevFeatures += ((k._1, v))
      }
    }

    println(nzf("Recorded %s point rates ", pointRates) + "abandoned " +
      nzf("%s all zero, ", allZeroSamples) +
      nzf("%s charging, ", chargingSamples) +
      nzf("%s negative drain, ", negDrainSamples) +
      nzf("%s > " + ABNORMAL_RATE + " drain, ", abandonedSamples) +
      nzf("%s zero drain BLC", zeroBLCSamples) + " samples.")
    finish(startTime)
    rates.toSeq
  }

  def nzf(formatString: String, number: Int) = {
    if (number > 0)
      formatString.format(number)
    else
      ""
  }

  def considerRate(r: CaratRate) = {
    if (r.isRateRange()) {
      true
    } else {
      if (r.rate() > ABNORMAL_RATE) {
        printf("Abandoning abnormally high rate " + r.toString(false))
        false
      } else
        true
    }
  }

  def correlation(name: String, rates: Array[CaratRate],
    aPriori: Map[Double, Double],
    models: Set[String], oses: Set[String],
    totalsByUuid: TreeMap[String, (Double, Double)]) = {
    var modelCorrelations = new scala.collection.immutable.HashMap[String, Double]
    var osCorrelations = new scala.collection.immutable.HashMap[String, Double]
    var userCorrelations = new scala.collection.immutable.HashMap[String, Double]

    val rateEvs = ProbUtil.normalize(DynamoAnalysisUtil.mapToRateEv(aPriori, rates).toMap)
    if (rateEvs != null) {
      for (model <- models) {
        /* correlation with this model */
        val rateModels = rates.map(x => {
          if (x.model == model)
            (x, 1.0)
          else
            (x, 0.0)
        }).toMap
        val norm = ProbUtil.normalize(rateModels)
        if (norm != null) {
          val corr = rateEvs.map(x => {
            x._2 * norm.getOrElse(x._1, 0.0)
          }).sum
          modelCorrelations += ((model, corr))
        } else
          println("ERROR: zero stddev for %s: %s".format(model, rateModels.map(x => { (x._1.model, x._2) })))
      }

      for (os <- oses) {
        /* correlation with this OS */
        val rateOses = rates.map(x => {
          if (x.os == os)
            (x, 1.0)
          else
            (x, 0.0)
        }).toMap
        val norm = ProbUtil.normalize(rateOses)
        if (norm != null) {
          val corr = rateEvs.map(x => {
            x._2 * norm.getOrElse(x._1, 0.0)
          }).sum
          osCorrelations += ((os, corr))
        } else
          println("ERROR: zero stddev for %s: %s".format(os, rateOses.map(x => { (x._1.os, x._2) })))
      }

      {
        val rateTotals = rates.map(x => {
          (x, totalsByUuid.getOrElse(x.uuid, (0.0, 0.0)))
        }).toMap
        val normSamples = ProbUtil.normalize(rateTotals.map(x => { (x._1, x._2._1) }))
        if (normSamples != null) {
          val corr = rateEvs.map(x => {
            x._2 * normSamples.getOrElse(x._1, 0.0)
          }).sum
          userCorrelations += (("Samples", corr))
        } else
          println("ERROR: zero stddev for %s: %s".format("Samples", rateTotals.map(x => { (x._1.uuid, x._2) })))
      }

      {
        val rateTotals = rates.map(x => {
          (x, totalsByUuid.getOrElse(x.uuid, (0.0, 0.0)))
        }).toMap
        val normApps = ProbUtil.normalize(rateTotals.map(x => { (x._1, x._2._2) }))
        if (normApps != null) {
          val corr = rateEvs.map(x => {
            x._2 * normApps.getOrElse(x._1, 0.0)
          }).sum
          userCorrelations += (("Apps", corr))
        } else
          println("ERROR: zero stddev for %s: %s".format("Apps", rateTotals.map(x => { (x._1.uuid, x._2) })))
      }

      for (k <- modelCorrelations)
        println("%s and %s correlated with %s".format(name, k._1, k._2))
      for (k <- osCorrelations)
        println("%s and %s correlated with %s".format(name, k._1, k._2))
      for (k <- userCorrelations)
        println("%s and %s correlated with %s".format(name, k._1, k._2))
    } else
      println("ERROR: Rates had a zero stddev, something is wrong!")

    (osCorrelations, modelCorrelations, userCorrelations)
  }
  /**
   * RDD Version of Correlation.
   *
   */
  def correlation(name: String, rates: RDD[CaratRate],
    aPriori: Map[Double, Double],
    models: Set[String], oses: Set[String],
    totalsByUuid: TreeMap[String, (Double, Double)]) = {
    var modelCorrelations = new scala.collection.immutable.HashMap[String, Double]
    var osCorrelations = new scala.collection.immutable.HashMap[String, Double]
    var userCorrelations = new scala.collection.immutable.HashMap[String, Double]

    val rateEvs = ProbUtil.normalize(DynamoAnalysisUtil.mapToRateEv(aPriori, rates).collectAsMap())
    if (rateEvs != null) {
      for (model <- models) {
        /* correlation with this model */
        val rateModels = rates.map(x => {
          if (x.model == model)
            (x, 1.0)
          else
            (x, 0.0)
        }).collectAsMap()
        val norm = ProbUtil.normalize(rateModels)
        if (norm != null) {
          val corr = rateEvs.map(x => {
            x._2 * norm.getOrElse(x._1, 0.0)
          }).sum
          modelCorrelations += ((model, corr))
        } else
          println("ERROR: zero stddev for %s: %s".format(model, rateModels.map(x => { (x._1.model, x._2) })))
      }

      for (os <- oses) {
        /* correlation with this OS */
        val rateOses = rates.map(x => {
          if (x.os == os)
            (x, 1.0)
          else
            (x, 0.0)
        }).collectAsMap()
        val norm = ProbUtil.normalize(rateOses)
        if (norm != null) {
          val corr = rateEvs.map(x => {
            x._2 * norm.getOrElse(x._1, 0.0)
          }).sum
          osCorrelations += ((os, corr))
        } else
          println("ERROR: zero stddev for %s: %s".format(os, rateOses.map(x => { (x._1.os, x._2) })))
      }

      {
        val rateTotals = rates.map(x => {
          (x, totalsByUuid.getOrElse(x.uuid, (0.0, 0.0)))
        }).collectAsMap()
        val normSamples = ProbUtil.normalize(rateTotals.map(x => { (x._1, x._2._1) }))
        if (normSamples != null) {
          val corr = rateEvs.map(x => {
            x._2 * normSamples.getOrElse(x._1, 0.0)
          }).sum
          userCorrelations += (("Samples", corr))
        } else
          println("ERROR: zero stddev for %s: %s".format("Samples", rateTotals.map(x => { (x._1.uuid, x._2) })))
      }

      {
        val rateTotals = rates.map(x => {
          (x, totalsByUuid.getOrElse(x.uuid, (0.0, 0.0)))
        }).collectAsMap()
        val normApps = ProbUtil.normalize(rateTotals.map(x => { (x._1, x._2._2) }))
        if (normApps != null) {
          val corr = rateEvs.map(x => {
            x._2 * normApps.getOrElse(x._1, 0.0)
          }).sum
          userCorrelations += (("Apps", corr))
        } else
          println("ERROR: zero stddev for %s: %s".format("Apps", rateTotals.map(x => { (x._1.uuid, x._2) })))
      }

      for (k <- modelCorrelations)
        println("%s and %s correlated with %s".format(name, k._1, k._2))
      for (k <- osCorrelations)
        println("%s and %s correlated with %s".format(name, k._1, k._2))
      for (k <- userCorrelations)
        println("%s and %s correlated with %s".format(name, k._1, k._2))
    } else
      println("ERROR: Rates had a zero stddev, something is wrong!")

    (osCorrelations, modelCorrelations, userCorrelations)
  }

  /**
   * FIXME: Filters inside a map of another RDD are not allowed, so we call collect on the returned a priori distribution here.
   * If this becomes a memory problem, averaging in the a priori dist should be done.
   */
  def getApriori(allRates: RDD[CaratRate]) = {
    val startTime = start
    // get BLCs
    assert(allRates != null, "AllRates should not be null when calculating aPriori.")
    val ap = allRates.filter(x => {
      !x.isRateRange()
    })
    assert(ap.count > 0, "AllRates should contain some rates that are not rateRanges and less than %s when calculating aPriori.".format(ABNORMAL_RATE))
    // Get their rates and frequencies (1.0 for all) and group by rate 
    val grouped = ap.map(x => {
      ((x.rate, 1.0))
    }).groupByKey()
    // turn arrays of 1.0s to frequencies
    println("Collecting aPriori.")
    val ret = grouped.map(x => { (x._1, x._2.sum) }).collectAsMap()
    finish(startTime)
    ret
  }

  def getApriori(allRates: Array[CaratRate]) = {
    val startTime = start
    // get BLCs
    assert(allRates != null, "AllRates should not be null when calculating aPriori.")
    val ap = allRates.filter(!_.isRateRange())
    assert(ap.length > 0, "AllRates should contain some rates that are not rateRanges and less than %s when calculating aPriori.".format(ABNORMAL_RATE))
    // Get their rates and frequencies (1.0 for all) and group by rate 
    val rates = ap.map(_.rate)
    val rateMap = new scala.collection.mutable.HashMap[Double, Double]
    for (k <- rates) {
      val old = rateMap.getOrElse(k, 0.0) + 1.0
      rateMap += ((k, old))
    }

    val sum = rateMap.map(_._2).sum
    val ret: scala.collection.mutable.Map[Double, Double] = rateMap.map(x => { (x._1, x._2 / sum) })
    finish(startTime)
    ret
  }

  /**
   * Get the distributions, xmax, ev's and ev distance of two collections of CaratRates.
   */
  def getDistanceAndDistributions(sc: SparkContext, one: RDD[CaratRate], two: RDD[CaratRate], aPrioriDistribution: Map[Double, Double],
    buckets: Int, smallestBucket: Double, decimals: Int, DEBUG: Boolean = false) = {
    val startTime = start

    val (probWith, ev /*, usersWith*/ ) = getEvAndDistribution(one, aPrioriDistribution)
    val (probWithout, evNeg /*, usersWithout*/ ) = getEvAndDistribution(two, aPrioriDistribution)
    finish(startTime, "GetDists")
    var evDistance = 0.0

    if (probWith != null && probWithout != null) {
      var fStart = start
      // Log bucketing:
      val (xmax, bucketed, bucketedNeg) = ProbUtil.logBucketDists(sc, probWith, probWithout, buckets, smallestBucket, decimals)
      finish(fStart, "LogBucketing")

      evDistance = evDiff(ev, evNeg)

      if (DEBUG) {
        ProbUtil.debugNonZero(bucketed.map(_._2), bucketedNeg.map(_._2), "bucket")
      }
      finish(startTime)
      (xmax, bucketed, bucketedNeg, ev, evNeg, evDistance /*, usersWith, usersWithout*/ )
    } else
      (0.0, null, null, 0.0, 0.0, 0.0 /*, usersWith, usersWithout*/ )
  }

  def getDistanceAndDistributionsUnBucketed(sc: SparkContext, one: RDD[CaratRate], two: RDD[CaratRate], aPrioriDistribution: Map[Double, Double]) = {
    val startTime = start
    val (probWith, ev /*, usersWith*/ ) = getEvAndDistribution(one, aPrioriDistribution)
    val (probWithout, evNeg /*, usersWithout*/ ) = getEvAndDistribution(two, aPrioriDistribution)
    finish(startTime, "GetDists")
    var evDistance = 0.0

    if (probWith != null && probWithout != null) {
      var fStart = start
      val xmax = ProbUtil.getxmax(probWith, probWithout)
      finish(fStart, "getxmax")

      evDistance = evDiff(ev, evNeg)
      finish(startTime)
      (xmax, probWith, probWithout, ev, evNeg, evDistance)
    } else {
      finish(startTime)
      (0.0, null, null, 0.0, 0.0, 0.0)
    }
  }

  /**
   * Non-RDD version to debug if my problems are rdd-based.
   */
  def getDistanceAndDistributionsUnBucketed(one: Array[CaratRate], two: Array[CaratRate], aPrioriDistribution: Map[Double, Double]) = {
    val startTime = start
    val (probWith, ev /*, usersWith*/ ) = getEvAndDistribution(one, aPrioriDistribution)
    val (probWithout, evNeg /*, usersWithout*/ ) = getEvAndDistribution(two, aPrioriDistribution)
    finish(startTime, "GetDists")
    var evDistance = 0.0

    if (probWith != null && probWithout != null) {
      var fStart = start
      val xmax = ProbUtil.getxmax(probWith, probWithout)
      finish(fStart, "getxmax")

      evDistance = evDiff(ev, evNeg)
      finish(startTime)
      (xmax, probWith, probWithout, ev, evNeg, evDistance)
    } else {
      finish(startTime)
      (0.0, null, null, 0.0, 0.0, 0.0)
    }
  }

  /**
   * Get the distributions, xmax, ev's and ev distance of two collections of CaratRates.
   */
  def getEvAndDistribution(one: RDD[CaratRate], aPrioriDistribution: Map[Double, Double], enoughWith: Boolean) = {
    val startTime = start

    var checkedWith = enoughWith
    if (!checkedWith)
      checkedWith = one.take(DIST_THRESHOLD).length == DIST_THRESHOLD
    finish(startTime, "Counting")

    if (checkedWith) {
      var fStart = start
      val probWith = getProbDist(aPrioriDistribution, one)
      finish(fStart, "GetFreq")
      val ev = ProbUtil.getEv(probWith)

      fStart = start
      val usersWith = one.map(_.uuid).collect().toSet.size
      finish(fStart, "userCount")

      finish(startTime)
      (probWith, ev, usersWith)
    } else {
      println("Not enough samples: withCount < %d".format(DIST_THRESHOLD))
      finish(startTime)
      (null, 0.0, 0)
    }
  }

  /**
   * Get the distributions, xmax, ev's and ev distance of two collections of CaratRates.
   */
  def getEvAndDistribution(one: RDD[CaratRate], aPrioriDistribution: Map[Double, Double]) = {
    val startTime = start
    //var fStart = start
    val probWith = getProbDist(aPrioriDistribution, one)
    if (probWith != null) {
      //finish(fStart, "GetFreq")
      val ev = ProbUtil.getEv(probWith)
      /*
      fStart = start
      val usersWith = one.map(_.uuid).collect().toSet.size
      finish(fStart, "userCount")
*/
      finish(startTime)
      (probWith, ev /*, usersWith*/ )
    } else {
      finish(startTime)
      (null, 0.0)
    }
  }

  /**
   * Get the distributions, xmax, ev's and ev distance of two collections of CaratRates.
   * Non-RDD version to debug performance issues.
   */
  def getEvAndDistribution(one: Array[CaratRate], aPrioriDistribution: Map[Double, Double]) = {
    val startTime = start
    //var fStart = start
    val probWith = getProbDist(aPrioriDistribution, one)
    if (probWith != null) {
      //finish(fStart, "GetFreq")
      val ev = ProbUtil.getEv(probWith)
      /*
      fStart = start
      val usersWith = one.map(_.uuid).collect().toSet.size
      finish(fStart, "userCount")
*/
      finish(startTime)
      (probWith, ev /*, usersWith*/ )
    } else {
      finish(startTime)
      (null, 0.0)
    }
  }

  def getDistanceAndDistributionsNoCount(sc: SparkContext, one: RDD[CaratRate], two: RDD[CaratRate], aPrioriDistribution: Map[Double, Double],
    buckets: Int, smallestBucket: Double, decimals: Int, DEBUG: Boolean = false) = {
    val startTime = start

    // probability distribution: r, count/sumCount

    /* Figure out max x value (maximum rate) and bucket y values of 
     * both distributions into n buckets, averaging inside a bucket
     */

    /* FIXME: Should not flatten RDD's, but figure out how to transform an
     * RDD of Rates => RDD of UniformDists => RDD of Double,Double pairs (Bucketed values)  
     */
    var fStart = start
    val freqWith = getFrequencies(aPrioriDistribution, one)
    val freqWithout = getFrequencies(aPrioriDistribution, two)
    finish(fStart, "GetFreq")

    fStart = start
    val usersWith = one.map(_.uuid).collect().toSet.size
    val usersWithout = two.map(_.uuid).collect().toSet.size
    finish(fStart, "userCount")

    var evDistance = 0.0

    if (DEBUG) {
      ProbUtil.debugNonZero(freqWith.map(_._1).collect(), freqWithout.map(_._1).collect(), "rates")
    }
    fStart = start
    // Log bucketing:
    val (xmax, bucketed, bucketedNeg, ev, evNeg) = ProbUtil.logBucketRDDFreqs(sc, freqWith, freqWithout, buckets, smallestBucket, decimals)
    finish(fStart, "LogBucketing")

    evDistance = evDiff(ev, evNeg)
    if (evDistance > 0) {
      var imprHr = (100.0 / evNeg - 100.0 / ev) / 3600.0
      val imprD = (imprHr / 24.0).toInt
      imprHr -= imprD * 24.0
      printf("evWith=%s evWithout=%s evDistance=%s improvement=%s days %s hours\n", ev, evNeg, evDistance, imprD, imprHr)
      /*val sumPdf = freqWith.map(_._2).reduce(_ + _)
          val pdf = freqWith.map(x => {x._1, x._2/sumPdf})
          correlations(pdf, one)*/
    } else {
      printf("evWith=%s evWithout=%s evDistance=%s\n", ev, evNeg, evDistance)
    }

    if (DEBUG && bucketed != null && bucketedNeg != null) {
      ProbUtil.debugNonZero(bucketed.map(_._2), bucketedNeg.map(_._2), "bucket")
    }
    finish(startTime)
    (xmax, bucketed, bucketedNeg, ev, evNeg, evDistance, usersWith, usersWithout)
  }

  /*def correlations(pdf:RDD[(Double, Double)], dist: RDD[(Double, CaratRate)]) = {
    // correlation with model:
    // FIXME: If I map this to its model, it will always be the same for uuid and bugs with-distributions.
    // is this supposed to be calculated from the entire set of rates? or perhaps aPriori?
    val modelDist = dist.map(x => { (x._1, x._2.model) })
    val cModel = ProbUtil.pearsonCorrelation(pdf, dist.)
  }*/

  /**
   * Convert a set of rates into their frequencies, interpreting rate ranges as slices
   * of `aPrioriDistribution`.
   */
  def getFrequencies(aPrioriDistribution: Map[Double, Double], samples: RDD[CaratRate]) = {
    val startTime = start
    val flatSamples = samples.flatMap(x => {
      if (x.isRateRange()) {
        val freqRange = aPrioriDistribution.filter(y => { x.rateRange.contains(y._1) })
        val arr = freqRange.map { x =>
          {
            (x._1, x._2)
          }
        }.toArray

        var sum = 0.0
        for (k <- arr) {
          sum += k._2
        }
        arr.map(x => { (x._1, x._2 / sum) })
      } else
        Array((x.rate, 1.0))
    })

    val ret = flatSamples.groupByKey().map(x => {
      (x._1, x._2.sum)
    })
    finish(startTime)
    ret
  }

  /**
   * Convert a set of rates into their frequencies, interpreting rate ranges as slices
   * of `aPrioriDistribution`.
   * Non-RDD version to debug performance problems.
   */
  def getFrequencies(aPrioriDistribution: Map[Double, Double], samples: Array[CaratRate]) = {
    val startTime = start
    val flatSamples = samples.flatMap(x => {
      if (x.isRateRange()) {
        val freqRange = aPrioriDistribution.filter(y => { x.rateRange.contains(y._1) })
        val arr = freqRange.map { x =>
          {
            (x._1, x._2)
          }
        }.toArray

        var sum = 0.0
        for (k <- arr) {
          sum += k._2
        }
        arr.map(x => { (x._1, x._2 / sum) })
      } else
        Array((x.rate, 1.0))
    })

    var hmap = new HashMap[Double, Double]

    for (k <- flatSamples)
      hmap += ((k._1, hmap.getOrElse(k._1, 0.0) + k._2))

    val ret = hmap.toArray
    finish(startTime)
    ret
  }

  def getProbDist(aPrioriDistribution: Map[Double, Double], samples: RDD[CaratRate]) = {
    val freq = getFrequencies(aPrioriDistribution, samples)
    val hasPoints = freq.take(1) match {
      case Array(t) => true
      case _ => false
    }

    if (hasPoints) {
      val sum = freq.map(_._2).reduce(_ + _)
      freq.map(x => { (x._1, x._2 / sum) })
    } else
      null
  }

  /**
   * Non-RDD version to debug performance problems.
   */
  def getProbDist(aPrioriDistribution: Map[Double, Double], samples: Array[CaratRate]) = {
    val freq = getFrequencies(aPrioriDistribution, samples)
    val hasPoints = freq.take(1) match {
      case Array(t) => true
      case _ => false
    }

    if (hasPoints) {
      val sum = freq.map(_._2).reduce(_ + _)
      freq.map(x => { (x._1, x._2 / sum) })
    } else
      null
  }

  def mapToRateEv(aPrioriDistribution: Map[Double, Double], samples: RDD[CaratRate]) = {
    val startTime = start
    val evSamples = samples.map(x => {
      if (x.isRateRange()) {
        val freqRange = aPrioriDistribution.filter(y => { x.rateRange.contains(y._1) })
        val arr = freqRange.map { x =>
          {
            (x._1, x._2)
          }
        }.toArray

        var sum = 0.0
        for (k <- arr) {
          sum += k._2
        }
        val norm = arr.map(x => { (x._1, x._2 / sum) })
        var ret = 0.0
        for (k <- norm)
          ret += k._1 * k._2
        (x, ret)
      } else
        (x, x.rate)
    })
    finish(start)
    evSamples
  }

  /**
   * Non-RDD version to debug performance problems.
   */
  def mapToRateEv(aPrioriDistribution: Map[Double, Double], samples: Array[CaratRate]) = {
    val startTime = start
    val evSamples = samples.map(x => {
      if (x.isRateRange()) {
        val freqRange = aPrioriDistribution.filter(y => { x.rateRange.contains(y._1) })
        val arr = freqRange.map { x =>
          {
            (x._1, x._2)
          }
        }.toArray

        var sum = 0.0
        for (k <- arr) {
          sum += k._2
        }
        val norm = arr.map(x => { (x._1, x._2 / sum) })
        var ret = 0.0
        for (k <- norm)
          ret += k._1 * k._2
        (x, ret)
      } else
        (x, x.rate)
    })
    finish(start)
    evSamples
  }

  def daemons_globbed(allApps: Set[String]) = {
    val startTime = start
    val globs = DAEMONS_LIST.filter(_.endsWith("*")).map(x => { x.substring(0, x.length - 1) })

    var matched = allApps.filter(x => {
      val globPrefix = globs.filter(x.startsWith(_))
      !globPrefix.isEmpty
    })

    println("Matched daemons with globs: " + matched)
    val ret = DAEMONS_LIST ++ matched
    finish(startTime)
    ret
  }

  def removeDaemons() {
    removeDaemons(DAEMONS_LIST)
  }

  def removeDaemons(daemonSet: Set[String]) {
    val startTime = start
    // add hog table key (which is the same as bug table app key)
    val kd = daemonSet.map(x => {
      (hogKey, x)
    }).toSeq
    DynamoDbItemLoop(DynamoDbDecoder.filterItems(hogsTable, kd: _*),
      DynamoDbDecoder.filterItemsFromKey(hogsTable, _, kd: _*),
      removeHogs(_, _))

    DynamoDbItemLoop(DynamoDbDecoder.filterItems(bugsTable, kd: _*),
      DynamoDbDecoder.filterItemsFromKey(bugsTable, _, kd: _*),
      removeBugs(_, _))

    finish(startTime)
  }

  def removeBugs(key: Key, results: java.util.List[java.util.Map[String, AttributeValue]]) {
    for (k <- results) {
      val uuid = k.get(resultKey).getS()
      val app = k.get(hogKey).getS()
      println("Removing Bug: %s, %s".format(uuid, app))
      DynamoDbDecoder.deleteItem(bugsTable, uuid, app)
    }
  }

  def removeHogs(key: Key, results: java.util.List[java.util.Map[String, AttributeValue]]) {
    for (k <- results) {
      val app = k.get(hogKey).getS()
      println("Deleting: " + app)
      DynamoDbDecoder.deleteItem(hogsTable, app)
    }
  }

  /**
   * Generic DynamoDb loop function. Gets items from a table using keys given, and continues until the table scan is complete.
   * This function achieves a block by block read until the end of a table, regardless of throughput or manual limits.
   */
  def DynamoDbItemLoop(getKeyAndResults: => (Key, java.util.List[java.util.Map[String, AttributeValue]]),
    getKeyAndMoreResults: Key => (Key, java.util.List[java.util.Map[String, AttributeValue]]),
    handleResults: (Key, java.util.List[java.util.Map[String, AttributeValue]]) => Unit) {
    val startTime = start
    var index = 0
    var (key, results) = getKeyAndResults
    println("Got: " + results.size + " results.")
    handleResults(null, results)

    while (key != null) {
      index += 1
      println("Continuing from key=" + key)
      val (key2, results2) = getKeyAndMoreResults(key)
      results = results2
      handleResults(key, results)
      key = key2
      println("Got: " + results.size + " results.")
    }
    finish(startTime)
  }
}
