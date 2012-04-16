package edu.berkeley.cs.amplab.carat

import spark._
import spark.SparkContext._
import spark.timeseries._
import edu.berkeley.cs.amplab.carat.plot.PlotUtil
import edu.berkeley.cs.amplab.carat.dynamodb.DynamoAnalysisUtil
import scala.collection.immutable.TreeMap
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import collection.JavaConversions._
import scala.collection.mutable.Buffer

/**
 * Run Carat Analysis without storing any data, just speed testing the implementation.
 * Uses the newer `StoredSampleAnalysisGeneric` for the analysis.
 *
 * @author Eemil Lagerspetz
 */

object PrintStats {

  
  /**
   *  Print dists instead of saving or plotting them. Used for speed testing.
   */

  def printDists(nature: String, keyValue1: String, keyValue2: String, title: String, titleNeg: String,
    one: Array[CaratRate], two: Array[CaratRate], aPrioriDistribution: Map[Double, Double], isBugOrHog: Boolean,
    filtered: Array[CaratRate], oses: Set[String], models: Set[String],
    totalsByUuid: scala.collection.immutable.TreeMap[String, (Double, Double)], usersWith: Int, usersWithout: Int, uuid: String) = {
    var hasSamples = true
    if (usersWith == 0 && usersWithout == 0) {
      hasSamples = one.take(1) match {
        case Array(t) => true
        case _ => false
      }
      hasSamples = two.take(1) match {
        case Array(t) => hasSamples && true
        case _ => false
      }
    }
    if (hasSamples) {
      val (xmax, probDist, probDistNeg, ev, evNeg, evDistance /*, usersWith, usersWithout*/ ) = DynamoAnalysisUtil.getDistanceAndDistributionsUnBucketed(one, two, aPrioriDistribution)
      if (probDist != null && probDistNeg != null && (!isBugOrHog || evDistance > 0)) {
        if (evDistance > 0) {
          var imprHr = (100.0 / evNeg - 100.0 / ev) / 3600.0
          val imprD = (imprHr / 24.0).toInt
          imprHr -= imprD * 24.0
          printf("%s evWith=%s evWithout=%s evDistance=%s improvement=%s days %s hours (%s vs %s users)\n", title, ev, evNeg, evDistance, imprD, imprHr, usersWith, usersWithout)
        } else {
          printf("%s evWith=%s evWithout=%s evDistance=%s (%s vs %s users)\n", title, ev, evNeg, evDistance, usersWith, usersWithout)
        }
        if (isBugOrHog && filtered != null) {
          val (osCorrelations, modelCorrelations, userCorrelations) = DynamoAnalysisUtil.correlation(title, filtered, aPrioriDistribution, models, oses, totalsByUuid)
          print(title, titleNeg, xmax, probDist, probDistNeg, ev, evNeg, evDistance, osCorrelations, modelCorrelations, userCorrelations, usersWith, usersWithout, uuid)
        } else
          print(title, titleNeg, xmax, probDist, probDistNeg, ev, evNeg, evDistance, null, null, null, usersWith, usersWithout, uuid)
      }
      isBugOrHog && evDistance > 0
    } else
      false
  }

  def printSkipped(nature: String, keyValue1: String, keyValue2: String, title: String) {
    printf("%s %s skipped for too few points.".format(nature, title))
  }

  def print(title: String, titleNeg: String, xmax: Double, distWith: Array[(Double, Double)],
    distWithout: Array[(Double, Double)],
    ev: Double, evNeg: Double, evDistance: Double,
    osCorrelations: scala.collection.immutable.Map[String, Double], modelCorrelations: scala.collection.immutable.Map[String, Double], userCorrelations: scala.collection.immutable.Map[String, Double],
    usersWith: Int, usersWithout: Int, uuid: String) {
    println("Calculated %s vs %s xmax=%s ev=%s evWithout=%s evDistance=%s osCorrelations=%s modelCorrelations=%s userCorrelations=%s uuid=%s".format(
      title, titleNeg, xmax, ev, evNeg, evDistance, osCorrelations, modelCorrelations, userCorrelations, uuid))
  }

  /**
   * The J-Score is the % of people with worse = higher energy use.
   * therefore, it is the size of the set of evDistances that are higher than mine,
   * compared to the size of the user base.
   * Note that the server side multiplies the JScore by 100, and we store it here
   * as a fraction.
   */

  def printJScores(allRates: Array[CaratRate], aPrioriDistribution: Map[Double, Double], distsWithUuid: TreeMap[String, Array[(Double, Double)]],
    distsWithoutUuid: TreeMap[String, Array[(Double, Double)]],
    parametersByUuid: TreeMap[String, (Double, Double, Double)],
    evDistanceByUuid: TreeMap[String, Double],
    appsByUuid: TreeMap[String, Set[String]],
    uuidToOsAndModel: scala.collection.mutable.HashMap[String, (String, String)],
    decimals: Int) {
    val oses = uuidToOsAndModel.map(_._2._1).toSet
    val models = uuidToOsAndModel.map(_._2._2).toSet
    val evByUuid = parametersByUuid.map(x => {
      (x._1, x._2._2)
    })

    for (os <- oses) {
      // can be done in parallel, independent of anything else
      val fromOs = allRates.filter(_.os == os)
      //val notFromOs = allRates.filter(_.os != os)
      // no distance check, not bug or hog
      printVarianceAndSampleCount(os, fromOs, aPrioriDistribution, evByUuid, uuidToOsAndModel)
    }

    for (model <- models) {
      // can be done in parallel, independent of anything else
      val fromModel = allRates.filter(_.model == model)
      //val notFromModel = allRates.filter(_.model != model)
      // no distance check, not bug or hog
      printVarianceAndSampleCount(model, fromModel, aPrioriDistribution, evByUuid, uuidToOsAndModel)
    }

    val dists = evDistanceByUuid.map(_._2).toSeq.sorted

    for (k <- distsWithUuid.keys) {
      val (xmax, ev, evNeg) = parametersByUuid.get(k).getOrElse((0.0, 0.0, 0.0))

      /**
       * jscore is the % of people with worse = higher energy use.
       * therefore, it is the size of the set of evDistances that are higher than mine,
       * compared to the size of the user base.
       */
      val jscore = {
        val temp = evDistanceByUuid.get(k).getOrElse(0.0)
        if (temp == 0)
          0
        else
          ProbUtil.nDecimal(dists.filter(_ > temp).size * 1.0 / dists.size, decimals)
      }
      val distWith = distsWithUuid.get(k).getOrElse(null)
      val distWithout = distsWithoutUuid.get(k).getOrElse(null)
      val apps = appsByUuid.get(k).getOrElse(null)
      if (distWith != null && distWithout != null && apps != null) {
        val (os, model) = uuidToOsAndModel.getOrElse(k, ("", ""))
        println("Calculated Profile for %s %s running %s xmax=%s ev=%s evWithout=%s jscore=%s apps=%s".format(k, model, os, xmax, ev, evNeg, jscore, apps.size))
      } else
        printf("Error: Could not plot jscore, because: distWith=%s distWithout=%s apps=%s\n", distWith, distWithout, apps)
    }
  }

  def printVarianceAndSampleCount(title: String,
    one: Array[CaratRate], aPrioriDistribution: Map[Double, Double],
    allEvs: scala.collection.immutable.TreeMap[String, Double],
    uuidToOsAndModel: scala.collection.mutable.HashMap[String, (String, String)]) = {
    val usersWith = one.map(_.uuid).size//.collect().toSet.size
    // the ev is over all the points in the distribution
    val (probOne, ev) = DynamoAnalysisUtil.getEvAndDistribution(one, aPrioriDistribution)
    // convert to prob dist
    val evOne = probOne.map(x => { (x._1 * x._2) })
    val mean = ProbUtil.mean(evOne)
    val variance = ProbUtil.variance(evOne, mean)
    val sampleCount = one.size//.count()

    val userEvs = allEvs.filter(x => {
      val p = uuidToOsAndModel.get(x._1).getOrElse("", "")
      p._1 == title || p._2 == title
    }).map(_._2).toSeq
    val meanU = ProbUtil.mean(userEvs)
    val varianceU = ProbUtil.variance(userEvs, meanU)

    var imprMin = (100.0 / (ev) - 100.0 / (ev + variance)) / 60.0
    var imprHr = (imprMin / 60.0).toInt
    imprMin -= imprHr * 60.0
    var imprD = (imprHr / 24.0).toInt
    imprHr -= imprD * 24

    println("%s ev=%s mean=%s variance=%s (%s d %s h %s min), clients=%s samples=%s".format(title, ev, mean, variance, imprD, imprHr, imprMin, usersWith, sampleCount))

    var imprMinU = (100.0 / (ev) - 100.0 / (ev + varianceU)) / 60.0
    var imprHrU = (imprMinU / 60.0).toInt
    imprMinU -= imprHrU * 60.0
    var imprDU = (imprHrU / 24.0).toInt
    imprHrU -= imprDU * 24

    println("%s ev=%s meanU=%s varianceU=%s (%s d %s h %s min), clients=%s samples=%s".format(title, ev, meanU, varianceU, imprDU, imprHrU, imprMinU, usersWith, sampleCount))
  }

  def globalCorrelations(name: String,
    osCorrelations: scala.collection.immutable.Map[String, Double],
    modelCorrelations: scala.collection.immutable.Map[String, Double],
    userCorrelations: scala.collection.immutable.Map[String, Double],
    usersWith: Int, usersWithout: Int, uuid: String = null) {
    println("Calculated global correlations: osCorrelations=%s modelCorrelations=%s userCorrelations=%s".format(osCorrelations, modelCorrelations, userCorrelations))
  }
}

