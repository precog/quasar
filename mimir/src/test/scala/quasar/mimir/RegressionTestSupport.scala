/*
 * Copyright 2014–2017 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.mimir

import scala.util.Random
import scala.collection.mutable

import quasar.precog.common._
import quasar.yggdrasil.util.CPathUtils._
import quasar.blueeyes.json._

trait RegressionTestSupport[M[+_]] {
  def makeTBounded: Double = {
    val theta = Random.nextGaussian * 10
    // we omit values of theta close to zero
    // this is so that normal distribution of the y-values doesn't need to depend on the x-values
    if (theta >= -3 && theta <= 3) makeTBounded
    else theta
  }

  def makeT: Double = {
    val theta = Random.nextGaussian * 10
    if (theta == 0) makeT
    else theta
  }

  def makeThetas(length: Int): Array[Double] = {
    val seq = List(makeT) ++ Seq.fill(length - 1)(makeTBounded)
    seq.toArray
  }

  def jvalues(samples: Seq[(Array[Double], Double)], cpaths: Seq[CPath], mod: Int = 1): Seq[JValue] = samples.zipWithIndex map { case ((xs, y), idx) =>
    val cvalues = xs.map { x => CDouble(x).asInstanceOf[CValue] } :+ CDouble(y.toDouble).asInstanceOf[CValue]
    val withCPath = {
      if (idx % mod == 0) cpaths zip cvalues.toSeq
      else if (idx % mod == 1) cpaths.tail zip cvalues.tail.toSeq
      else cpaths.tail.tail zip cvalues.tail.tail.toSeq
    }
    val withJPath = withCPath map { case (cpath, cvalue) => cPathToJPaths(cpath, cvalue) head }  // `head` is only okay if we don't have any homogeneous arrays
    val withJValue = withJPath map { case (jpath, cvalue) => (jpath, cvalue.toJValue) }
    withJValue.foldLeft(JArray(Nil).asInstanceOf[JValue]) { case (target, (jpath, jvalue)) => target.unsafeInsert(jpath, jvalue) }
  }

  def stdDevMean(values: List[Double]): (Double, Double) = {
    val count = values.size
    val sum = values.sum
    val sumsq = values map { x => math.pow(x, 2) } sum

    val stdDev = math.sqrt(count * sumsq - sum * sum) / count
    val mean = sum / count

    (stdDev, mean)
  }

  //more robust way to deal with outliers than stdDev
  //the `constant` is the conversion constant to the units of standard deviation
  def madMedian(values: List[Double]): (Double, Double) = {
    val constant = 0.6745

    def computeMedian(values: List[Double]) = {
      val length = values.length
      val sorted = values.sorted

      if (length % 2 == 0) {
        val idx1 = (length / 2) - 1
        val idx2 = (length / 2)
        (sorted(idx1) + sorted(idx2)) / 2
      } else {
        sorted(length / 2)
      }
    }

    val median = computeMedian(values)

    val diffs = values map { v => math.abs(v - median) }
    val mad = computeMedian(diffs) / constant

    (mad, median)
  }

  def combineResults(num: Int, thetas: List[List[Double]]) = {
    thetas.foldLeft(mutable.Seq.fill(num)(List.empty[Double])) { case (acc, li) =>
      var i = 0
      while (i < li.length) {
        acc(i) = acc(i) :+ li(i)
        i += 1
      }
      acc
    }
  }

  def isOk(actual: Double, computed: List[Double]): Boolean = {
    val (mad, median) = madMedian(computed)
    val diff = math.abs(median - actual)
    val numStdDev = 4D

    diff < mad * numStdDev
  }
}
