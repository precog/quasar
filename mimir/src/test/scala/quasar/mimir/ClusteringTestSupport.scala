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

import scala.util.Random._

import quasar.blueeyes._
import quasar.precog.common._
import quasar.precog.util.IOUtils
import spire.implicits._

trait ClusteringTestSupport {

  case class GeneratedPointSet(points: Array[Array[Double]], centers: Array[Array[Double]])

  def genPoints(n: Int, dimension: Int, k: Int): GeneratedPointSet = {

    def genPoint(x: => Double): Array[Double] = Array.fill(dimension)(x)

    val s = math.pow(2 * k, 1.0 / dimension)
    val centers = (1 to k).map({ _ => genPoint(nextDouble * s) }).toArray
    val points = (1 to n).map({ _ =>
      val c = nextInt(k)
      genPoint(nextGaussian) + centers(c)
    }).toArray

    GeneratedPointSet(points, centers)
  }

  def pointToJson(p: Array[Double]): RValue = {
    RArray(p.toSeq map (CNum(_)): _*)
  }

  def pointsToJson(points: Array[Array[Double]]): List[RValue] = points.toList map (pointToJson(_))

  def writePointsToDataset[A](points: Array[Array[Double]])(f: String => A): A = {
    writeRValuesToDataset(pointsToJson(points))(f)
  }

  def writeRValuesToDataset[A](jvals: List[RValue])(f: String => A): A = {
    val lines = jvals map { _.toJValue.renderCompact }
    val tmpFile = java.io.File.createTempFile("values", ".json")
    IOUtils.writeSeqToFile(lines, tmpFile).unsafePerformIO
    val pointsString0 = "filesystem" + tmpFile.toString
    val pointsString = pointsString0.take(pointsString0.length - 5)
    val result = f(pointsString)
    tmpFile.delete()
    result
  }

}
