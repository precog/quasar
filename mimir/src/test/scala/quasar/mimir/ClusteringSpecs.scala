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

import quasar.precog.common._
import quasar.yggdrasil._

import spire.implicits._
import scala.util.Random
import quasar.blueeyes._
import scalaz._

trait ClusteringLibSpecs[M[+_]] extends EvaluatorSpecification[M]
    with ClusteringTestSupport
    with LongIdMemoryDatasetConsumer[M]{ self =>

  import dag._
  import instructions._
  import library._

  val line = Line(0, 0, "")

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(graph, defaultEvaluationContext) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  def kMediansCost(points: Array[Array[Double]], centers: Array[Array[Double]]): Double = {
    points.foldLeft(0.0) { (cost, p) =>
      cost + centers.map({ c => (p - c).norm }).qmin
    }
  }

  val ClusterIdPattern = """cluster\d+""".r

  def isGoodCluster(clusterMap: Map[String, SValue], points: Array[Array[Double]], centers: Array[Array[Double]], k: Int, dimension: Int) = {
    val targetCost = kMediansCost(points, centers)

    clusterMap must haveSize(k)
    clusterMap.keys forall (k => (k startsWith "cluster") must_== true)

    def getPoint(sval: SValue): List[Double] = sval match {
      case SArray(arr) =>
        arr.collect({ case SDecimal(n) => n.toDouble }).toList
      case SObject(obj) =>
        obj.toList.sortBy(_._1) flatMap { case (_, v) => getPoint(v) }
      case _ => sys.error("not supported")
    }

    val clusters: Array[Array[Double]] = clusterMap.values.map({ sval =>
      val arr = getPoint(sval)
      arr must haveSize(dimension)
      arr.toArray
    }).toArray

    val cost = kMediansCost(points, clusters)

    cost must be_<(3 * targetCost)
  }

  def clusterInput(dataset: String, k: Long) = {
    dag.Morph2(KMediansClustering,
      dag.AbsoluteLoad(Const(CString(dataset))(line))(line),
      dag.Const(CLong(k))(line)
    )(line)
  }

  "k-medians clustering" should {
    "compute trivial k-medians clustering" in {
      val dimension = 4
      val k = 5
      val GeneratedPointSet(points, centers) = genPoints(2000, dimension, k)

      writePointsToDataset(points) { dataset =>
        val input  = clusterInput(dataset, k)
        val result = testEval(input)

        result must haveSize(1)
        val (Vector(), SObject(obj)) = result.head
        obj.keys.toSet must_== Set("model1")
        obj("model1") must beLike {
          case SObject(clusterMap) => isGoodCluster(clusterMap, points, centers, k, dimension)
        }
      }
    }

    "return result when given one row, with k > 1" in {
      val k = 5
      val clusterIds = (1 to k).map("cluster" + _).toSet

      val input = dag.Morph2(KMediansClustering,
        dag.Const(CNum(4.4))(line),
        dag.Const(CLong(k))(line)
      )(line)

      val result = testEval(input)

      result must haveSize(1)

      result must haveAllElementsLike { case (ids, SObject(obj)) if ids.size == 0 =>
        obj.keys mustEqual Set("model1")
        obj("model1") must beLike {
          case SObject(clusterMap) =>
            clusterMap.keys mustEqual clusterIds
            clusterIds.map(clusterMap(_)) must haveAllElementsLike {
              case SDecimal(d) => d mustEqual(4.4)
            }
        }
      }
    }

    "return result when given fewer than k numeric rows" in {
      val k = 8
      val clusterIds = (1 to k).map("cluster" + _).toSet
      val dataset = "/hom/numbers"

      val input = clusterInput(dataset, k)

      val numbers = dag.AbsoluteLoad(dag.Const(CString(dataset))(line))(line)

      val result = testEval(input)
      val resultNumbers = testEval(numbers)

      result must haveSize(1)

      val expected = resultNumbers collect { case (_, SDecimal(num)) => num }

      result must haveAllElementsLike { case (ids, SObject(obj)) if ids.size == 0 =>
        obj.keys mustEqual Set("model1")
        obj("model1") must beLike {
          case SObject(clusterMap) =>
            clusterMap.keys mustEqual clusterIds
            clusterIds.map(clusterMap(_)) must haveAllElementsLike {
              case SDecimal(d) => expected must contain(d)
            }

            val actual = clusterIds.map(clusterMap(_)) collect { case SDecimal(d) => d }
            actual.toSet mustEqual expected
        }
      }
    }

    "return result when given fewer than k rows, where the rows are objects" in {
      val k = 8
      val clusterIds = (1 to k).map("cluster" + _).toSet
      val dataset = "/hom/heightWeight"

      val input = clusterInput(dataset, k)

      val data = dag.AbsoluteLoad(dag.Const(CString(dataset))(line))(line)

      val result = testEval(input)
      val resultData = testEval(data)

      result must haveSize(1)

      val expected = resultData collect { case (_, SObject(obj)) => obj }

      result must haveAllElementsLike { case (ids, SObject(obj)) if ids.size == 0 =>
        obj.keys mustEqual Set("model1")
        obj("model1") must beLike {
          case SObject(clusterMap) =>
            clusterMap.keys mustEqual clusterIds
            clusterIds.map(clusterMap(_)) must haveAllElementsLike {
              case SObject(obj) => expected must contain(obj)
            }

            val actual = clusterIds.map(clusterMap(_)) collect { case SObject(obj) => obj }
            actual.toSet mustEqual expected
        }
      }
    }

    "compute k-medians clustering with two distinct schemata" in {
      val dimensionA = 4
      val dimensionB = 12
      val k = 15

      val GeneratedPointSet(pointsA, centersA) = genPoints(5000, dimensionA, k)
      val GeneratedPointSet(pointsB, centersB) = genPoints(5000, dimensionB, k)

      val jvalsA = pointsToJson(pointsA) map { v => RObject(Map("a" -> v)) }
      val jvalsB = pointsToJson(pointsB) map { v => RObject(Map("b" -> v)) }
      val jvals = Random.shuffle(jvalsA ++ jvalsB)

      writeRValuesToDataset(jvals) { dataset =>
        val input                    = clusterInput(dataset, k)
        val result                   = testEval(input)
        val (Vector(), SObject(obj)) = result.head

        def checkmodel(model: SValue) = model must beLike {
          case SObject(clusterMap) =>
            clusterMap("cluster1") must beLike { case SObject(schemadCluster) =>
              if (schemadCluster contains "a")
                isGoodCluster(clusterMap, pointsA, centersA, k, dimensionA)
              else
                isGoodCluster(clusterMap, pointsB, centersB, k, dimensionB)
            }
        }
        (obj.keys must_== Set("model1", "model2")) && checkmodel(obj("model1")) && checkmodel(obj("model2"))
      }
    }

    "compute k-medians clustering with overlapping schemata" in {
      val dimension = 6
      val k = 20

      val GeneratedPointSet(pointsA, centersA) = genPoints(5000, dimension, k)

      val points = pointsA.zipWithIndex map {
        case (p, i) if i % 3 == 2 => p ++ Array.fill(3)(Random.nextDouble)
        case (p, _)               => p
      }

      writePointsToDataset(points) { dataset =>
        val input                    = clusterInput(dataset, k)
        val result                   = testEval(input)
        val (Vector(), SObject(obj)) = result.head

        def checkmodel(model: SValue) = model must beLike {
          case SObject(clusterMap) =>
            clusterMap("cluster1") must beLike {
              case SArray(arr) if arr.size == 6 =>
                isGoodCluster(clusterMap, pointsA, centersA, k, dimension)
              case SArray(arr) if arr.size == 9 =>
                ok
            }
        }

        (obj.keys.toSet must_== Set("model1", "model2")) && checkmodel(obj("model1")) && checkmodel(obj("model2"))
      }
    }
  }

  def assign(points: Array[Array[Double]], centers: Array[Array[Double]]): Map[RValue, String] = {
    points.map { p =>
      val id = (0 until centers.length) minBy { i => (p - centers(i)).norm }
      pointToJson(p) -> ("cluster" + (id +  1))
    }.toMap
  }

  def makeClusters(centers: Array[Array[Double]]) = {
    RObject(pointsToJson(centers).zipWithIndex.map { case (ctr, idx) =>
      ("cluster" + (idx + 1), ctr)
    }.toMap)
  }

  def createDAG(pointsDataSet: String, modelDataSet: String) = {
    val points = dag.AbsoluteLoad(Const(CString(pointsDataSet))(line))(line)

    val input = dag.Morph2(AssignClusters,
      points,
      dag.AbsoluteLoad(Const(CString(modelDataSet))(line))(line)
    )(line)

    dag.Join(JoinObject, IdentitySort,
      input,
      dag.Join(WrapObject, Cross(Some(TableModule.CrossOrder.CrossRight)),
        Const(CString("point"))(line),
        points)(line))(line)
  }

  def testCluster(model: Map[String, SValue], clusterMap: Map[String, RValue], assignments: Map[RValue, String], point: RValue) = {
    model.keySet mustEqual Set("clusterId", "clusterCenter")

    model("clusterId") must beLike { case SString(clusterId) =>
      clusterId must_== assignments(point)
    }

    model("clusterCenter") must beLike { case SArray(arr0) =>
      val arr = arr0 collect { case SDecimal(d) => d }

      val rvalue = clusterMap((model("clusterId"): @unchecked) match {
        case SString(s) => s
      })
      val res = (rvalue: @unchecked) match {
        case RArray(values) => values collect { case CNum(x) => x }
      }

      arr must_== res
    }
  }

  "assign clusters" should {
    "assign correctly with a single schema" in {
      val size = 3000
      val dimension = 4
      val k = 8

      val GeneratedPointSet(points, centers) = genPoints(size, dimension, k)

      val clusters = makeClusters(centers)

      val clusterMap = clusters match { case RObject(xs) => xs }

      val model1 = RObject(Map("model1" -> clusters))
      val assignments = assign(points, centers)

      writeRValuesToDataset(List(model1)) { modelDataSet =>
        writePointsToDataset(points) { pointsDataSet =>
          val input2 = createDAG(pointsDataSet, modelDataSet)
          val result = testEval(input2)

          result must haveSize(size)

          result must haveAllElementsLike { case (ids, SObject(obj)) =>
            ids.length mustEqual 2
            obj.keySet mustEqual Set("point", "model1")

            val point = obj("point")

            obj("model1") must beLike { case SObject(model) =>
              testCluster(model, clusterMap, assignments, point.toRValue)
            }
          }
        }
      }
    }

    "assign correctly with two distinct schemata" in {
      val dimensionA = 4
      val dimensionB = 12
      val k = 15
      val size = 5000

      val GeneratedPointSet(pointsA, centersA) = genPoints(size, dimensionA, k)
      val GeneratedPointSet(pointsB, centersB) = genPoints(size, dimensionB, k)

      val points = Random.shuffle(pointsA.toList ++ pointsB.toList).toArray

      val clustersA = makeClusters(centersA)
      val clustersB = makeClusters(centersB)

      val clusterMapA = clustersA match { case RObject(xs) => xs }
      val clusterMapB = clustersB match { case RObject(xs) => xs }

      val models = RObject(Map("model1" -> clustersA, "model2" -> clustersB))

      val assignmentsA = assign(pointsA ++ pointsB, centersA)
      val assignmentsB = assign(pointsB, centersB)

      writeRValuesToDataset(List(models)) { modelDataSet =>
        writePointsToDataset(points) { pointsDataSet =>
          val input = createDAG(pointsDataSet, modelDataSet)
          val result = testEval(input)

          result must haveSize(points.size)

          result must haveAllElementsLike { case (ids, SObject(obj)) =>
            ids.length mustEqual 2

            (obj.keySet mustEqual Set("point", "model1")) or
              (obj.keySet mustEqual Set("point", "model1", "model2"))

            val point = obj("point")

            point must beLike { case SArray(arr) =>
              (arr must haveSize(dimensionA)) or
                (arr must haveSize(dimensionB))
            }

            obj("model1") must beLike { case SObject(model) =>
              testCluster(model, clusterMapA, assignmentsA, point.toRValue)
            }

            if (obj.contains("model2")) {
              obj("model2") must beLike { case SObject(model) =>
                testCluster(model, clusterMapB, assignmentsB, point.toRValue)
              }
            } else {
              ok
            }
          }
        }
      }
    }

    "assign correctly with two overlapping schemata" in {
      val dimension = 6
      val k = 20
      val size = 5000

      val GeneratedPointSet(points0, centers) = genPoints(size, dimension, k)

      val points = points0.zipWithIndex map {
        case (p, i) if i % 3 == 2 => p ++ Array.fill(3)(Random.nextDouble)
        case (p, _) => p
      }

      val clusters = makeClusters(centers)

      val clusterMap = clusters match { case RObject(xs) => xs }

      val model = RObject(Map("model1" -> clusters))

      val assignments = assign(points, centers)

      writeRValuesToDataset(List(model)) { modelDataSet =>
        writePointsToDataset(points) { pointsDataSet =>
          val input2 = createDAG(pointsDataSet, modelDataSet)
          val result = testEval(input2)

          result must haveSize(size)

          result must haveAllElementsLike { case (ids, SObject(obj)) =>
            ids.length mustEqual 2

            (obj.keySet mustEqual Set("point", "model1"))

            val point = obj("point")

            obj("model1") must beLike { case SObject(model) =>
              testCluster(model, clusterMap, assignments, point.toRValue)
            }
          }
        }
      }
    }

    "assign correctly with multiple rows of schema with overlapping modelIds" in {
      val input = dag.Morph2(AssignClusters,
        dag.AbsoluteLoad(Const(CString("/hom/clusteringData"))(line))(line),
        dag.AbsoluteLoad(Const(CString("/hom/clusteringModel"))(line))(line)
      )(line)

      val result0 = testEval(input)

      result0 must haveSize(7)

      val result = result0 collect { case (ids, value) if ids.size == 2 => value }

      result mustEqual Set(
        SObject(Map("model1" -> SObject(Map("clusterId" -> SString("cluster2"), "clusterCenter" -> SObject(Map("bar" -> SDecimal(9.0), "foo" -> SDecimal(4.4))))))),
        SObject(Map("model2" -> SObject(Map("clusterId" -> SString("cluster1"), "clusterCenter" -> SObject(Map("baz" -> SDecimal(4.0))))))),
        SObject(Map("model1" -> SObject(Map("clusterId" -> SString("cluster2"), "clusterCenter" -> SArray(Vector(SDecimal(6.0), SDecimal(3.0), SDecimal(2.0))))))),
        SObject(Map("model1" -> SObject(Map("clusterId" -> SString("cluster3"), "clusterCenter" -> SArray(Vector(SDecimal(0.0), SDecimal(3.2), SDecimal(5.1))))))),
        SObject(Map("model1" -> SObject(Map("clusterId" -> SString("cluster1"), "clusterCenter" -> SArray(Vector(SDecimal(2.1), SDecimal(3.3), SDecimal(4.0))))))))
    }
  }
}

object ClusteringLibSpecs extends ClusteringLibSpecs[Need]
