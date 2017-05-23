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
import quasar.blueeyes._
import scalaz._

case class Precision(p: Double)
class AlmostEqual(d: Double) {
  def ~=(d2: Double)(implicit p: Precision) = (d - d2).abs <= p.p
}

trait StatsLibSpecs[M[+_]] extends EvaluatorSpecification[M]
    with LongIdMemoryDatasetConsumer[M]{ self =>

  import dag._
  import instructions._
  import library._

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(graph, defaultEvaluationContext) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  implicit def add_~=(d: Double) = new AlmostEqual(d)
  implicit val precision = Precision(0.000000001)

  "homogenous sets" should {
    "median with odd number of elements" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Median,
        dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }

      result2.toSet must_== Set(13)
    }

    "median with even number of elements" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Median,
        dag.AbsoluteLoad(Const(CString("/hom/numbers5"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }

      result2.toSet must_== Set(2)
    }

    "median with singleton" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Median,
        Const(CLong(42))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }

      result2.toSet must_== Set(42)
    }

    "mode" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Mode,
        dag.AbsoluteLoad(Const(CString("/hom/numbers2"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)
      result.head must_== SArray(Vector(SDecimal(1)))
    }.pendingUntilFixed

    "mode with a singleton" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Mode,
        Const(CLong(42))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2.toSet must_== Set(Vector(SDecimal(42)))
    }.pendingUntilFixed

    "mode where each value appears exactly once" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Mode,
        dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2.toSet must_== Set(Vector(SDecimal(1), SDecimal(12), SDecimal(13), SDecimal(42), SDecimal(77)))
    }.pendingUntilFixed

    "assign dummy variables to loaded dataset" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Dummy,
        dag.AbsoluteLoad(Const(CString("/hom/numbers"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(5)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 1 => d
      }

      result2 must_== Set(
        Vector(SDecimal(0), SDecimal(0), SDecimal(0), SDecimal(0), SDecimal(1)),
        Vector(SDecimal(0), SDecimal(0), SDecimal(0), SDecimal(1), SDecimal(0)),
        Vector(SDecimal(0), SDecimal(0), SDecimal(1), SDecimal(0), SDecimal(0)),
        Vector(SDecimal(0), SDecimal(1), SDecimal(0), SDecimal(0), SDecimal(0)),
        Vector(SDecimal(1), SDecimal(0), SDecimal(0), SDecimal(0), SDecimal(0)))
    }

    "assign dummy variables to loaded dataset across slices" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Dummy,
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val expected = Vector.tabulate(22) { i =>
        Vector.fill(22)(SDecimal(0)).updated(i, SDecimal(1))
      }.toSet

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 1 => d
      }

      result2 must_== expected
    }

    "compute rank" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Rank,
        dag.AbsoluteLoad(Const(CString("/hom/numbers6"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(0,2,3,4,7,8)
    }

    "compute rank, denseRank, indexedRank" in {
      val line = Line(1, 1, "")

      val data = dag.AbsoluteLoad(Const(CString("/het/numbersDoubleLong"))(line))(line)

      def wrapper(d: DepGraph, name: String) =
        Join(WrapObject, Cross(None), Const(CString(name))(line), d)(line)

      def morpher(rank: Morphism1, name: String) =
        wrapper(dag.Morph1(rank, data)(line), name)

      def joiner(lhs: DepGraph, rhs: DepGraph): DepGraph =
        Join(JoinObject, IdentitySort, lhs, rhs)(line)

      val input = List(
        morpher(Rank, "rank"),
        morpher(DenseRank, "denseRank"),
        morpher(IndexedRank, "indexedRank"),
        wrapper(data, "point")
      ).reduceLeft(joiner)

      // sort a tuple by its first (Long) field
      val ordering = scala.math.Ordering.by[(SValue, _), SValue](_._1)

      // this is ugly, but so is the structure coming out of testEval :/
      val result: List[Map[String, SValue]] = testEval(input).toList.collect {
        case (Vector(k), SObject(v)) => (k, v)
      }.sorted(ordering).map(_._2)

      val expected = List(
        Map("indexedRank" -> SDecimal(0), "denseRank" -> SDecimal(0), "rank" -> SDecimal(0), "point" -> SDecimal(-30.2)),
        Map("indexedRank" -> SDecimal(1), "denseRank" -> SDecimal(0), "rank" -> SDecimal(0), "point" -> SDecimal(-30.2)),
        Map("indexedRank" -> SDecimal(2), "denseRank" -> SDecimal(1), "rank" -> SDecimal(2), "point" -> SDecimal(-2)),
        Map("indexedRank" -> SDecimal(3), "denseRank" -> SDecimal(1), "rank" -> SDecimal(2), "point" -> SDecimal(-2)),
        Map("indexedRank" -> SDecimal(4), "denseRank" -> SDecimal(2), "rank" -> SDecimal(4), "point" -> SDecimal(0)),
        Map("indexedRank" -> SDecimal(5), "denseRank" -> SDecimal(3), "rank" -> SDecimal(5), "point" -> SDecimal(10.1)),
        Map("indexedRank" -> SDecimal(6), "denseRank" -> SDecimal(4), "rank" -> SDecimal(6), "point" -> SDecimal(12.6)),
        Map("indexedRank" -> SDecimal(7), "denseRank" -> SDecimal(5), "rank" -> SDecimal(7), "point" -> SDecimal(15)),
        Map("indexedRank" -> SDecimal(8), "denseRank" -> SDecimal(5), "rank" -> SDecimal(7), "point" -> SDecimal(15)),
        Map("indexedRank" -> SDecimal(9), "denseRank" -> SDecimal(5), "rank" -> SDecimal(7), "point" -> SDecimal(15)),
        Map("indexedRank" -> SDecimal(10), "denseRank" -> SDecimal(5), "rank" -> SDecimal(7), "point" -> SDecimal(15)),
        Map("indexedRank" -> SDecimal(11), "denseRank" -> SDecimal(6), "rank" -> SDecimal(11), "point" -> SDecimal(30)),
        Map("indexedRank" -> SDecimal(12), "denseRank" -> SDecimal(7), "rank" -> SDecimal(12), "point" -> SDecimal(40.3)),
        Map("indexedRank" -> SDecimal(13), "denseRank" -> SDecimal(7), "rank" -> SDecimal(12), "point" -> SDecimal(40.3)),
        Map("indexedRank" -> SDecimal(14), "denseRank" -> SDecimal(8), "rank" -> SDecimal(14), "point" -> SDecimal(50))
      )

      result must_== expected
    }

    "compute rank, denseRank, indexedRank on heterogenous numbers" in {
      val line = Line(1, 1, "")

      val data = dag.AbsoluteLoad(Const(CString("/het/numbersDoubleLong"))(line))(line)

      def wrapper(d: DepGraph, name: String) =
        Join(WrapObject, Cross(None), Const(CString(name))(line), d)(line)

      def morpher(rank: Morphism1, name: String) =
        wrapper(dag.Morph1(rank, data)(line), name)

      def joiner(lhs: DepGraph, rhs: DepGraph): DepGraph =
        Join(JoinObject, IdentitySort, lhs, rhs)(line)

      val input = List(
        morpher(Rank, "rank"),
        morpher(DenseRank, "denseRank"),
        morpher(IndexedRank, "indexedRank"),
        wrapper(data, "point")
      ).reduceLeft(joiner)

      // sort a tuple by its first (Long) field
      val ordering = scala.math.Ordering.by[(SValue, _), SValue](_._1)

      // this is ugly, but so is the structure coming out of testEval :/
      val result: List[Map[String, SValue]] = testEval(input).toList.collect {
        case (Vector(k), SObject(v)) => (k, v)
      }.sorted(ordering).map(_._2)

      val expected = List(
        Map("indexedRank" -> SDecimal(0), "denseRank" -> SDecimal(0), "rank" -> SDecimal(0), "point" -> SDecimal(-30.2)),
        Map("indexedRank" -> SDecimal(1), "denseRank" -> SDecimal(0), "rank" -> SDecimal(0), "point" -> SDecimal(-30.2)),
        Map("indexedRank" -> SDecimal(2), "denseRank" -> SDecimal(1), "rank" -> SDecimal(2), "point" -> SDecimal(-2)),
        Map("indexedRank" -> SDecimal(3), "denseRank" -> SDecimal(1), "rank" -> SDecimal(2), "point" -> SDecimal(-2)),
        Map("indexedRank" -> SDecimal(4), "denseRank" -> SDecimal(2), "rank" -> SDecimal(4), "point" -> SDecimal(0)),
        Map("indexedRank" -> SDecimal(5), "denseRank" -> SDecimal(3), "rank" -> SDecimal(5), "point" -> SDecimal(10.1)),
        Map("indexedRank" -> SDecimal(6), "denseRank" -> SDecimal(4), "rank" -> SDecimal(6), "point" -> SDecimal(12.6)),
        Map("indexedRank" -> SDecimal(7), "denseRank" -> SDecimal(5), "rank" -> SDecimal(7), "point" -> SDecimal(15)),
        Map("indexedRank" -> SDecimal(8), "denseRank" -> SDecimal(5), "rank" -> SDecimal(7), "point" -> SDecimal(15)),
        Map("indexedRank" -> SDecimal(9), "denseRank" -> SDecimal(5), "rank" -> SDecimal(7), "point" -> SDecimal(15)),
        Map("indexedRank" -> SDecimal(10), "denseRank" -> SDecimal(5), "rank" -> SDecimal(7), "point" -> SDecimal(15)),
        Map("indexedRank" -> SDecimal(11), "denseRank" -> SDecimal(6), "rank" -> SDecimal(11), "point" -> SDecimal(30)),
        Map("indexedRank" -> SDecimal(12), "denseRank" -> SDecimal(7), "rank" -> SDecimal(12), "point" -> SDecimal(40.3)),
        Map("indexedRank" -> SDecimal(13), "denseRank" -> SDecimal(7), "rank" -> SDecimal(12), "point" -> SDecimal(40.3)),
        Map("indexedRank" -> SDecimal(14), "denseRank" -> SDecimal(8), "rank" -> SDecimal(14), "point" -> SDecimal(50))
      )

      result must_== expected
    }

    "compute indexedRank" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(IndexedRank,
        dag.AbsoluteLoad(Const(CString("/hom/numbers6"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(0,1,2,3,4,5,6,7,8,9)
    }

    "compute rank within a filter" in {
      val line = Line(1, 1, "")

      val numbers = dag.AbsoluteLoad(Const(CString("/hom/numbers6"))(line))(line)

      val input = Filter(IdentitySort,
        numbers,
        Join(Eq, Cross(None),
          dag.Morph1(Rank, numbers)(line),
          Const(CLong(4))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(3)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(11)
    }

    "compute rank resulting in a boolean set" in {
      val line = Line(1, 1, "")

      val input = Join(Eq, Cross(None),
        dag.Morph1(Rank,
          dag.AbsoluteLoad(Const(CString("/hom/numbers6"))(line))(line))(line),
        Const(CLong(4))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val (tr, fls) = result partition {
        case (ids, STrue) if ids.length == 1 => true
        case (ids, SFalse) if ids.length == 1 => false
        case _ => false
      }

      tr.size mustEqual 3
      fls.size mustEqual 7
    }

    "compute rank within a join" in {
      val line = Line(1, 1, "")

      val input = Join(Add, Cross(None),
        dag.Morph1(Rank,
          dag.AbsoluteLoad(Const(CString("/hom/numbers6"))(line))(line))(line),
        Const(CLong(2))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(2,4,5,6,9,10)
    }

    "compute denseRank" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(DenseRank,
        dag.AbsoluteLoad(Const(CString("/hom/numbers6"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(0,1,2,3,4,5)
    }

    "compute denseRank within a filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.AbsoluteLoad(Const(CString("/hom/numbers6"))(line))(line),
        Join(Eq, Cross(None),
          dag.Morph1(DenseRank,
            dag.AbsoluteLoad(Const(CString("/hom/numbers6"))(line))(line))(line),
          Const(CLong(3))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(3)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(11)
    }

    "compute denseRank within a join" in {
      val line = Line(1, 1, "")

      val input = Join(Add, Cross(None),
        dag.Morph1(DenseRank,
          dag.AbsoluteLoad(Const(CString("/hom/numbers6"))(line))(line))(line),
        Const(CLong(2))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(2,3,4,5,6,7)
    }

    "compute linear correlation" in {
      val line = Line(1, 1, "")
      val heightWeight = dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line)

      val input = dag.Morph2(LinearCorrelation,
        Join(DerefObject, Cross(None),
          heightWeight,
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          heightWeight,
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 0.9998746737089123)
      }

      result2.toSet must_== Set(true)
    }

    "compute covariance" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(Covariance,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 400.08)
      }

      result2.toSet must_== Set(true)
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LinearRegression,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 0.6862906545903664
          val bool2 = yint.toDouble ~= 67.54013997529848
          bool1 -> bool2
        }
      }

      result2.toList must_== List(true -> true)
    }

    "compute the correct coefficients in a simple log regression" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LogarithmicRegression,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 38.8678597674246945
          val bool2 = yint.toDouble ~= -46.97865418113420686
          bool1 -> bool2
        }
      }

      result2.toList must_== List(true -> true)
    }
  }

  "heterogenous sets" should {
    "median" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Median,
        dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }

      result2.toSet must_== Set(13)
    }

    "mode in the case there is only one" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Mode,
        dag.AbsoluteLoad(Const(CString("/het/numbers2"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2.toSet must_== Set(Vector(SDecimal(1)))
    }.pendingUntilFixed

    "mode in the case there is more than one" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Mode,
        dag.AbsoluteLoad(Const(CString("/het/random"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2.toSet must_== Set(Vector(SDecimal(4), SString("a")))
    }.pendingUntilFixed

    "compute rank" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Rank,
        dag.AbsoluteLoad(Const(CString("/het/numbers6"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(18)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(0,1,2,3,4,5,7,8,9,12,13,15,16,17)
    }

    "compute indexedRank" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(IndexedRank,
        dag.AbsoluteLoad(Const(CString("/het/numbers6"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(18)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17)
    }

    "compute rank heterogenously" in {
      val line = Line(1, 1, "")
      val data = dag.AbsoluteLoad(Const(CString("/het/numbers6"))(line))(line)
      val input = Join(
        JoinArray,
        IdentitySort,
        Operate(WrapArray, data)(line),
        Operate(WrapArray, dag.Morph1(Rank, data)(line))(line)
      )(line)

      val result = testEval(input).map(_._2)

      def arr(ns: Long*) = SArray(Vector(ns.map(n => SDecimal(n)):_*))
      def sv(n: Long) = SDecimal(n)
      def tpl(v: SValue, rank: SValue) = SArray(Vector(v, rank))

      val expected = Set(
        tpl(SObject(Map("foo" -> SString("bar"))), sv(0)),
        tpl(arr(9, 10, 11), sv(1)),
        tpl(SFalse, sv(2)),
        tpl(STrue, sv(3)),
        tpl(SString("alissa"), sv(4)),
        tpl(sv(-10), sv(5)),
        tpl(sv(-10), sv(5)),
        tpl(sv(0), sv(7)),
        tpl(sv(5), sv(8)),
        tpl(sv(11), sv(9)),
        tpl(sv(11), sv(9)),
        tpl(sv(11), sv(9)),
        tpl(sv(12), sv(12)),
        tpl(sv(34), sv(13)),
        tpl(sv(34), sv(13)),
        tpl(SObject(Map()), sv(15)),
        tpl(arr(), sv(16)),
        tpl(SNull, sv(17))
      )

      result must_== expected
    }

    "compute rank within an equals filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.AbsoluteLoad(Const(CString("/het/numbers6"))(line))(line),
        Join(Eq, Cross(None),
          dag.Morph1(Rank, dag.AbsoluteLoad(Const(CString("/het/numbers6"))(line))(line))(line),
          Const(CLong(13))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(2)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(34)
    }

    "compute rank within another equals filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.AbsoluteLoad(Const(CString("/het/numbers6"))(line))(line),
        Join(Eq, Cross(None),
          dag.Morph1(Rank,
            dag.AbsoluteLoad(Const(CString("/het/numbers6"))(line))(line))(line),
          Const(CLong(5))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(2)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(-10)

    }

    "compute rank within a less-than filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.AbsoluteLoad(Const(CString("/het/numbers6"))(line))(line),
        Join(LtEq, Cross(None),
          dag.Morph1(Rank,
            dag.AbsoluteLoad(Const(CString("/het/numbers6"))(line))(line))(line),
          Const(CLong(10))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(12)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(-10,0,5,11)
    }

    "compute rank within a join" in {
      val line = Line(1, 1, "")

      val input = Join(Add, Cross(None),
        dag.Morph1(Rank,
          dag.AbsoluteLoad(Const(CString("/het/numbers6"))(line))(line))(line),
        Const(CLong(2))(line))(line)

      val result = testEval(input)

      result must haveSize(18)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(5, 10, 14, 6, 9, 2, 17, 7, 3, 18, 11, 19, 4, 15)
    }

    "compute denseRank" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(DenseRank,
        dag.AbsoluteLoad(Const(CString("/het/numbers6"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(18)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(0,1,2,3,4,5,6,7,8,9,10,11,12,13)
    }

    "compute denseRank within an equals filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.AbsoluteLoad(Const(CString("/het/numbers6"))(line))(line),
        Join(Eq, Cross(None),
          dag.Morph1(DenseRank,
            dag.AbsoluteLoad(Const(CString("/het/numbers6"))(line))(line))(line),
          Const(CLong(10))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(2)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(34)
    }

    "compute denseRank within a less-than filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.AbsoluteLoad(Const(CString("/het/numbers6"))(line))(line),
        Join(LtEq, Cross(None),
          dag.Morph1(DenseRank,
            dag.AbsoluteLoad(Const(CString("/het/numbers6"))(line))(line))(line),
          Const(CLong(9))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(13)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(-10,0,5,11,12)
    }

    "compute denseRank within a join" in {
      val line = Line(1, 1, "")

      val input = Join(Add, Cross(None),
        dag.Morph1(DenseRank,
          dag.AbsoluteLoad(Const(CString("/het/numbers6"))(line))(line))(line),
        Const(CLong(2))(line))(line)

      val result = testEval(input)

      result must haveSize(18)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(5, 10, 14, 6, 9, 13, 2, 12, 7, 3, 11, 8, 4, 15)
    }

    "compute linear correlation" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LinearCorrelation,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("het/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("het/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 0.9998746737089123)
      }

      result2.toSet must_== Set(true)
    }

    "compute covariance" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(Covariance,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("het/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("het/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 400.08)
      }

      result2.toSet must_== Set(true)
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LinearRegression,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("het/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("het/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint)  = fields("intercept")
          val bool1           = slope.toDouble ~= 0.6862906545903664
          val bool2           = yint.toDouble ~= 67.54013997529848

          bool1 -> bool2
        }
      }

      result2.toList must_== List(true -> true)
    }

    "compute the correct coefficients in a simple log regression" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LogarithmicRegression,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("het/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("het/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 38.8678597674246945
          val bool2 = yint.toDouble ~= -46.97865418113420686
          bool1 -> bool2
        }
      }

      result2.toList must_== List(true -> true)
    }
  }


  "for homogenous sets, in a cross, the appropriate stats function" should {
    "compute linear correlation" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LinearCorrelation,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 0.9998746737089123)
      }

      result2.toSet must_== Set(true)
    }

    "compute covariance" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(Covariance,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 400.08)
      }

      result2.toSet must_== Set(true)
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LinearRegression,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 0.6862906545903664
          val bool2 = yint.toDouble ~= 67.54013997529848
          bool1 -> bool2
        }
      }

      result2.toList must_== List(true -> true)
    }

    "compute the correct coefficients in a simple log regression" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LogarithmicRegression,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 38.8678597674246945
          val bool2 = yint.toDouble ~= -46.97865418113420686
          bool1 -> bool2
        }
      }

      result2.toList must_== List(true -> true)
    }
  }

  "for the same homogenous set, the appropriate stats function" should {
    "compute linear correlation" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LinearCorrelation,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 1)
      }

      result2.toSet must_== Set(true) //todo test this answer to a certain level of accuracy
    }

    "compute covariance" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(Covariance,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 582.96)
      }

      result2.toSet must_== Set(true)
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LinearRegression,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 1
          val bool2 = yint.toDouble ~= 0
          bool1 -> bool2
        }
      }

      result2.toList must_== List(true -> true)
    }

    "compute the correct coefficients in a simple log regression" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LogarithmicRegression,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 56.591540847739639
          val bool2 = yint.toDouble ~= -166.690263558904667
          bool1 -> bool2
        }
      }

      result2.toList must_== List(true -> true)
    }
  }


  "for a homogenous set and a value, the appropriate stats function" should {
    "compute linear correlation" in {
      "with value on the right" in {
        val line = Line(1, 1, "")

        val input = dag.Morph2(LinearCorrelation,
          Join(DerefObject, Cross(None),
            dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
            Const(CString("height"))(line))(line),
          Const(CLong(5))(line))(line)

        val result = testEval(input)

        result must haveSize(0)
      }

      "with value on the left" in {
        val line = Line(1, 1, "")

        val input = dag.Morph2(LinearCorrelation,
          Const(CLong(5))(line),
          Join(DerefObject, Cross(None),
            dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
            Const(CString("height"))(line))(line))(line)

        val result = testEval(input)

        result must haveSize(0)
      }
    }

    "compute covariance" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(Covariance,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Const(CLong(5))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 0)
      }

      result2.toSet must_== Set(true)
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LinearRegression,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
          Const(CString("height"))(line))(line),
        Const(CLong(5))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 0
          val bool2 = yint.toDouble ~= 5
          bool1 -> bool2
        }
      }

      result2.toList must_== List(true -> true)
    }

    "compute the correct coefficients in a simple log regression" >> {
      "with a positive constant y-value" >> {
        val line = Line(1, 1, "")

        val input = dag.Morph2(LogarithmicRegression,
          Join(DerefObject, Cross(None),
            dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
            Const(CString("height"))(line))(line),
          Const(CLong(5))(line))(line)

        val result = testEval(input)

        result must haveSize(1)

        val result2 = result collect {
          case (ids, SObject(fields)) if ids.length == 0 => {
            val SDecimal(slope) = fields("slope")
            val SDecimal(yint) = fields("intercept")
            val bool1 = slope.toDouble ~= 0
            val bool2 = yint.toDouble ~= 5
            bool1 -> bool2
          }
        }

        result2.toList must_== List(true -> true)
      }

      "with a negative constant x-value" >> {
        val line = Line(1, 1, "")

        val input = dag.Morph2(LogarithmicRegression,
          Operate(Neg,
            Const(CLong(5))(line))(line),
          Join(DerefObject, Cross(None),
            dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line),
            Const(CString("height"))(line))(line))(line)

        val result = testEval(input)

        val input2 = dag.AbsoluteLoad(Const(CString("hom/heightWeight"))(line))(line)

        val result2 = testEval(input2)

        result must haveSize(0)
        result2 must haveSize(5)

      }

      "with a negative x-value in one object" >> {
        val line = Line(1, 1, "")

        val input = dag.Morph2(LogarithmicRegression,
          Join(DerefObject, Cross(None),
            dag.AbsoluteLoad(Const(CString("hom/heightWeight_neg"))(line))(line),
            Const(CString("height"))(line))(line),
          Join(DerefObject, Cross(None),
            dag.AbsoluteLoad(Const(CString("hom/heightWeight_neg"))(line))(line),
            Const(CString("weight"))(line))(line))(line)

        val result = testEval(input)

        result must haveSize(1)

        val result2 = result collect {
          case (ids, SObject(fields)) if ids.length == 0 => {
            val SDecimal(slope) = fields("slope")
            val SDecimal(yint) = fields("intercept")
            val bool1 = slope.toDouble ~= 38.8678597674246945
            val bool2 = yint.toDouble ~= -46.97865418113420686
            bool1 -> bool2
          }
        }

        result2.toList must_== List(true -> true)
      }
    }
  }

  "homogenous sets across two slice boundaries (22 elements)" should {
    "median with odd number of elements" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Median,
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }

      result2.toSet must_== Set(-1.5)
    }

    "median with even number of elements" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Median,
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }

      result2.toSet must_== Set(-1.5)
    }

    "median with singleton" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Median,
        Const(CLong(42))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }

      result2.toSet must_== Set(42)
    }

    "mode" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Mode,
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2.toSet must_== Set(Vector(SDecimal(1)))
    }.pendingUntilFixed

    "mode with a singleton" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Mode,
        Const(CLong(42))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2.toSet must_== Set(Vector(SDecimal(42)))
    }.pendingUntilFixed

    "mode where each value appears exactly once" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Mode,
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2.toSet must_== Set(Vector(SDecimal(1), SDecimal(12), SDecimal(13), SDecimal(42), SDecimal(77)))
    }.pendingUntilFixed

    "compute rank" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Rank,
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(0, 5, 10, 14, 20, 1, 13, 12, 7, 3, 18, 11, 19, 4, 15)
    }

    "compute rank within a filter" in {
      val line = Line(1, 1, "")

      val numbers = dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line)

      val input = Filter(IdentitySort,
        numbers,
        Join(Eq, Cross(None),
          dag.Morph1(Rank, numbers)(line),
          Const(CLong(4))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(-9)
    }

    "compute rank resulting in a boolean set" in {
      val line = Line(1, 1, "")

      val input = Join(Eq, Cross(None),
        dag.Morph1(Rank,
          dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line),
        Const(CLong(4))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val (tr, fls) = result partition {
        case (ids, STrue) if ids.length == 1 => true
        case (ids, SFalse) if ids.length == 1 => false
        case _ => false
      }

      tr.size mustEqual 1
      fls.size mustEqual 21
    }

    "compute rank within a join" in {
      val line = Line(1, 1, "")

      val input = Join(Add, Cross(None),
        dag.Morph1(Rank,
          dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line),
        Const(CLong(3))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(10,14,6,21,13,17,22,7,3,18,16,23,8,4,15)
    }

    "compute denseRank" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(DenseRank,
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(5,10,14,1,6,9,13,2,12,7,3,11,8,4,0)
    }

    "compute denseRank within a filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line),
        Join(Eq, Cross(None),
          dag.Morph1(DenseRank,
            dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line),
          Const(CLong(3))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(-9)
    }

    "compute denseRank within a join" in {
      val line = Line(1, 1, "")

      val input = Join(Add, Cross(None),
        dag.Morph1(DenseRank,
          dag.AbsoluteLoad(Const(CString("/hom/numbersAcrossSlices"))(line))(line))(line),
        Const(CLong(3))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(5,10,14,6,9,13,17,12,7,3,16,11,8,4,15)
    }

    "compute covariance" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(Covariance,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeightAcrossSlices"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeightAcrossSlices"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 301.5)
      }

      result2.toSet must_== Set(true)
    }

    "compute linear correlation" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LinearCorrelation,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeightAcrossSlices"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeightAcrossSlices"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 0.2832061115667535364)
      }

      result2.toSet must_== Set(true)
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LinearRegression,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeightAcrossSlices"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeightAcrossSlices"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 0.551488261704282626112
          val bool2 = yint.toDouble ~= 96.337568593067376154
          bool1 -> bool2
        }
      }

      result2.toList must_== List(true -> true)
    }

    "compute the correct coefficients in a simple log regression" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LogarithmicRegression,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeightAcrossSlices"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("hom/heightWeightAcrossSlices"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 35.885416368041469881
          val bool2 = yint.toDouble ~= -14.930463966129221
          bool1 -> bool2
        }
      }

      result2.toList must_== List(true -> true)
    }

    "simple exponential smoothing" in {
      val line = Line(1, 1, "")
      val data = dag.AbsoluteLoad(Const(CString("hom/heightWeightAcrossSlices"))(line))(line)
      def const[A: CValueType](a: A): Const = Const(CValueType[A](a))(line)

      val left = Join(JoinObject, IdentitySort,
          Join(WrapObject, Cross(None), const("smooth"),
            Join(DerefObject, Cross(None), data, const("weight"))(line))(line),
          Join(WrapObject, Cross(None), const("by"),
            Join(DerefObject, Cross(None), data, const("height"))(line))(line))(line)
      val right = const(BigDecimal(0.5))
      val input = dag.Morph2(SimpleExponentialSmoothing, left, right)(line)

      val result = testEval(input)

      result must haveAllElementsLike {
        case (ids, _) => ids.size must_== 1
        case _        => ko
      }

      result must haveSize(22)

      val values = result collect { case (_, SDecimal(x)) => x }
      values must contain(BigDecimal(88), BigDecimal(104), BigDecimal(131.5))
    }

    "double exponential smoothing" in {
      val line = Line(1, 1, "")
      val data = dag.AbsoluteLoad(Const(CString("hom/heightWeightAcrossSlices"))(line))(line)
      def const[A: CValueType](a: A): Const = Const(CValueType[A](a))(line)

      val left = Join(JoinObject, IdentitySort,
          Join(WrapObject, Cross(None), const("smooth"),
            Join(DerefObject, Cross(None), data, const("weight"))(line))(line),
          Join(WrapObject, Cross(None), const("by"),
            Join(DerefObject, Cross(None), data, const("height"))(line))(line))(line)
      val right = Join(JoinObject, Cross(None),
        Join(WrapObject, Cross(None), const("alpha"), const(BigDecimal(0.7)))(line),
        Join(WrapObject, Cross(None), const("beta"), const(BigDecimal(0.5)))(line))(line)
      val input = dag.Morph2(DoubleExponentialSmoothing, left, right)(line)

      val result = testEval(input)

      result must haveAllElementsLike {
        case (ids, _) => ids.size must_== 1
        case _ => ko
      }

      result must haveSize(22)

      val values = result collect { case (_, SDecimal(x)) => x }
      values must contain(BigDecimal(88), BigDecimal(120), BigDecimal(156.9))
    }
  }

  "heterogenous sets across two slice boundaries (22 elements)" should {
    "median" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Median,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d
      }

      result2.toSet must_== Set(1)
    }

    "mode in the case there is only one" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Mode,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2.toSet must_== Set(Vector(SDecimal(1)))
    }.pendingUntilFixed

    "mode in the case there is more than one" >> {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Mode,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SArray(d)) if ids.length == 0 => d
      }

      result2.toSet must_== Set(Vector(SDecimal(4), SString("a")))
    }.pendingUntilFixed

    "compute rank" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(Rank,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(0, 10, 20, 1, 6, 9, 13, 2, 17, 7, 3, 18, 16, 11, 8, 19, 4, 15)
    }

    "compute rank within an equals filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line),
        Join(Eq, Cross(None),
          dag.Morph1(Rank,
            dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line),
          Const(CLong(17))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(12)
    }

    "compute rank within another equals filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line),
        Join(Eq, Cross(None),
          dag.Morph1(Rank,
            dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line),
          Const(CLong(9))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(-3)
    }

    "compute rank within a less-than filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line),
        Join(LtEq, Cross(None),
          dag.Morph1(Rank,
            dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line),
          Const(CLong(10))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(11)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(-1,-3)
    }

    "compute rank within a join" in {
      val line = Line(1, 1, "")

      val input = Join(Add, Cross(None),
        dag.Morph1(Rank,
          dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line),
        Const(CLong(3))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(5, 10, 14, 20, 6, 21, 9, 13, 22, 12, 7, 3, 18, 16, 11, 23, 19, 4)
    }

    "compute denseRank" in {
      val line = Line(1, 1, "")

      val input = dag.Morph1(DenseRank,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(0, 5, 10, 14, 1, 6, 9, 13, 2, 17, 12, 7, 3, 16, 11, 8, 4, 15)
    }

    "compute denseRank within an equals filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line),
        Join(Eq, Cross(None),
          dag.Morph1(DenseRank,
            dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line),
          Const(CLong(13))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(5)
    }

    "compute denseRank within a less-than filter" in {
      val line = Line(1, 1, "")

      val input = Filter(IdentitySort,
        dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line),
        Join(LtEq, Cross(None),
          dag.Morph1(DenseRank,
            dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line),
          Const(CLong(10))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(13)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(0,-3,-1)
    }

    "compute denseRank within a join" in {
      val line = Line(1, 1, "")

      val input = Join(Add, Cross(None),
        dag.Morph1(DenseRank,
          dag.AbsoluteLoad(Const(CString("/het/numbersAcrossSlices"))(line))(line))(line),
        Const(CLong(3))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2.toSet must_== Set(5, 10, 14, 20, 6, 9, 13, 17, 12, 7, 3, 18, 16, 11, 8, 19, 4, 15)
    }

    "compute covariance" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(Covariance,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("het/heightWeightAcrossSlices"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("het/heightWeightAcrossSlices"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 874.2741666666666)
      }

      result2.toSet must_== Set(true)
    }

    "compute correlation of 0 when datasets are uncorrelated" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LinearCorrelation,
        Join(DerefArray, Cross(None),
          dag.AbsoluteLoad(Const(CString("uncorrelated"))(line))(line),
          Const(CLong(0))(line))(line),
        Join(DerefArray, Cross(None),
          dag.AbsoluteLoad(Const(CString("uncorrelated"))(line))(line),
          Const(CLong(1))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)
      result must haveAllElementsLike { case (ids, SDecimal(d)) if ids.length == 0 =>
        d.toDouble must_== 0.0
      }
    }

    "compute linear correlation" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LinearCorrelation,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("het/heightWeightAcrossSlices"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("het/heightWeightAcrossSlices"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 0 => (d.toDouble ~= 0.7835742008825)
      }

      result2.toSet must_== Set(true)
    }

    "compute the correct coefficients in a simple linear regression" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LinearRegression,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("het/heightWeightAcrossSlices"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("het/heightWeightAcrossSlices"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 1.454654738762821849
          val bool2 = yint.toDouble ~= 27.0157291837095112508
          bool1 -> bool2
        }
      }

      result2.toList must_== List(true -> true)
    }

    "compute the correct coefficients in a simple log regression" in {
      val line = Line(1, 1, "")

      val input = dag.Morph2(LogarithmicRegression,
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("het/heightWeightAcrossSlices"))(line))(line),
          Const(CString("height"))(line))(line),
        Join(DerefObject, Cross(None),
          dag.AbsoluteLoad(Const(CString("het/heightWeightAcrossSlices"))(line))(line),
          Const(CString("weight"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (ids, SObject(fields)) if ids.length == 0 => {
          val SDecimal(slope) = fields("slope")
          val SDecimal(yint) = fields("intercept")
          val bool1 = slope.toDouble ~= 84.092713766496588959
          val bool2 = yint.toDouble ~= -220.42413606579986360
          bool1 -> bool2
        }
      }

      result2.toList must_== List(true -> true)
    }

    "simple exponential smoothing" in {
      val line = Line(1, 1, "")
      val data = dag.AbsoluteLoad(Const(CString("het/heightWeightAcrossSlices"))(line))(line)
      def const[A: CValueType](a: A): Const = Const(CValueType[A](a))(line)

      val left = Join(JoinObject, IdentitySort,
          Join(WrapObject, Cross(None), const("smooth"),
            Join(DerefObject, Cross(None), data, const("weight"))(line))(line),
          Join(WrapObject, Cross(None), const("by"),
            Join(DerefObject, Cross(None), data, const("height"))(line))(line))(line)
      val right = const(BigDecimal(0.7))
      val input = dag.Morph2(SimpleExponentialSmoothing, left, right)(line)

      val result = testEval(input)

      result must haveSize(17)

      result must haveAllElementsLike {
        case (ids, _) => ids.size must_== 1
        case _ => ko
      }

      val values = result collect { case (_, SDecimal(x)) => x }
      values must contain(BigDecimal(131), BigDecimal("40.0777"), BigDecimal("103.72331"))
    }

    "double exponential smoothing" in {
      val line = Line(1, 1, "")
      val data = dag.AbsoluteLoad(Const(CString("het/heightWeightAcrossSlices"))(line))(line)
      def const[A: CValueType](a: A): Const = Const(CValueType[A](a))(line)

      val left = Join(JoinObject, IdentitySort,
          Join(WrapObject, Cross(None), const("smooth"),
            Join(DerefObject, Cross(None), data, const("weight"))(line))(line),
          Join(WrapObject, Cross(None), const("by"),
            Join(DerefObject, Cross(None), data, const("height"))(line))(line))(line)
      val right = Join(JoinObject, Cross(None),
        Join(WrapObject, Cross(None), const("alpha"), const(BigDecimal(0.7)))(line),
        Join(WrapObject, Cross(None), const("beta"), const(BigDecimal(0.5)))(line))(line)
      val input = dag.Morph2(DoubleExponentialSmoothing, left, right)(line)

      val result = testEval(input)

      result must haveSize(17)

      result must haveAllElementsLike {
        case (ids, _) => ids.size must_== 1
        case _ => ko
      }

      val values = result collect { case (_, SDecimal(x)) => x }
      values must contain(BigDecimal(131), BigDecimal("1.111"), BigDecimal("53.06660"))
    }
  }
}

object StatsLibSpecs extends StatsLibSpecs[Need]
