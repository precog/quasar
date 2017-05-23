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

import quasar.blueeyes._, json._
import quasar.precog.common._
import quasar.yggdrasil._
import quasar.precog.util.Identifier
import scalaz._

trait TypeInferencerSpecs[M[+_]] extends EvaluatorSpecification[M]
    with LongIdMemoryDatasetConsumer[M] {

  import dag._
  import instructions.{
    Line,
    BuiltInFunction2Op,
    Add, Neg,
    DerefArray, DerefObject,
    ArraySwap, WrapObject, JoinObject
  }
  import quasar.yggdrasil.bytecode._
  import library._

  def flattenType(jtpe : JType) : Map[JPath, Set[CType]] = {
    def flattenAux(jtpe : JType) : Set[(JPath, Option[CType])] = jtpe match {
      case p : JPrimitiveType => Schema.ctypes(p).map(tpe => (NoJPath, Some(tpe)))

      case JArrayFixedT(elems) =>
        for((i, jtpe) <- elems.toSet; (path, ctpes) <- flattenAux(jtpe)) yield (JPathIndex(i) \ path, ctpes)

      case JObjectFixedT(fields) =>
        for((field, jtpe) <- fields.toSet; (path, ctpes) <- flattenAux(jtpe)) yield (JPathField(field) \ path, ctpes)

      case JUnionT(left, right) => flattenAux(left) ++ flattenAux(right)

      case u @ (JArrayUnfixedT | JObjectUnfixedT) => Set((NoJPath, None))

      case x => sys.error("Unexpected: " + x)
    }

    flattenAux(jtpe).groupBy(_._1).mapValues(_.flatMap(_._2))
  }

  def extractLoads(graph : DepGraph): Map[String, Map[JPath, Set[CType]]] = {

    def merge(left: Map[String, Map[JPath, Set[CType]]], right: Map[String, Map[JPath, Set[CType]]]): Map[String, Map[JPath, Set[CType]]] = {
      def mergeAux(left: Map[JPath, Set[CType]], right: Map[JPath, Set[CType]]): Map[JPath, Set[CType]] = {
        left ++ right.map { case (path, ctpes) => path -> (ctpes ++ left.getOrElse(path, Set())) }
      }
      left ++ right.map { case (file, jtpes) => file -> mergeAux(jtpes, left.getOrElse(file, Map())) }
    }

    def extractSpecLoads(spec: BucketSpec):  Map[String, Map[JPath, Set[CType]]] = spec match {
      case UnionBucketSpec(left, right) =>
        merge(extractSpecLoads(left), extractSpecLoads(right))

      case IntersectBucketSpec(left, right) =>
        merge(extractSpecLoads(left), extractSpecLoads(right))

      case Group(id, target, child) =>
        merge(extractLoads(target), extractSpecLoads(child))

      case UnfixedSolution(id, target) =>
        extractLoads(target)

      case Extra(target) =>
        extractLoads(target)
    }

    graph match {
      case _ : Root                                 => Map()
      case New(parent)                              => extractLoads(parent)
      case AbsoluteLoad(Const(CString(path)), jtpe) => Map(path -> flattenType(jtpe))
      case Operate(_, parent)                       => extractLoads(parent)
      case Reduce(_, parent)                        => extractLoads(parent)
      case Morph1(_, parent)                        => extractLoads(parent)
      case Morph2(_, left, right)                   => merge(extractLoads(left), extractLoads(right))
      case Join(_, joinSort, left, right)           => merge(extractLoads(left), extractLoads(right))
      case Filter(_, target, boolean)               => merge(extractLoads(target), extractLoads(boolean))
      case AddSortKey(parent, _, _, _)              => extractLoads(parent)
      case Memoize(parent, _)                       => extractLoads(parent)
      case Distinct(parent)                         => extractLoads(parent)
      case Split(spec, child, _)                    => merge(extractSpecLoads(spec), extractLoads(child))
      case _: SplitGroup | _: SplitParam            => Map()
      case x                                        => sys.error("Unexpected: " + x)
    }
  }

  val cLiterals = Set(CBoolean, CLong, CDouble, CNum, CString, CNull, CDate, CPeriod)

  "type inference" should {
    "propagate structure/type information through a trivial Join/DerefObject node" in {
      val line = Line(1, 1, "")

      val input =
        Join(DerefObject, Cross(None),
          AbsoluteLoad(Const(CString("/file"))(line))(line),
          Const(CString("column"))(line))(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> cLiterals)
      )

      result must_== expected
    }

    "propagate structure/type information through New nodes" in {
      val line = Line(1, 1, "")

      val input =
        Operate(Neg,
          New(
            Join(DerefObject, Cross(None),
              AbsoluteLoad(Const(CString("/file"))(line))(line),
              Const(CString("column"))(line))(line))(line))(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through Operate nodes" in {
      val line = Line(1, 1, "")

      val input =
        Operate(Neg,
          Join(DerefObject, Cross(None),
            AbsoluteLoad(Const(CString("/file"))(line))(line),
            Const(CString("column"))(line))(line))(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through Reduce nodes" in {
      val line = Line(1, 1, "")

      val input =
        Reduce(Mean,
          Join(DerefObject, Cross(None),
            AbsoluteLoad(Const(CString("/file"))(line))(line),
            Const(CString("column"))(line))(line))(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through Morph1 nodes" in {
      val line = Line(1, 1, "")

      val input =
        Morph1(Median,
          Join(DerefObject, Cross(None),
            AbsoluteLoad(Const(CString("/file"))(line))(line),
            Const(CString("column"))(line))(line))(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through Morph2 nodes" in {
      val line = Line(1, 1, "")

      val input =
        Morph2(Covariance,
          Join(DerefObject, Cross(None),
            AbsoluteLoad(Const(CString("/file0"))(line))(line),
            Const(CString("column0"))(line))(line),
          Join(DerefObject, Cross(None),
            AbsoluteLoad(Const(CString("/file1"))(line))(line),
            Const(CString("column1"))(line))(line))(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file0" -> Map(JPath("column0") -> Set(CLong, CDouble, CNum)),
        "/file1" -> Map(JPath("column1") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through DerefArray Join nodes" in {
      val line = Line(1, 1, "")

      val input =
        Operate(Neg,
          New(
            Join(DerefArray, Cross(None),
              AbsoluteLoad(Const(CString("/file"))(line))(line),
              Const(CLong(0))(line))(line))(line))(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath(0) -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through ArraySwap Join nodes" in {
      val line = Line(1, 1, "")

      val input =
        Join(ArraySwap, Cross(None),
          Join(DerefObject, Cross(None),
            AbsoluteLoad(Const(CString("/file0"))(line))(line),
            Const(CString("column0"))(line))(line),
          Join(DerefObject, Cross(None),
            AbsoluteLoad(Const(CString("/file1"))(line))(line),
            Const(CString("column1"))(line))(line))(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file0" -> Map(JPath("column0") -> Set[CType]()),
        "/file1" -> Map(JPath("column1") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through WrapObject Join nodes" in {
      val line = Line(1, 1, "")

      val input =
        Join(WrapObject, Cross(None),
          Join(DerefObject, Cross(None),
            AbsoluteLoad(Const(CString("/file0"))(line))(line),
            Const(CString("column0"))(line))(line),
          Join(DerefObject, Cross(None),
            AbsoluteLoad(Const(CString("/file1"))(line))(line),
            Const(CString("column1"))(line))(line))(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file0" -> Map(JPath("column0") -> Set(CString)),
        "/file1" -> Map(JPath("column1") -> cLiterals)
      )

      result must_== expected
    }

    "propagate structure/type information through Op2 Join nodes" in {
      val line = Line(1, 1, "")

      val input =
        Join(BuiltInFunction2Op(minOf), IdentitySort,
          Join(DerefObject, Cross(None),
            AbsoluteLoad(Const(CString("/file0"))(line))(line),
            Const(CString("column0"))(line))(line),
          Join(DerefObject, Cross(None),
            AbsoluteLoad(Const(CString("/file0"))(line))(line),
            Const(CString("column1"))(line))(line))(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file0" -> Map(
          JPath("column0") -> Set(CLong, CDouble, CNum),
          JPath("column1") -> Set(CLong, CDouble, CNum)
        )
      )

      result must_== expected
    }

    "propagate structure/type information through Filter nodes" in {
      val line = Line(1, 1, "")

      val input =
        Filter(IdentitySort,
          Join(DerefObject, Cross(None),
            AbsoluteLoad(Const(CString("/file0"))(line))(line),
            Const(CString("column0"))(line))(line),
          Join(DerefObject, Cross(None),
            AbsoluteLoad(Const(CString("/file1"))(line))(line),
            Const(CString("column1"))(line))(line))(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file0" -> Map(JPath("column0") -> cLiterals),
        "/file1" -> Map(JPath("column1") -> Set(CBoolean))
      )

      result must_== expected
    }

    "propagate structure/type information through AddSortKey nodes" in {
      val line = Line(1, 1, "")

      val input =
        Operate(Neg,
          AddSortKey(
            Join(DerefObject, Cross(None),
              AbsoluteLoad(Const(CString("/file"))(line))(line),
              Const(CString("column"))(line))(line),
            "foo", "bar", 23
          )
        )(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through Memoize nodes" in {
      val line = Line(1, 1, "")

      val input =
        Operate(Neg,
          Memoize(
            Join(DerefObject, Cross(None),
              AbsoluteLoad(Const(CString("/file"))(line))(line),
              Const(CString("column"))(line))(line),
            23
          )
        )(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through Distinct nodes" in {
      val line = Line(1, 1, "")

      val input =
        Operate(Neg,
          Distinct(
            Join(DerefObject, Cross(None),
              AbsoluteLoad(Const(CString("/file"))(line))(line),
              Const(CString("column"))(line))(line))(line))(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through Split nodes (1)" in {
      val line = Line(1, 1, "")

      def clicks = AbsoluteLoad(Const(CString("/file"))(line))(line)

      val id = new Identifier

      val input =
        Split(
          Group(
            1,
            clicks,
            UnfixedSolution(0,
              Join(DerefObject, Cross(None),
                clicks,
                Const(CString("column0"))(line))(line))),
          Join(Add, Cross(None),
            Join(DerefObject, Cross(None),
              SplitParam(0, id)(line),
              Const(CString("column1"))(line))(line),
            Join(DerefObject, Cross(None),
              SplitGroup(1, clicks.identities, id)(line),
              Const(CString("column2"))(line))(line))(line), id)(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(
          JPath("column0") -> cLiterals,
          JPath("column0.column1") -> Set(CLong, CDouble, CNum),
          JPath("column2") -> Set(CLong, CDouble, CNum)
        )
      )

      result mustEqual expected
    }

    "propagate structure/type information through Split nodes (2)" in {
      val line = Line(1, 1, "")
      def clicks = AbsoluteLoad(Const(CString("/clicks"))(line))(line)

      val id = new Identifier

      // clicks := //clicks forall 'user { user: 'user, num: count(clicks.user where clicks.user = 'user) }
      val input =
        Split(
          Group(0,
            Join(DerefObject, Cross(None), clicks, Const(CString("user"))(line))(line),
            UnfixedSolution(1,
              Join(DerefObject, Cross(None),
                clicks,
                Const(CString("user"))(line))(line))),
          Join(JoinObject, Cross(None),
            Join(WrapObject, Cross(None),
              Const(CString("user"))(line),
              SplitParam(1, id)(line))(line),
            Join(WrapObject, Cross(None),
              Const(CString("num"))(line),
              Reduce(Count,
                SplitGroup(0, clicks.identities, id)(line))(line))(line))(line), id)(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/clicks" -> Map(
          JPath("user") -> cLiterals
        )
      )

      result must_== expected
    }

    "propagate structure/type information through Split nodes (3)" in {
      val line = Line(1, 1, "")
      def clicks = AbsoluteLoad(Const(CString("/clicks"))(line))(line)

      val id = new Identifier

      // clicks := //clicks forall 'user { user: 'user, age: clicks.age, num: count(clicks.user where clicks.user = 'user) }
      val input =
        Split(
          Group(0,
            Join(DerefObject, Cross(None), clicks, Const(CString("user"))(line))(line),
            UnfixedSolution(1,
              Join(DerefObject, Cross(None),
                clicks,
                Const(CString("user"))(line))(line))),
          Join(JoinObject, Cross(None),
            Join(JoinObject, Cross(None),
              Join(WrapObject, Cross(None),
                Const(CString("user"))(line),
                SplitParam(1, id)(line))(line),
              Join(WrapObject, Cross(None),
                Const(CString("num"))(line),
                Reduce(Count,
                  SplitGroup(0, clicks.identities, id)(line))(line))(line))(line),
            Join(WrapObject, Cross(None),
              Const(CString("age"))(line),
              Join(DerefObject, Cross(None),
                clicks,
                Const(CString("age"))(line))(line))(line))(line), id)(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/clicks" -> Map(
          JPath("user") -> cLiterals,
          JPath("age") -> cLiterals
        )
      )

      result must_== expected
    }

    "rewrite loads for a trivial but complete DAG such that they will restrict the columns loaded" in {
      val line = Line(1, 1, "")

      val input =
        Join(Add, IdentitySort,
          Join(DerefObject, Cross(None),
            AbsoluteLoad(Const(CString("/clicks"))(line))(line),
            Const(CString("time"))(line))(line),
          Join(DerefObject, Cross(None),
            AbsoluteLoad(Const(CString("/hom/heightWeight"))(line))(line),
            Const(CString("height"))(line))(line))(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/clicks" -> Map(JPath("time") -> Set(CLong, CDouble, CNum)),
        "/hom/heightWeight" -> Map(JPath("height") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "negate type inference from deref by wrap" in {
      val line = Line(1, 1, "")

      val clicks = AbsoluteLoad(Const(CString("/clicks"))(line))(line)

      val input =
        Join(DerefObject, Cross(None),
          Join(WrapObject, Cross(None),
            Const(CString("foo"))(line),
            clicks)(line),
          Const(CString("foo"))(line))(line)

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/clicks" -> Map(NoJPath -> cLiterals))

      result mustEqual expected
    }

    "propagate type information through split->wrap->deref" in {
      val line = Line(1, 1, "")

      val clicks = AbsoluteLoad(Const(CString("/clicks"))(line))(line)

      val id = new Identifier

      val clicksTime =
        Join(DerefObject, Cross(None),
          clicks,
          Const(CString("time"))(line))(line)

      val split =
        Split(
          Group(0, clicks, UnfixedSolution(1, clicksTime)),
          Join(WrapObject, Cross(None),
            Const(CString("foo"))(line),
            SplitGroup(0, Identities.Specs(Vector(LoadIds("/clicks"))), id)(line))(line), id)(line)

      val input =
        Join(DerefObject, Cross(None),
          split,
          Const(CString("foo"))(line))(line)

      /*
       clicks := //clicks

       split := solve 'time
         clicks' := (clicks where clicks.time = 'time)
         { "foo": clicks' }

       split.foo
       */

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/clicks" -> Map(
          NoJPath -> cLiterals,
          JPath("time") -> cLiterals))

      result mustEqual expected
    }
  }
}

object TypeInferencerSpecs extends TypeInferencerSpecs[Need]

