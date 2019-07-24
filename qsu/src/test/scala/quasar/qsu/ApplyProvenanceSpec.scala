/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.qsu

import slamdata.Predef.{Map => SMap, _}

import quasar.{IdStatus, Qspec, Type}, IdStatus.{ExcludeId, IncludeId}
import quasar.common.JoinType
import quasar.contrib.iota._
import quasar.contrib.pathy.AFile
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.ski.κ
import quasar.qscript.{
  construction,
  LeftSide,
  OnUndefined,
  PlannerError,
  ReduceFuncs,
  RightSide
}
import quasar.qscript.MapFuncsCore.RecIntLit
import quasar.qsu.mra.ProvImpl

import cats.instances.list._

import matryoshka._
import matryoshka.data.Fix

import org.specs2.matcher.{Expectable, Matcher, MatchResult}

import pathy.Path, Path.{file, Sandboxed}

import scalaz.{\/, Cofree}
import scalaz.syntax.apply._
import scalaz.syntax.equal._
import scalaz.syntax.show._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.map._
import scalaz.std.tuple._

import shims.{eqToScalaz, orderToCats, orderToScalaz, showToCats, showToScalaz}

object ApplyProvenanceSpec extends Qspec with QSUTTypes[Fix] {

  import ApplyProvenance.{AuthenticatedQSU, computeFuncDims}
  import QScriptUniform.{DTrans, Rotation}

  type F[A] = PlannerError \/ A
  type QSU[A] = QScriptUniform[A]

  val J = Fixed[Fix[EJson]]

  val qsu = QScriptUniform.AnnotatedDsl[Fix, Symbol]
  val func = construction.Func[Fix]
  val recFunc = construction.RecFunc[Fix]

  val qprov = ProvImpl[Fix[EJson], IdAccess, IdType]
  val B = Bucketing(qprov)
  val app = ApplyProvenance[Fix, F](qprov, _: QSUGraph)

  import qprov.syntax._

  type P = qprov.P
  val P = qprov.empty

  implicit class QProvHelpers(p: P) {
    def prjPath(name: String): P =
      p.projectStatic(J.str(name), IdType.Dataset)

    def infRow(id: Symbol): P =
      p.inflateExtend(IdAccess.identity(id), IdType.Dataset)

    def infBuk(id: Symbol, idx: Int): P =
      p.inflateSubmerge(IdAccess.bucket(id, idx), IdType.Expr)

    def infGrp(id: Symbol, idx: Int): P =
      p.inflateSubmerge(IdAccess.groupKey(id, idx), IdType.Expr)

    def infMap(id: Symbol): P =
      p.inflateExtend(IdAccess.identity(id), IdType.Map)

    def cnjMap(id: Symbol): P =
      p.inflateConjoin(IdAccess.identity(id), IdType.Map)

    def infArr(id: Symbol): P =
      p.inflateExtend(IdAccess.identity(id), IdType.Array)

    def cnjArr(id: Symbol): P =
      p.inflateConjoin(IdAccess.identity(id), IdType.Array)

    def injMap(key: String): P =
      p.injectStatic(J.str(key), IdType.Map)

    def injArr(idx: Int): P =
      p.injectStatic(J.int(idx), IdType.Array)

    def prjMap(key: String): P =
      p.projectStatic(J.str(key), IdType.Map)

    def prjArr(idx: Int): P =
      p.projectStatic(J.int(idx), IdType.Array)
  }

  val root = Path.rootDir[Sandboxed]
  val afile: AFile = root </> file("foobar")
  val FooBar: P = P.prjPath("foobar")

  def funcDims[A](fm: FreeMapA[A])(f: A => P): Option[P] =
    computeFuncDims(qprov, fm)(f)

  "provenance application" should {

    "produce provenance for map" in {
      val fm: RecFreeMap = recFunc.Add(recFunc.Hole, RecIntLit(17))

      val tree: Cofree[QSU, Symbol] =
        qsu.map('name0,
          (qsu.read('name1, (afile, ExcludeId)), fm))

      val dims: SMap[Symbol, P] = SMap(
        'name0 -> FooBar.infRow('name1),
        'name1 -> FooBar.infRow('name1))

      tree must haveDimensions(dims)
    }

    "produce normalized provenance for map" in {
      val fm1: RecFreeMap =
        recFunc.StaticMapS(
          "A" -> recFunc.ProjectKeyS(recFunc.Hole, "X"),
          "B" -> recFunc.ProjectKeyS(recFunc.Hole, "Y"))

      val fm2: RecFreeMap =
        recFunc.Add(recFunc.ProjectKeyS(recFunc.Hole, "B"), RecIntLit(17))

      val tree: Cofree[QSU, Symbol] =
        qsu.map('name2, (
          qsu.map('name1, (
            qsu.read('name0, (afile, ExcludeId)),
            fm1)),
          fm2))

      val dims: SMap[Symbol, P] = SMap(
        'name0 -> FooBar.infRow('name0),
        'name1 ->
          (FooBar.infRow('name0).prjMap("X").injMap("A") ∧
            FooBar.infRow('name0).prjMap("Y").injMap("B")),
        'name2 ->
          (FooBar.infRow('name0).prjMap("X") ∧
            FooBar.infRow('name0).prjMap("Y")))

      tree must haveDimensions(dims)
    }

    "compute correct provenance nested dimEdits" >> {
      val tree =
        qsu.lpReduce('n4, (
          qsu.map('n3, (
            qsu.dimEdit('n2, (
              qsu.dimEdit('n1, (
                qsu.read('n0, (afile, ExcludeId)),
                DTrans.Group(func.ProjectKeyS(func.Hole, "x")))),
              DTrans.Group(func.ProjectKeyS(func.Hole, "y")))),
            recFunc.ProjectKeyS(recFunc.Hole, "pop"))),
          ReduceFuncs.Sum(())))

      tree must haveDimensions(SMap(
        'n4 -> FooBar.infRow('n2).infBuk('n4, 0).infBuk('n4, 1).prjMap("pop").reduce,
        'n3 -> FooBar.infRow('n2).infGrp('n2, 0).infGrp('n2, 1).prjMap("pop"),
        'n2 -> FooBar.infRow('n2).infGrp('n2, 0).infGrp('n2, 1),
        'n1 -> FooBar.infRow('n1).infGrp('n1, 0),
        'n0 -> FooBar.infRow('n0)
      ))
    }
  }

  "left shift provenance" >> {
    "flatten with trivial struct and repair is dimensional flatten" >> {
      val tree =
        qsu.leftShift('n0, (
          qsu.read('n1, (afile, ExcludeId)),
          recFunc.Hole,
          ExcludeId,
          OnUndefined.Omit,
          RightTarget[Fix],
          Rotation.FlattenArray))

      tree must haveDimensions(SMap(
        'n0 -> FooBar.infRow('n1).cnjArr('n0),
        'n1 -> FooBar.infRow('n1)
      ))
    }

    "shift with trivial struct and repair is dimensional shift" >> {
      val tree =
        qsu.leftShift('n0, (
          qsu.read('n1, (afile, ExcludeId)),
          recFunc.Hole,
          ExcludeId,
          OnUndefined.Omit,
          RightTarget[Fix],
          Rotation.ShiftArray))

      tree must haveDimensions(SMap(
        'n0 -> FooBar.infRow('n1).infArr('n0),
        'n1 -> FooBar.infRow('n1)
      ))
    }

    "include id affects provenance" >> {
      val tree =
        qsu.leftShift('n0, (
          qsu.read('n1, (afile, ExcludeId)),
          recFunc.Hole,
          IncludeId,
          OnUndefined.Omit,
          RightTarget[Fix],
          Rotation.ShiftArray))

      tree must haveDimensions(SMap(
        'n0 ->
          (FooBar.infRow('n1).infArr('n0).injArr(0) ∧
            FooBar.infRow('n1).infArr('n0).injArr(1)),
        'n1 -> FooBar.infRow('n1)
      ))
    }

    "struct affects provenance prior to shift" >> {
      val tree =
        qsu.leftShift('n0, (
          qsu.read('n1, (afile, ExcludeId)),
          recFunc.ProjectKeyS(recFunc.Hole, "k"),
          ExcludeId,
          OnUndefined.Omit,
          RightTarget[Fix],
          Rotation.ShiftArray))

      tree must haveDimensions(SMap(
        'n0 -> FooBar.infRow('n1).prjMap("k").infArr('n0),
        'n1 -> FooBar.infRow('n1)
      ))
    }

    "repair affects provenance after shift" >> {
      val tree =
        qsu.leftShift('n0, (
          qsu.read('n1, (afile, ExcludeId)),
          recFunc.Hole,
          ExcludeId,
          OnUndefined.Omit,
          func.ProjectKeyS(RightTarget[Fix], "k"),
          Rotation.ShiftArray))

      tree must haveDimensions(SMap(
        'n0 -> FooBar.infRow('n1).infArr('n0).prjMap("k"),
        'n1 -> FooBar.infRow('n1)
      ))
    }

    "non-structure preserving result in repair results in shift provenance" >> {
      val tree =
        qsu.leftShift('n0, (
          qsu.read('n1, (afile, ExcludeId)),
          recFunc.Hole,
          ExcludeId,
          OnUndefined.Omit,
          func.Add(RightTarget[Fix], func.Constant(J.int(42))),
          Rotation.ShiftArray))

      tree must haveDimensions(SMap(
        'n0 -> FooBar.infRow('n1).infArr('n0),
        'n1 -> FooBar.infRow('n1)
      ))
    }
  }

  "AutoJoin provenance" >> {
    "joining on projections is equivalent to projecting from the join" >> {
      val joinOnP =
        qsu._autojoin2('n0, (
          qsu.map('n1, (
            qsu.read('n2, (afile, ExcludeId)),
            recFunc.MakeMapS("A", recFunc.ProjectKeyS(recFunc.Hole, "X")))),
          qsu.map('n3, (
            qsu.read('n4, (afile, ExcludeId)),
            recFunc.MakeMapS("B", recFunc.ProjectKeyS(recFunc.Hole, "Y")))),
          func.ConcatMaps(func.LeftSide, func.RightSide)))

      val pOnJoin =
        qsu._autojoin2('n0, (
          qsu.read('n2, (afile, ExcludeId)),
          qsu.read('n4, (afile, ExcludeId)),
          func.ConcatMaps(
            func.MakeMapS("A", func.ProjectKeyS(func.LeftSide, "X")),
            func.MakeMapS("B", func.ProjectKeyS(func.RightSide, "Y")))))

      val jop = app(QSUGraph.fromAnnotatedTree[Fix](joinOnP map (Some(_)))._2) flatMap {
        case AuthenticatedQSU(g, a) => a.lookupDimsE[F](g.root)
      }

      val poj = app(QSUGraph.fromAnnotatedTree[Fix](pOnJoin map (Some(_)))._2) flatMap {
        case AuthenticatedQSU(g, a) => a.lookupDimsE[F](g.root)
      }

      (jop |@| poj)(_ must_= _) getOrElse ko
    }
  }

  "ThetaJoin provenance" >> {
    "should be joined when a subset of branches are referenced" >> {
      val otherfile: AFile = root </> file("other")
      val Other: P = P.prjPath("other")

      val n2 =
        qsu.map('n2, (
          qsu.read('n0, (afile, ExcludeId)),
          recFunc.ProjectKeyS(recFunc.Hole, "B")))

      val n3 =
        qsu.map('n3, (
          qsu.read('n1, (otherfile, ExcludeId)),
          recFunc.ProjectKeyS(recFunc.Hole, "A")))

      val n4 =
        qsu.thetaJoin('n4, (
          n2, n3,
          func.Eq(
            func.ProjectKeyS(func.LeftSide, "id"),
            func.ProjectKeyS(func.RightSide, "id")),
          JoinType.Inner,
          func.ProjectKeyS(func.RightSide, "C")))

      val p0 = FooBar.infRow('n0)
      val p1 = Other.infRow('n1)
      val p2 = p0.prjMap("B")
      val p3 = p1.prjMap("A")
      val p4 = (p2 ∧ p3).prjMap("C")

      val dims = SMap(
        'n0 -> p0,
        'n1 -> p1,
        'n2 -> p2,
        'n3 -> p3,
        'n4 -> p4)

      n4 must haveDimensions(dims)
    }
  }

  "Union provenance" >> {
    "should be affected independently by FreeMaps" in {
      val r =
        qsu.read('n1, (afile, ExcludeId))

      val shiftX =
        qsu.leftShift('n0, (
          r,
          recFunc.ProjectKeyS(recFunc.Hole, "x"),
          ExcludeId,
          OnUndefined.Omit,
          RightTarget[Fix],
          Rotation.ShiftArray))

      val shiftY =
        qsu.leftShift('n2, (
          r,
          recFunc.ProjectKeyS(recFunc.Hole, "y"),
          ExcludeId,
          OnUndefined.Omit,
          RightTarget[Fix],
          Rotation.ShiftMap))

      val fm: RecFreeMap =
        recFunc.Add(
          recFunc.ProjectKeyS(recFunc.Hole, "a"),
          recFunc.ProjectKeyS(recFunc.Hole, "b"))

      val tree: Cofree[QSU, Symbol] =
        qsu.map('n4, (
          qsu.union('n3, (shiftX, shiftY)),
          fm))

      val pR = FooBar.infRow('n1)

      val pX = pR.prjMap("x").infArr('n0)
      val pY = pR.prjMap("y").infMap('n2)

      val p3 = pX ∨ pY

      val p4 =
        (pX.prjMap("a") ∧ pX.prjMap("b")) ∨ (pY.prjMap("a") ∧ pY.prjMap("b"))

      val dims: SMap[Symbol, P] = SMap(
        'n0 -> pX,
        'n1 -> pR,
        'n2 -> pY,
        'n3 -> p3,
        'n4 -> p4)

      tree must haveDimensions(dims)
    }
  }

  "MapFunc provenance" >> {
    val rdims = P.prjPath("data").infRow('a)

    "make map injects" >> {
      val res = funcDims(func.MakeMapS("k", func.Hole))(κ(rdims))
      res must_= Some(rdims.injMap("k"))
    }

    "make array injects" >> {
      val res = funcDims(func.MakeArray(func.Hole))(κ(rdims))
      res must_= Some(rdims.injArr(0))
    }

    "project key projects" >> {
      val res = funcDims(func.ProjectKeyS(func.Hole, "k"))(κ(rdims))
      res must_= Some(rdims.prjMap("k"))
    }

    "project index projects" >> {
      val res = funcDims(func.ProjectIndexI(func.Hole, 1))(κ(rdims))
      res must_= Some(rdims.prjArr(1))
    }

    "concat map joins" >> {
      val fm = func.ConcatMaps(
        func.MakeMapS("k1", func.Hole),
        func.MakeMapS("k2", func.Hole))


      val exp = rdims.injMap("k1") ∧ rdims.injMap("k2")

      funcDims(fm)(κ(rdims)) must_= Some(exp)
    }

    "concat array joins, updating rhs injects" >> {
      val l = rdims.injArr(0)
      val r = rdims.injArr(0)

      val mf = func.ConcatArrays(func.LeftSide, func.RightSide)

      val res = funcDims(mf) {
        case LeftSide => l
        case RightSide => r
      }

      val exp =
        rdims.injArr(0) ∧ rdims.injArr(1)

      res must_= Some(exp)
    }.pendingUntilFixed("ch1487")

    "concat array where lhs is not static makes rhs existential" >> {
      val l = rdims.injectDynamic
      val r = rdims.injArr(0)

      val mf = func.ConcatArrays(func.LeftSide, func.RightSide)

      val res = funcDims(mf) {
        case LeftSide => l
        case RightSide => r
      }

      val sid = (J.int(0), IdType.Array)

      res.exists(_.foldMapScalarIds((s, t) => List((s, t))).exists(_ ≟ sid)) must beFalse
    }.pendingUntilFixed("ch1487")

    "concat array where lhs is static and rhs isn't joins" >> {
      val l = rdims.injArr(0)
      val r = rdims.injectDynamic

      val mf = func.ConcatArrays(func.LeftSide, func.RightSide)

      val res = funcDims(mf) {
        case LeftSide => l
        case RightSide => r
      }

      val sid = (J.int(0), IdType.Array)

      res.exists(_.foldMapScalarIds((s, t) => List((s, t))).exists(_ ≟ sid)) must beTrue
    }

    "delete key" >> {
      "identity when key is static" >> {
        funcDims(func.DeleteKeyS(func.Hole, "k"))(κ(rdims)) must_= Some(rdims)
      }

      "join when key is dynamic" >> {
        val l = rdims.prjMap("obj")
        val r = rdims.prjMap("keyName")

        val mf = func.DeleteKey(
          func.ProjectKeyS(func.Hole, "obj"),
          func.ProjectKeyS(func.Hole, "keyName"))

        funcDims(mf)(κ(rdims)) must_= Some(l ∧ r)
      }
    }

    "typecheck is identity" >> {
      val fm = func.Typecheck(func.Hole, Type.Top)
      funcDims(fm)(κ(rdims)) must_= Some(rdims)
    }

    "undefined is empty" >> {
      funcDims(func.Undefined)(κ(rdims)) must beNone
    }

    "constant is empty" >> {
      funcDims(func.Constant(J.str("nope")))(κ(rdims)) must beNone
    }

    "default is to join" >> {
      val fm = func.Add(
        func.ProjectKeyS(func.Hole, "x"),
        func.ProjectKeyS(func.Hole, "y"))

      val exp = rdims.prjMap("x") ∧ rdims.prjMap("y")

      funcDims(fm)(κ(rdims)) must_= Some(exp)
    }
  }

  // checks the expected dimensions
  def haveDimensions(expected: SMap[Symbol, P])
      : Matcher[Cofree[QSU, Symbol]] =
    new Matcher[Cofree[QSU, Symbol]] {
      def apply[S <: Cofree[QSU, Symbol]](s: Expectable[S]): MatchResult[S] = {

        val (renames, inputGraph): (QSUGraph.Renames, QSUGraph) =
          QSUGraph.fromAnnotatedTree[Fix](s.value.map(Some(_)))

        val expectedDims: SMap[Symbol, P] =
          expected.map { case (k, v) =>
            val newP = renames.foldLeft(v)((p, t) => B.rename(t._1, t._2, p))
            (renames(k), newP)
          }

        val actual: PlannerError \/ AuthenticatedQSU[Fix, P] = app(inputGraph)

        actual.bimap[MatchResult[S], MatchResult[S]](
        { err =>
          failure(s"provenance application produced unexpected planner error: ${err}", s)
        },
        { case aqsu @ AuthenticatedQSU(resultGraph, qauth) =>
          result(
            qauth.dims ≟ expectedDims,
            s"received expected authenticated QSU:\n${aqsu.shows}",
            s"received unexpected dims:\n${printMultiline(qauth.dims.toList)}\n" + // `aqsu.shows` prunes orphan dims
            s"wth graph:\n${resultGraph.shows}\n" +
            s"expected:\n[\n${printMultiline(expectedDims.toList)}\n]",
            s)
        }).merge
      }
    }
}
