/*
 * Copyright 2014–2020 SlamData Inc.
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

import quasar.{Qspec, TreeMatchers}
import quasar.IdStatus
import quasar.contrib.iota._
import quasar.contrib.matryoshka._
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.qscript.{
  construction,
  LeftSide,
  MapFuncsCore,
  OnUndefined,
  RightSide
}
import quasar.qsu.mra.ProvImpl

import matryoshka._
import matryoshka.data._
import pathy.Path._

import shims.orderToCats

object CatchTransposeSpec extends Qspec with QSUTTypes[Fix] with TreeMatchers {
  import QSUGraph.Extractors._
  import QScriptUniform.{Retain, Rotation}
  import ApplyProvenance.AuthenticatedQSU

  val qsu = QScriptUniform.DslT[Fix]
  val rec = construction.RecFunc[Fix]
  val func = construction.Func[Fix]
  val qprov = ProvImpl[Fix[EJson], IdAccess, IdType]
  val J = Fixed[Fix[EJson]]

  type P = qprov.P
  type J = Fix[EJson]

  val readA = qsu.read(
    rootDir[Sandboxed] </> file("dataA.json"),
    IdStatus.ExcludeId)

  val readB = qsu.read(
    rootDir[Sandboxed] </> file("dataB.json"),
    IdStatus.ExcludeId)

  def runOn(qgraph: QSUGraph, auth: Option[QAuth[P]] = None): QSUGraph =
    CatchTranspose[Fix, P](AuthenticatedQSU(qgraph, auth getOrElse QAuth.empty[Fix, P])).graph

  "reifying uncoalesced transpose" should {
    "convert a minimal values transpose" >> {
      val g = QSUGraph.fromTree[Fix](
        qsu.transpose(readA, Retain.Values, Rotation.ShiftMap))

      runOn(g) must beLike {
        case LeftShift(
          Read(_, _),
          struct,
          IdStatus.ExcludeId,
          OnUndefined.Emit,
          repair,
          Rotation.ShiftMap) =>

        struct must beTreeEqual(rec.Hole)
        repair must beTreeEqual(func.RightSide)
      }
    }

    "convert a minimal ids transpose" >> {
      val g = QSUGraph.fromTree[Fix](
        qsu.transpose(readA, Retain.Identities, Rotation.FlattenArray))

      runOn(g) must beLike {
        case LeftShift(
          Read(_, _),
          struct,
          IdStatus.IdOnly,
          OnUndefined.Emit,
          repair,
          Rotation.FlattenArray) =>

        struct must beTreeEqual(rec.Hole)
        repair must beTreeEqual(func.RightSide)
      }
    }

    "convert multiple transpose" >> {
      val g = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.transpose(readA, Retain.Values, Rotation.FlattenMap),
          qsu.transpose(readB, Retain.Values, Rotation.FlattenArray),
          _(MapFuncsCore.ConcatMaps(_, _)))))

      runOn(g) must beLike {
        case AutoJoin2C(
          LeftShift(
            _,
            structA,
            IdStatus.ExcludeId,
            OnUndefined.Emit,
            repairA,
            Rotation.FlattenMap),
          LeftShift(
            _,
            structB,
            IdStatus.ExcludeId,
            OnUndefined.Emit,
            repairB,
            Rotation.FlattenArray),
          MapFuncsCore.ConcatMaps(LeftSide, RightSide)) =>

        structA must beTreeEqual(rec.Hole)
        repairA must beTreeEqual(func.RightSide)

        structB must beTreeEqual(rec.Hole)
        repairB must beTreeEqual(func.RightSide)
      }
    }

    "convert mappable region applied to tranpose into repair" >> {
      val g = QSUGraph.fromTree[Fix](
        qsu.map(
          qsu.transpose(
            readA,
            Retain.Values,
            Rotation.ShiftMap),
          rec.StaticMapS(
            "foo" -> rec.ProjectKeyS(rec.Hole, "foo"),
            "bar" -> rec.ProjectKeyS(rec.Hole, "bar"))))

      runOn(g) must beLike {
        case LeftShift(
          Read(_, _),
          struct,
          IdStatus.ExcludeId,
          OnUndefined.Emit,
          repair,
          Rotation.ShiftMap) =>

        struct must beTreeEqual(rec.Hole)

        repair must beTreeEqual(
          func.StaticMapS(
            "foo" -> func.ProjectKeyS(func.RightSide, "foo"),
            "bar" -> func.ProjectKeyS(func.RightSide, "bar")))
      }
    }

    "convert mappable region applied to transpose source into struct" >> {
      val g = QSUGraph.fromTree[Fix](
        qsu.transpose(
          qsu.map(
            readA,
            rec.ProjectKeyS(rec.Hole, "foo")),
          Retain.Values,
          Rotation.ShiftMap))

      runOn(g) must beLike {
        case LeftShift(
          Read(_, _),
          struct,
          IdStatus.ExcludeId,
          OnUndefined.Emit,
          repair,
          Rotation.ShiftMap) =>

        struct must beTreeEqual(rec.ProjectKeyS(rec.Hole, "foo"))

        repair must beTreeEqual(func.RightSide)
      }
    }

    "convert both struct and repair" >> {
      val g = QSUGraph.fromTree[Fix](
        qsu.map(
          qsu.transpose(
            qsu.map(
              readA,
              rec.ProjectKeyS(rec.Hole, "quux")),
            Retain.Values,
            Rotation.ShiftMap),
          rec.StaticMapS(
            "foo" -> rec.ProjectKeyS(rec.Hole, "foo"),
            "bar" -> rec.ProjectKeyS(rec.Hole, "bar"))))

      runOn(g) must beLike {
        case LeftShift(
          Read(_, _),
          struct,
          IdStatus.ExcludeId,
          OnUndefined.Emit,
          repair,
          Rotation.ShiftMap) =>

        struct must beTreeEqual(rec.ProjectKeyS(rec.Hole, "quux"))

        repair must beTreeEqual(
          func.StaticMapS(
            "foo" -> func.ProjectKeyS(func.RightSide, "foo"),
            "bar" -> func.ProjectKeyS(func.RightSide, "bar")))
      }
    }

    "halt mappable region conversion at grouped nodes" >> {
      val ann = QScriptUniform.AnnotatedDsl[Fix, Symbol]

      val tree =
        ann.map('n0, (
          ann.transpose('n1, (
            ann.map('n2, (
              ann.read('n3, (
                rootDir[Sandboxed] </> file("dataA.json"),
                IdStatus.ExcludeId)),
              rec.ProjectKeyS(rec.Hole, "quux"))),
            Retain.Values,
            Rotation.ShiftMap)),
          rec.StaticMapS(
            "foo" -> rec.ProjectKeyS(rec.Hole, "foo"),
            "bar" -> rec.ProjectKeyS(rec.Hole, "bar"))))

      val (rn, g) = QSUGraph.fromAnnotatedTree[Fix](tree.map(Some(_)))
      val keys = SMap((rn('n2), 0) -> func.ProjectKeyS(func.Hole, "groupkey"))
      val auth = QAuth[Fix, P](SMap(), keys)

      runOn(g, Some(auth)) must beLike {
        case LeftShift(
          Map(
            Read(_, _),
            f),
          struct,
          IdStatus.ExcludeId,
          OnUndefined.Emit,
          repair,
          Rotation.ShiftMap) =>

        f must beTreeEqual(rec.ProjectKeyS(rec.Hole, "quux"))

        repair must beTreeEqual(
          func.StaticMapS(
            "foo" -> func.ProjectKeyS(func.RightSide, "foo"),
            "bar" -> func.ProjectKeyS(func.RightSide, "bar")))
      }
    }
  }
}
