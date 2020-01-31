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

import slamdata.Predef._

import quasar.{Qspec, TreeMatchers}
import quasar.IdStatus.ExcludeId
import quasar.contrib.iota._
import quasar.contrib.matryoshka._
import quasar.contrib.pathy.AFile
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.qscript.construction

import matryoshka._
import matryoshka.data._
import pathy.Path._

object CoalesceSquashedMappableSpec extends Qspec with QSUTTypes[Fix] with TreeMatchers {
  import QSUGraph.Extractors._
  import QScriptUniform.{Retain, Rotation}

  val qsu = QScriptUniform.DslT[Fix]
  val rec = construction.RecFunc[Fix]
  val mf = construction.Func[Fix]
  val J = Fixed[Fix[EJson]]

  val dataA: AFile = rootDir </> file("dataA")
  val dataB: AFile = rootDir </> file("dataB")

  val coalesced = CoalesceSquashedMappable[Fix] _

  "reshapes a Map node producing a map" >> {
    val g = QSUGraph.fromTree[Fix](
      qsu.map(
        qsu.dimEdit(
          qsu.map(
            qsu.read(dataA, ExcludeId),
            rec.StaticMapS(
              "a" -> rec.ProjectKeyS(rec.Hole, "x"),
              "b" -> rec.Multiply(rec.ProjectKeyS(rec.Hole, "y"), rec.Constant(J.int(7))))),
          QScriptUniform.DTrans.Squash[Fix]()),
        rec.StaticMapS(
          "a" -> rec.ProjectKeyS(rec.Hole, "a"),
          "tot" -> rec.ProjectKeyS(rec.Hole, "b"),
          "sub" -> rec.Subtract(rec.ProjectKeyS(rec.Hole, "b"), rec.Constant(J.int(10))))))

    coalesced(g) must beLike {
      case Map(Read(_, ExcludeId), f) =>
        val expected =
          rec.StaticMapS(
            "a" -> rec.ProjectKeyS(rec.Hole, "x"),
            "tot" -> rec.Multiply(rec.ProjectKeyS(rec.Hole, "y"), rec.Constant(J.int(7))),
            "sub" -> rec.Subtract(
              rec.Multiply(rec.ProjectKeyS(rec.Hole, "y"), rec.Constant(J.int(7))),
              rec.Constant(J.int(10))))

        f must beTreeEqual(expected)
    }
  }

  "series of reshapings to a map node" >> {
    val g = QSUGraph.fromTree[Fix](
      qsu.map(
        qsu.dimEdit(
          qsu.map(
            qsu.dimEdit(
              qsu.map(
                qsu.read(dataA, ExcludeId),
                rec.StaticMapS(
                  "a" -> rec.ProjectKeyS(rec.Hole, "x"),
                  "b" -> rec.Multiply(rec.ProjectKeyS(rec.Hole, "y"), rec.Constant(J.int(7))))),
              QScriptUniform.DTrans.Squash[Fix]()),
            rec.StaticMapS(
              "a" -> rec.ProjectKeyS(rec.Hole, "a"),
              "tot" -> rec.ProjectKeyS(rec.Hole, "b"),
              "sub" -> rec.Subtract(rec.ProjectKeyS(rec.Hole, "b"), rec.Constant(J.int(10))))),
          QScriptUniform.DTrans.Squash[Fix]()),
        rec.StaticMapS(
          "Category" -> rec.ProjectKeyS(rec.Hole, "a"),
          "Total" -> rec.ProjectKeyS(rec.Hole, "tot"),
          "Subtotal" -> rec.ProjectKeyS(rec.Hole, "sub"))))

    coalesced(g) must beLike {
      case Map(Read(_, ExcludeId), f) =>
        val expected =
          rec.StaticMapS(
            "Category" -> rec.ProjectKeyS(rec.Hole, "x"),
            "Total" -> rec.Multiply(rec.ProjectKeyS(rec.Hole, "y"), rec.Constant(J.int(7))),
            "Subtotal" -> rec.Subtract(
              rec.Multiply(rec.ProjectKeyS(rec.Hole, "y"), rec.Constant(J.int(7))),
              rec.Constant(J.int(10))))

        f must beTreeEqual(expected)
    }
  }

  "reshaping a mappable region of multiple roots" >> {
    val read =
      qsu.read(dataB, ExcludeId)

    val orders =
      qsu.transpose(
        qsu.map(read, rec.ProjectKeyS(rec.Hole, "orders")),
        Retain.Values,
        Rotation.ShiftArray)

    val g = QSUGraph.fromTree[Fix](
      qsu.dimEdit(
        qsu.map(
          qsu.dimEdit(
            qsu._autojoin2(
              qsu._autojoin2(
                qsu._autojoin2(
                  read,
                  orders,
                  mf.ConcatMaps(
                    mf.MakeMapS("a", mf.ProjectKeyS(mf.LeftSide, "a")),
                    mf.MakeMapS("x", mf.ProjectKeyS(mf.RightSide, "x")))),
                read,
                mf.ConcatMaps(
                  mf.LeftSide,
                  mf.MakeMapS("b", mf.ProjectKeyS(mf.RightSide, "b")))),
              orders,
              mf.ConcatMaps(
                mf.LeftSide,
                mf.MakeMapS("y", mf.ProjectKeyS(mf.RightSide, "y")))),
            QScriptUniform.DTrans.Squash[Fix]()),
          rec.StaticMapS(
            "alpha" -> rec.ProjectKeyS(rec.Hole, "a"),
            "bravo" -> rec.ProjectKeyS(rec.Hole, "b"),
            "xray" -> rec.ProjectKeyS(rec.Hole, "x"),
            "yankee" -> rec.ProjectKeyS(rec.Hole, "y"))),
        QScriptUniform.DTrans.Squash[Fix]()))

    coalesced(g) must beLike {
      case DimEdit(
          AutoJoin2(
            AutoJoin2(
              AutoJoin2(
                Read(_, ExcludeId),
                Transpose(Map(Read(_, ExcludeId), _), Retain.Values, Rotation.ShiftArray),
                combine0),
              Read(_, ExcludeId),
              combine1),
            Transpose(Map(Read(_, ExcludeId), _), Retain.Values, Rotation.ShiftArray),
            combine2),
          QScriptUniform.DTrans.Squash()) =>

        combine0 must beTreeEqual(
          mf.ConcatMaps(
            mf.MakeMapS("alpha", mf.ProjectKeyS(mf.LeftSide, "a")),
            mf.MakeMapS("xray", mf.ProjectKeyS(mf.RightSide, "x"))))

        combine1 must beTreeEqual(
          mf.ConcatMaps(
            mf.LeftSide,
            mf.MakeMapS("bravo", mf.ProjectKeyS(mf.RightSide, "b"))))

        combine2 must beTreeEqual(
          mf.ConcatMaps(
            mf.LeftSide,
            mf.MakeMapS("yankee", mf.ProjectKeyS(mf.RightSide, "y"))))
    }
  }

  "multiple reshapings to a mappable region of multiple roots" >> {
    val read =
      qsu.read(dataB, ExcludeId)

    val orders =
      qsu.transpose(
        qsu.map(read, rec.ProjectKeyS(rec.Hole, "orders")),
        Retain.Values,
        Rotation.ShiftArray)

    val g = QSUGraph.fromTree[Fix](
      qsu.dimEdit(
        qsu.map(
          qsu.dimEdit(
            qsu.map(
              qsu.dimEdit(
                qsu._autojoin2(
                  qsu._autojoin2(
                    qsu._autojoin2(
                      read,
                      orders,
                      mf.ConcatMaps(
                        mf.MakeMapS("a", mf.ProjectKeyS(mf.LeftSide, "a")),
                        mf.MakeMapS("x", mf.ProjectKeyS(mf.RightSide, "x")))),
                    read,
                    mf.ConcatMaps(
                      mf.LeftSide,
                      mf.MakeMapS("b", mf.ProjectKeyS(mf.RightSide, "b")))),
                  orders,
                  mf.ConcatMaps(
                    mf.LeftSide,
                    mf.MakeMapS("y", mf.ProjectKeyS(mf.RightSide, "y")))),
                QScriptUniform.DTrans.Squash[Fix]()),
              rec.StaticMapS(
                "alpha" -> rec.ProjectKeyS(rec.Hole, "a"),
                "bravo" -> rec.ProjectKeyS(rec.Hole, "b"),
                "xray" -> rec.ProjectKeyS(rec.Hole, "x"),
                "yankee" -> rec.ProjectKeyS(rec.Hole, "y"))),
            QScriptUniform.DTrans.Squash[Fix]()),
          rec.StaticMapS(
            "ALPHA" -> rec.Upper(rec.ProjectKeyS(rec.Hole, "alpha")),
            "BRAVO" -> rec.Upper(rec.ProjectKeyS(rec.Hole, "bravo")),
            "XRAY" -> rec.Upper(rec.ProjectKeyS(rec.Hole, "xray")),
            "YANKEE" -> rec.Upper(rec.ProjectKeyS(rec.Hole, "yankee")))),
        QScriptUniform.DTrans.Squash[Fix]()))

    coalesced(g) must beLike {
      case DimEdit(
          AutoJoin2(
            AutoJoin2(
              AutoJoin2(
                Read(_, ExcludeId),
                Transpose(Map(Read(_, ExcludeId), _), Retain.Values, Rotation.ShiftArray),
                combine0),
              Read(_, ExcludeId),
              combine1),
            Transpose(Map(Read(_, ExcludeId), _), Retain.Values, Rotation.ShiftArray),
            combine2),
          QScriptUniform.DTrans.Squash()) =>

        combine0 must beTreeEqual(
          mf.ConcatMaps(
            mf.MakeMapS("ALPHA", mf.Upper(mf.ProjectKeyS(mf.LeftSide, "a"))),
            mf.MakeMapS("XRAY", mf.Upper(mf.ProjectKeyS(mf.RightSide, "x")))))

        combine1 must beTreeEqual(
          mf.ConcatMaps(
            mf.LeftSide,
            mf.MakeMapS("BRAVO", mf.Upper(mf.ProjectKeyS(mf.RightSide, "b")))))

        combine2 must beTreeEqual(
          mf.ConcatMaps(
            mf.LeftSide,
            mf.MakeMapS("YANKEE", mf.Upper(mf.ProjectKeyS(mf.RightSide, "y")))))
    }
  }
}
