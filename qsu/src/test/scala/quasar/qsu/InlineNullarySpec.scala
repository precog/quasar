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

import quasar.{Qspec, TreeMatchers}
import quasar.IdStatus.ExcludeId
import quasar.contrib.iota._
import quasar.contrib.matryoshka._
import quasar.contrib.pathy.AFile
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.qscript.{construction, Hole, JoinSide}

import matryoshka._
import matryoshka.data._
import pathy.Path._

object InlineNullarySpec extends Qspec with QSUTTypes[Fix] with TreeMatchers {
  import QSUGraph.Extractors._

  val qsu = QScriptUniform.DslT[Fix]
  val rec = construction.RecFunc[Fix]
  val mf = construction.Func[Fix]
  val J = Fixed[Fix[EJson]]

  val dataA: AFile = rootDir </> file("dataA")
  val dataB: AFile = rootDir </> file("dataB")

  def inlineNullary(g: QSUGraph): QSUGraph =
    InlineNullary[Fix](g)

  "inlining nullary nodes" should {
    "be identity for nullary Map nodes" >> {
      val fm =
        rec.MakeMapS("now", rec.Now[Hole])

      val g = QSUGraph.fromTree[Fix](
        qsu.map(qsu.unreferenced(), fm))

      inlineNullary(g) must beLike {
        case Map(Unreferenced(), f) =>
          f.linearize must beTreeEqual(fm.linearize)
      }
    }

    "convert an AutoJoin2 with a nullary root into a Map" >> {
      val key = rec.Constant[Hole](J.str("k1"))
      val fm = mf.MakeMap(mf.LeftSide, mf.RightSide)

      val g = QSUGraph.fromTree[Fix](
        qsu._autojoin2(
          qsu.map(
            qsu.unreferenced(),
            key),
          qsu.read(dataA, ExcludeId),
          fm))

      inlineNullary(g) must beLike {
        case Map(Read(p, ExcludeId), f) =>
          val exp = mf.MakeMapS("k1", mf.Hole)
          p must_= dataA
          f.linearize must beTreeEqual(exp)
      }
    }

    "convert an AutoJoin3 with one nullary root into an AutoJoin2" >> {
      val g = QSUGraph.fromTree[Fix](
        qsu._autojoin3(
          qsu.map(
            qsu.unreferenced(),
            rec.Eq(rec.Now[Hole], rec.Constant(J.int(0)))),
          qsu.read(dataA, ExcludeId),
          qsu.read(dataB, ExcludeId),
          mf.Cond(mf.LeftSide3, mf.Center, mf.RightSide3)))

      inlineNullary(g) must beLike {
        case AutoJoin2(Read(l, ExcludeId), Read(r, ExcludeId), f) =>
          l must_= dataA
          r must_= dataB

          val exp = mf.Cond(
            mf.Eq(mf.Now[JoinSide], mf.Constant(J.int(0))),
            mf.LeftSide,
            mf.RightSide)

          f must beTreeEqual(exp)
      }
    }

    "convert an AutoJoin3 with two nullary roots into a Map" >> {
      val g = QSUGraph.fromTree[Fix](
        qsu._autojoin3(
          qsu.map(
            qsu.unreferenced(),
            rec.Eq(rec.Now[Hole], rec.Constant(J.int(0)))),
          qsu.read(dataA, ExcludeId),
          qsu.map(
            qsu.unreferenced(),
            rec.Constant[Hole](J.str("row"))),
          mf.Cond(mf.LeftSide3, mf.Center, mf.RightSide3)))

      inlineNullary(g) must beLike {
        case Map(Read(l, ExcludeId), f) =>
          l must_= dataA

          val exp = mf.Cond(
            mf.Eq(mf.Now[Hole], mf.Constant(J.int(0))),
            mf.Hole,
            mf.Constant(J.str("row")))

          f.linearize must beTreeEqual(exp)
      }
    }
  }
}
