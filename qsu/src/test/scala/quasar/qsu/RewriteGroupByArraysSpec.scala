/*
 * Copyright 2014–2018 SlamData Inc.
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

import quasar.{ejson, IdStatus, Qspec}
import quasar.common.data.Data
import quasar.qscript.{construction, Hole, LeftSide, MapFuncsCore, MFC, RightSide, SrcHole}
import slamdata.Predef._

import matryoshka.data.Fix
import pathy.Path, Path.Sandboxed
import scalaz.{Need, StateT}

object RewriteGroupByArraysSpec extends Qspec with QSUTTypes[Fix] {
  import QSUGraph.Extractors._
  import IdStatus.ExcludeId

  type F[A] = StateT[Need, Long, A]

  val qsu = QScriptUniform.DslT[Fix]
  val json = ejson.Fixed[Fix[ejson.EJson]]
  val func = construction.Func[Fix]
  val rw = RewriteGroupByArrays[Fix, F] _

  val afile = Path.rootDir[Sandboxed] </> Path.file("afile")
  val afile2 = Path.rootDir[Sandboxed] </> Path.file("afile2")
  val afile3 = Path.rootDir[Sandboxed] </> Path.file("afile3")

  "re-nesting flat groupby structure" should {
    "leave a single groupby alone" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.groupBy(
          qsu.unreferenced(),
          qsu.autojoin2((
            qsu.unreferenced(),
            qsu.cstr("foo"),
            _(MapFuncsCore.ProjectKey(_, _))))))

      eval(rw(qgraph)) must beLike {
        case GroupBy(
          Unreferenced(),
          AutoJoin2C(
            Unreferenced(),
            DataConstantMapped(Data.Str("foo")),
            MapFuncsCore.ProjectKey(LeftSide, RightSide))) => ok
      }
    }

    "rewrite a pair of group keys" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.groupBy(
          qsu.read(afile, ExcludeId),
          qsu.autojoin2((
            qsu.unary(
              qsu.autojoin2((
                qsu.read(afile, ExcludeId),
                qsu.cstr("foo"),
                _(MapFuncsCore.ProjectKey(_, _)))),
              MFC(MapFuncsCore.MakeArray[Fix, Hole](SrcHole))),
            qsu.unary(
              qsu.autojoin2((
                qsu.read(afile, ExcludeId),
                qsu.cstr("bar"),
                _(MapFuncsCore.ProjectKey(_, _)))),
              MFC(MapFuncsCore.MakeArray[Fix, Hole](SrcHole))),
            _(MapFuncsCore.ConcatArrays(_, _))))))

      eval(rw(qgraph)) must beLike {
        case GroupBy(
          GroupBy(
            Read(`afile`, ExcludeId),
            AutoJoin2C(
              Read(`afile`, ExcludeId),
              DataConstantMapped(Data.Str("foo")),
              MapFuncsCore.ProjectKey(LeftSide, RightSide))),
          AutoJoin2C(
            GroupBy(
              Read(`afile`, ExcludeId),
              AutoJoin2C(
                Read(`afile`, ExcludeId),
                DataConstantMapped(Data.Str("foo")),
                MapFuncsCore.ProjectKey(LeftSide, RightSide))),
            DataConstantMapped(Data.Str("bar")),
            MapFuncsCore.ProjectKey(LeftSide, RightSide))) => ok
      }
    }

    "rewrite a triple of group keys" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.groupBy(
          qsu.read(afile, ExcludeId),
          qsu.autojoin2((
            qsu.autojoin2((
              qsu.unary(
                qsu.autojoin2((
                  qsu.read(afile, ExcludeId),
                  qsu.cstr("foo"),
                  _(MapFuncsCore.ProjectKey(_, _)))),
                MFC(MapFuncsCore.MakeArray[Fix, Hole](SrcHole))),
              qsu.unary(
                qsu.autojoin2((
                  qsu.read(afile, ExcludeId),
                  qsu.cstr("bar"),
                  _(MapFuncsCore.ProjectKey(_, _)))),
                MFC(MapFuncsCore.MakeArray[Fix, Hole](SrcHole))),
              _(MapFuncsCore.ConcatArrays(_, _)))),
            qsu.unary(
              qsu.autojoin2((
                qsu.read(afile, ExcludeId),
                qsu.cstr("baz"),
                _(MapFuncsCore.ProjectKey(_, _)))),
              MFC(MapFuncsCore.MakeArray[Fix, Hole](SrcHole))),
            _(MapFuncsCore.ConcatArrays(_, _))))))

      eval(rw(qgraph)) must beLike {
        case GroupBy(
          GroupBy(    // s2
            GroupBy(   // s1
              Read(`afile`, ExcludeId),
              AutoJoin2C(
                Read(`afile`, ExcludeId),
                DataConstantMapped(Data.Str("foo")),
                MapFuncsCore.ProjectKey(LeftSide, RightSide))),
            AutoJoin2C(
              GroupBy(  // s1
                Read(`afile`, ExcludeId),
                AutoJoin2C(
                  Read(`afile`, ExcludeId),
                  DataConstantMapped(Data.Str("foo")),
                  MapFuncsCore.ProjectKey(LeftSide, RightSide))),
              DataConstantMapped(Data.Str("bar")),
              MapFuncsCore.ProjectKey(LeftSide, RightSide))),
          AutoJoin2C(
            GroupBy(    // s2
              GroupBy(
                Read(`afile`, ExcludeId),
                AutoJoin2C(
                  Read(`afile`, ExcludeId),
                  DataConstantMapped(Data.Str("foo")),
                  MapFuncsCore.ProjectKey(LeftSide, RightSide))),
              AutoJoin2C(
                GroupBy(    // s1
                  Read(`afile`, ExcludeId),
                  AutoJoin2C(
                    Read(`afile`, ExcludeId),
                    DataConstantMapped(Data.Str("foo")),
                    MapFuncsCore.ProjectKey(LeftSide, RightSide))),
                DataConstantMapped(Data.Str("bar")),
                MapFuncsCore.ProjectKey(LeftSide, RightSide))),
            DataConstantMapped(Data.Str("baz")),
            MapFuncsCore.ProjectKey(LeftSide, RightSide))) => ok
      }
    }

    "rewrite a triple of group keys, the first of which is constant" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.groupBy(
          qsu.read(afile, ExcludeId),
          qsu.autojoin2((
            qsu.autojoin2((
              qsu.unary(
                qsu.unreferenced(),
                MFC(MapFuncsCore.Constant[Fix, Hole](json.arr(List(json.nul()))))),
              qsu.unary(
                qsu.autojoin2((
                  qsu.read(afile, ExcludeId),
                  qsu.cstr("bar"),
                  _(MapFuncsCore.ProjectKey(_, _)))),
                MFC(MapFuncsCore.MakeArray[Fix, Hole](SrcHole))),
              _(MapFuncsCore.ConcatArrays(_, _)))),
            qsu.unary(
              qsu.autojoin2((
                qsu.read(afile, ExcludeId),
                qsu.cstr("baz"),
                _(MapFuncsCore.ProjectKey(_, _)))),
              MFC(MapFuncsCore.MakeArray[Fix, Hole](SrcHole))),
            _(MapFuncsCore.ConcatArrays(_, _))))))

      eval(rw(qgraph)) must beLike {
        case GroupBy(
          GroupBy(    // s2
            GroupBy(   // s1
              Read(`afile`, ExcludeId),
              DataConstant(Data.Arr(List(Data.Null)))),
            AutoJoin2C(
              GroupBy(  // s1
                Read(`afile`, ExcludeId),
                DataConstant(Data.Arr(List(Data.Null)))),
              DataConstantMapped(Data.Str("bar")),
              MapFuncsCore.ProjectKey(LeftSide, RightSide))),
          AutoJoin2C(
            GroupBy(    // s2
              GroupBy(
                Read(`afile`, ExcludeId),
                DataConstant(Data.Arr(List(Data.Null)))),
              AutoJoin2C(
                GroupBy(    // s1
                  Read(`afile`, ExcludeId),
                  DataConstant(Data.Arr(List(Data.Null)))),
                DataConstantMapped(Data.Str("bar")),
                MapFuncsCore.ProjectKey(LeftSide, RightSide))),
            DataConstantMapped(Data.Str("baz")),
            MapFuncsCore.ProjectKey(LeftSide, RightSide))) => ok
      }
    }

    "rewrite a triple of group keys, the last of which is constant" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.groupBy(
          qsu.read(afile, ExcludeId),
          qsu.autojoin2((
            qsu.autojoin2((
              qsu.unary(
                qsu.autojoin2((
                  qsu.read(afile, ExcludeId),
                  qsu.cstr("foo"),
                  _(MapFuncsCore.ProjectKey(_, _)))),
                MFC(MapFuncsCore.MakeArray[Fix, Hole](SrcHole))),
              qsu.unary(
                qsu.autojoin2((
                  qsu.read(afile, ExcludeId),
                  qsu.cstr("bar"),
                  _(MapFuncsCore.ProjectKey(_, _)))),
                MFC(MapFuncsCore.MakeArray[Fix, Hole](SrcHole))),
              _(MapFuncsCore.ConcatArrays(_, _)))),
            qsu.unary(
              qsu.unreferenced(),
              MFC(MapFuncsCore.Constant[Fix, Hole](json.arr(List(json.nul()))))),
            _(MapFuncsCore.ConcatArrays(_, _))))))

      eval(rw(qgraph)) must beLike {
        case GroupBy(
          GroupBy(    // s2
            GroupBy(   // s1
              Read(`afile`, ExcludeId),
              AutoJoin2C(
                Read(`afile`, ExcludeId),
                DataConstantMapped(Data.Str("foo")),
                MapFuncsCore.ProjectKey(LeftSide, RightSide))),
            AutoJoin2C(
              GroupBy(  // s1
                Read(`afile`, ExcludeId),
                AutoJoin2C(
                  Read(`afile`, ExcludeId),
                  DataConstantMapped(Data.Str("foo")),
                  MapFuncsCore.ProjectKey(LeftSide, RightSide))),
              DataConstantMapped(Data.Str("bar")),
              MapFuncsCore.ProjectKey(LeftSide, RightSide))),
          DataConstant(Data.Arr(List(Data.Null)))) => ok
      }
    }

    "not produce an invalid graph" in {
      val rlp2 = qsu.unreferenced()
      val rlp8 = qsu.unary(rlp2, MFC(MapFuncsCore.Constant[Fix, Hole](json.int(0))))
      val rlp3 = qsu.unary(rlp2, MFC(MapFuncsCore.Constant[Fix, Hole](json.str("a"))))
      val rlp0 = qsu.read(afile, ExcludeId)
      val rlp4 = qsu.autojoin2((rlp0, rlp3, _(MapFuncsCore.ProjectKey(_, _))))
      val rlp9 = qsu.autojoin2((rlp4, rlp8, _(MapFuncsCore.ProjectIndex(_, _))))
      val rlp10 = qsu.unary(rlp9, MFC(MapFuncsCore.MakeArray[Fix, Hole](SrcHole)))
      val rlp7 = qsu.unary(rlp4, MFC(MapFuncsCore.MakeArray[Fix, Hole](SrcHole)))
      val rlp11 = qsu.autojoin2((rlp7, rlp10, _(MapFuncsCore.ConcatArrays(_, _))))
      val rlp12 = qsu.groupBy(rlp0, rlp11)

      val qgraph = QSUGraph.fromTree[Fix](rlp12)

      eval(rw(qgraph)).foldMapUp(_ => 0) mustEqual 0
    }
  }

  def eval[A](fa: F[A]): A = fa.eval(0L).value
}
