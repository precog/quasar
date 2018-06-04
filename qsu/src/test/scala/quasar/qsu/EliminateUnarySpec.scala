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

import quasar.{Data, Qspec, Type}
import quasar.ejson.{EJson, Fixed}
import quasar.qscript.{
  Hole,
  MapFuncsCore,
  SrcHole,
  Take
}
import quasar.qsu.{QScriptUniform => QSU}
import slamdata.Predef._

import matryoshka.data.Fix
import org.specs2.matcher.Matcher
import org.specs2.matcher.MatchersImplicits._
import iotaz.CopK

object EliminateUnarySpec extends Qspec with QSUTTypes[Fix] {
  import QSUGraph.Extractors._

  val qsu = QScriptUniform.DslT[Fix]
  val elim = EliminateUnary[Fix] _

  val IC = CopK.Inject[MapFuncCore, MapFunc]

  val J = Fixed[Fix[EJson]]

  "unary node elimination" should {
    "eliminate a single unary node" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.unary(qsu.unreferenced(), IC(MapFuncsCore.Negate(SrcHole: Hole))))

      elim(qgraph) must beLike {
        case Map(Unreferenced(), _) => ok
      }
    }

    "eliminate unary around a read" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.unary(qsu.tread1("foo"), IC(MapFuncsCore.Negate(SrcHole: Hole))))

      elim(qgraph) must beLike {
        case Map(
          Transpose(Read(_), QSU.Retain.Values, QSU.Rotation.ShiftMap),
          FMFC1(MapFuncsCore.Negate(SrcHole))) => ok
      }
    }

    "eliminate unary around a read within a subset" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.subset(
          qsu.unary(
            qsu.tread1("foo"),
            IC(MapFuncsCore.Negate(SrcHole: Hole))),
          Take,
          qsu.cint(11)))

      elim(qgraph) must beLike {
        case Subset(
          Map(
            Transpose(Read(_), QSU.Retain.Values, QSU.Rotation.ShiftMap),
            FMFC1(MapFuncsCore.Negate(SrcHole))),
          Take,
          DataConstantMapped(Data.Int(i))) => i mustEqual 11
      }
    }

    "eliminate unary on two branches of an autojoin3" in {
      val tmp1 =
        qsu.unary(
          qsu.unreferenced(),
          IC(MapFuncsCore.Constant[Fix, Hole](J.int(1))))

      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin3((
          tmp1,
          tmp1,
          qsu.unreferenced(),
          _(MapFuncsCore.Guard(_, Type.AnyObject, _, _)))))

      elim(qgraph) must not(containUnary)
    }
  }

  val containUnary: Matcher[QSUGraph] = { graph: QSUGraph =>
    val nodes = graph.vertices.values collect {
      case u @ QSU.Unary(_, _) => u
    }

    (!nodes.isEmpty, s"$graph contains unary nodes: $nodes", s"$graph does not contain any unary nodes")
  }
}
