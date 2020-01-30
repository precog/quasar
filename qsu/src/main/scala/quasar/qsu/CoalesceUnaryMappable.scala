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

import slamdata.Predef.{Int, List, Map => SMap, Symbol}
import quasar.fp._
import quasar.qscript.{
  construction,
  Center,
  LeftSide,
  LeftSide3,
  RightSide,
  RightSide3
}
import quasar.qscript.RecFreeS._
import quasar.qsu.{QScriptUniform => QSU}

import scalaz.syntax.bind._
import scalaz.syntax.equal._

import matryoshka.BirecursiveT

/** Coalesces adjacent mappable regions of a single root. */
final class CoalesceUnaryMappable[T[_[_]]: BirecursiveT] private () extends QSUTTypes[T] {
  import QSUGraph.Extractors._
  import MappableRegion.MaximalUnary

  val mf = construction.Func[T]

  def apply(graph: QSUGraph): QSUGraph =
    graph rewrite {
      case g @ MaximalUnary(src, fm) if g.root =/= src.root =>
        g.overwriteAtRoot(QScriptUniform.Map(src.root, fm.asRec))

      case g @ AutoJoin2(left, right, combine) =>
        val nodes = mapNodes(List(left, right))

        if (nodes.isEmpty)
          g
        else {
          val (l, lf) = nodes.getOrElse(0, (left.root, mf.Hole))
          val (r, rf) = nodes.getOrElse(1, (right.root, mf.Hole))

          val cmb = combine flatMap {
            case LeftSide => lf >> mf.LeftSide
            case RightSide => rf >> mf.RightSide
          }

          g.overwriteAtRoot(QSU.AutoJoin2(l, r, cmb))
        }

      case g @ AutoJoin3(left, center, right, combine) =>
        val nodes = mapNodes(List(left, center, right))

        if (nodes.isEmpty)
          g
        else {
          val (l, lf) = nodes.getOrElse(0, (left.root, mf.Hole))
          val (c, cf) = nodes.getOrElse(1, (center.root, mf.Hole))
          val (r, rf) = nodes.getOrElse(2, (right.root, mf.Hole))

          val cmb = combine flatMap {
            case LeftSide3 => lf >> mf.LeftSide3
            case Center => cf >> mf.Center
            case RightSide3 => rf >> mf.RightSide3
          }

          g.overwriteAtRoot(QSU.AutoJoin3(l, c, r, cmb))
        }
    }

  def mapNodes(gs: List[QSUGraph]): SMap[Int, (Symbol, FreeMap)] =
    gs.zipWithIndex.foldLeft(SMap[Int, (Symbol, FreeMap)]()) {
      case (acc, (Map(s, fm), i)) => acc.updated(i, (s.root, fm.linearize))
      case (acc, _) => acc
    }
}

object CoalesceUnaryMappable {
  def apply[T[_[_]]: BirecursiveT](graph: QSUGraph[T]): QSUGraph[T] =
    (new CoalesceUnaryMappable[T]).apply(graph)
}
