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

import slamdata.Predef._

import quasar.common.effect.NameGenerator
import quasar.contrib.pathy.AFile
import quasar.qsu.{QScriptUniform => QSU}
import quasar.qscript.{HoleR, MFC, MapFuncsCore}
  import quasar.qscript.construction.RecFunc
import quasar.IdStatus, IdStatus._

import matryoshka.{BirecursiveT, ShowT}

import scalaz.{Monad, Applicative, IMap, Cord}
import scalaz.std.list._
import scalaz.syntax.show._
import scalaz.syntax.monad._

object RewriteIdsFunction {
  import QSUGraph.Extractors._

  final case class ToRewrite(
    idsSymbol: Symbol,
    readSymbol: Symbol,
    newSymbol: Symbol,
    path: AFile
  )

  private def collectRewrites[F[_]: Monad: NameGenerator, T[_[_]]: BirecursiveT](graph: QSUGraph[T]) =
    graph.foldMapDownM { g =>
      g.unfold match {
        case QSU.GetIds(inner) => inner.unfold match {
          case QSU.LPRead(path) =>
            NameGenerator[F].prefixedName("rwids").map { newName =>
              ToRewrite(g.root, inner.root, Symbol(newName), path).point[List]
            }
          case _ => List[ToRewrite]().point[F]
        }
        case _ => List[ToRewrite]().point[F]
      }
    }

  private def rewriteLPRead[T[_[_]]: BirecursiveT](rws: List[ToRewrite], graph: QSUGraph[T]): QSUGraph[T] =
    rws.foldLeft(graph) { (inpGraph: QSUGraph[T], inp: ToRewrite) => inpGraph rewrite {
      case g if g.root == inp.readSymbol =>
        QSUGraph(g.root, g.vertices.updated(inp.newSymbol, QSU.Read(inp.path, IncludeId)))
          .overwriteAtRoot(QSU.Map(inp.newSymbol, RecFunc[T].ProjectIndexI(HoleR, 1)))
    }}

  private def rewriteIds[T[_[_]]: BirecursiveT](rws: List[ToRewrite], graph: QSUGraph[T]): QSUGraph[T] =
    rws.foldLeft(graph) { (inpGraph: QSUGraph[T], inp: ToRewrite) => inpGraph rewrite {
      case g if g.root == inp.idsSymbol =>
        scala.Predef.println(g)
        g.overwriteAtRoot(QSU.Map(inp.newSymbol, RecFunc[T].ProjectIndexI(HoleR, 0)))
    }}

//  private def rewriteLPReads[T[_[_]]: BirecursiveT](graph: QSUGraph[T]): QSUGraph[T] =
//    graph rewrite { case g@LPRead(path) => g.overwriteAtRoot(QSU.Read(path, ExcludeId)) }


  def apply[
      T[_[_]]: BirecursiveT: ShowT,
      F[_]: Monad: NameGenerator](
      qgraph: QSUGraph[T])
      : F[QSUGraph[T]] = {

    collectRewrites[F, T](qgraph) map { rws =>
      rewriteIds(rws, rewriteLPRead(rws, qgraph))
//      rewriteLPReads(rewriteIds(rws, rewriteLPRead(rws, qgraph)))
    }
  }
}
