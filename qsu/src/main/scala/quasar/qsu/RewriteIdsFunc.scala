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
import quasar.IdStatus, IdStatus._

import matryoshka.{BirecursiveT, ShowT}

import scalaz.{Monad, Applicative}
import scalaz.std.list._
import scalaz.syntax.monad._

object RewriteIdsFunction {
  final case class ToRewrite(
    idsSymbol: Symbol,
    readSymbol: Symbol,
    newSymbol: Symbol,
    idStatus: IdStatus,
    path: AFile
  )

  def apply[
      T[_[_]]: BirecursiveT: ShowT,
      F[_]: Applicative: Monad: NameGenerator](
      qgraph: QSUGraph[T])
      : F[QSUGraph[T]] = {

    val func = quasar.qscript.construction.RecFunc[T]

    val rewrites: F[List[ToRewrite]] = qgraph.foldMapDownM { graph =>
      graph.unfold match {
        case QSU.Unary(inner, MFC(MapFuncsCore.Ids(_))) => inner.unfold match {
          case QSU.Read(path, idStatus) =>
            NameGenerator[F].prefixedName("rwids").map { newName =>
              ToRewrite(graph.root, inner.root, Symbol(newName), idStatus, path).point[List]
            }
          case _ => List[ToRewrite]().point[F]
        }
        case _ => List[ToRewrite]().point[F]
      }
    }

    rewrites.map { rws =>
      rws.foldLeft(qgraph){ (graph: QSUGraph[T], inp: ToRewrite) => inp.idStatus match {
        case IdOnly => {
          graph.replace(inp.idsSymbol, inp.readSymbol)
        }
        case IncludeId => {
          QSUGraph(
            graph.root,
            graph.vertices.updated(inp.idsSymbol, QSU.Map(inp.readSymbol, func.ProjectIndexI(HoleR, 0)))
          )
        }
        case ExcludeId => {
          QSUGraph(
            graph.root,
            graph.vertices
              .updated(inp.newSymbol, QSU.Read(inp.path, IncludeId))
              .updated(inp.readSymbol, QSU.Map(inp.newSymbol, func.ProjectIndexI(HoleR, 1)))
              .updated(inp.idsSymbol, QSU.Map(inp.newSymbol, func.ProjectIndexI(HoleR, 0)))
          )
        }
      }}
    }
  }
}
