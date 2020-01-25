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

package quasar.connector

import slamdata.Predef._

import quasar.{NonTerminal, RenderTree, ScalarStages}, RenderTree.ops._

import cats.~>
import cats.implicits._

import fs2.Stream

import monocle.Lens

import qdata.QDataDecode

import scalaz.Show
import scalaz.syntax.show._

import shims.showToScalaz

import tectonic.Plate

sealed trait QueryResult[F[_]] extends Product with Serializable {

  def stages: ScalarStages

  def mapK[G[_]](f: F ~> G): QueryResult[G]
}

object QueryResult extends QueryResultInstances {

  final case class Parsed[F[_], A](
      decode: QDataDecode[A],
      data: Stream[F, A],
      stages: ScalarStages)
      extends QueryResult[F] {

    def mapK[G[_]](f: F ~> G): QueryResult[G] =
      Parsed[G, A](decode, data.translate[F, G](f), stages)
  }

  final case class Typed[F[_]](
      format: DataFormat,
      data: Stream[F, Byte],
      stages: ScalarStages)
      extends QueryResult[F] {

    def mapK[G[_]](f: F ~> G): QueryResult[G] =
      Typed[G](format, data.translate[F, G](f), stages)
  }

  final case class Stateful[F[_], P <: Plate[Unit], S](
      format: DataFormat,
      plateF: F[P],
      state: P => F[Option[S]],
      data: Option[S] => Stream[F, Byte],
      stages: ScalarStages)
      extends QueryResult[F] {

    def mapK[G[_]](f: F ~> G): QueryResult[G] =
      Stateful[G, P, S](
        format,
        f(plateF),
        p => f(state(p)),
        data(_).translate[F, G](f),
        stages)
  }

  def parsed[F[_], A](q: QDataDecode[A], d: Stream[F, A], ss: ScalarStages)
      : QueryResult[F] =
    Parsed(q, d, ss)

  def typed[F[_]](tpe: DataFormat, data: Stream[F, Byte], ss: ScalarStages)
      : QueryResult[F] =
    Typed(tpe, data, ss)

  def stateful[F[_], P <: Plate[Unit], S](
      format: DataFormat,
      plateF: F[P],
      state: P => F[Option[S]],
      data: Option[S] => Stream[F, Byte],
      stages: ScalarStages)
      : QueryResult[F] =
    Stateful(format, plateF, state, data, stages)

  def stages[F[_]]: Lens[QueryResult[F], ScalarStages] =
    Lens((_: QueryResult[F]).stages)(ss => {
      case Parsed(q, d, _) => Parsed(q, d, ss)
      case Typed(f, d, _) => Typed(f, d, ss)
      case Stateful(f, p, s, d, _) => Stateful(f, p, s, d, ss)
    })
}

sealed abstract class QueryResultInstances {
  import QueryResult._

  implicit def renderTree[F[_]]: RenderTree[QueryResult[F]] =
    RenderTree make {
      case Parsed(_, _, ss) =>
        NonTerminal(List("Parsed"), none, List(ss.render))
      case Typed(f, _, ss) =>
        NonTerminal(List("Typed"), none, List(f.shows.render, ss.render))
      case Stateful(f, _, _, _, ss) =>
        NonTerminal(List("Stateful"), none, List(f.shows.render, ss.render))
    }

  implicit def show[F[_]]: Show[QueryResult[F]] =
    RenderTree.toShow
}
