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

import slamdata.Predef.{Array, Byte, Product, Serializable, SuppressWarnings, List}

import quasar.{NonTerminal, RenderTree, ScalarStages}, RenderTree.ops._
import quasar.higher.HFunctor

import fs2.Stream

import monocle.Lens

import qdata.QDataDecode

import scalaz.{~>, Show}
import scalaz.std.option._
import scalaz.syntax.show._

import shims._

sealed trait QueryResult[F[_]] extends Product with Serializable {

  def data: Stream[F, _]

  def stages: ScalarStages

  def modifyData[G[_]](f: Stream[F, ?] ~> Stream[G, ?]): QueryResult[G]

  def hmap[G[_]](f: F ~> G): QueryResult[G] =
    modifyData(λ[Stream[F, ?] ~> Stream[G, ?]](_.translate[F, G](f.asCats)))
}

object QueryResult extends QueryResultInstances {

  final case class Parsed[F[_], A](decode: QDataDecode[A], data: Stream[F, A], stages: ScalarStages)
      extends QueryResult[F] {

    def modifyData[G[_]](f: Stream[F, ?] ~> Stream[G, ?]): QueryResult[G] =
      Parsed(decode, f(data), stages)
  }

  sealed trait Unparsed[F[_]] extends QueryResult[F] {
    def data: Stream[F, Byte]

    def stages: ScalarStages

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def modifyBytes[G[_]](f: Stream[F, Byte] => Stream[G, Byte]): Unparsed[G] =
      this match {
        case Compressed(s, c) => Compressed(s, c.modifyBytes(f))
        case Typed(t, d, ss) => Typed(t, f(d), ss)
      }

    def modifyData[G[_]](f: Stream[F, ?] ~> Stream[G, ?]): Unparsed[G] =
      modifyBytes[G](f(_))
  }

  object Unparsed {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def stages[F[_]]: Lens[Unparsed[F], ScalarStages] =
      Lens((_: Unparsed[F]).stages)(ss => {
        case Compressed(s, u) => Compressed(s, stages[F].set(ss)(u))
        case Typed(t, d, _) => Typed(t, d, ss)
      })

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    implicit def renderTree[F[_]]: RenderTree[Unparsed[F]] =
      RenderTree make {
        case Compressed(s, c) => NonTerminal(List("Compressed"), some(c.shows), List(c.render))
        case Typed(t, _, ss) => NonTerminal(List("Typed"), some(t.shows), List(ss.render))
      }

    implicit def show[F[_]]: Show[Unparsed[F]] =
      RenderTree.toShow
  }

  final case class Compressed[F[_]](scheme: CompressionScheme, content: Unparsed[F])
      extends Unparsed[F] {

    def data = content.data
    def stages = content.stages
  }

  final case class Typed[F[_]](tpe: ParsableType, data: Stream[F, Byte], stages: ScalarStages)
      extends Unparsed[F]

  def compressed[F[_]](scheme: CompressionScheme, content: Unparsed[F]): Unparsed[F] =
    Compressed(scheme, content)

  def parsed[F[_], A](q: QDataDecode[A], d: Stream[F, A], ss: ScalarStages): QueryResult[F] =
    Parsed(q, d, ss)

  def typed[F[_]](tpe: ParsableType, data: Stream[F, Byte], ss: ScalarStages): Unparsed[F] =
    Typed(tpe, data, ss)

  def stages[F[_]]: Lens[QueryResult[F], ScalarStages] =
    Lens((_: QueryResult[F]).stages)(ss => {
      case Parsed(q, d, _) => Parsed(q, d, ss)
      case u: Unparsed[F] => Unparsed.stages.set(ss)(u)
    })
}

sealed abstract class QueryResultInstances {
  import QueryResult._

  implicit val hfunctor: HFunctor[QueryResult] =
    new HFunctor[QueryResult] {
      def hmap[A[_], B[_]](fa: QueryResult[A])(f: A ~> B) =
        fa hmap f
    }

  implicit def renderTree[F[_]]: RenderTree[QueryResult[F]] =
    RenderTree make {
      case Parsed(_, _, ss) => NonTerminal(List("Parsed"), none, List(ss.render))
      case u: Unparsed[F] => u.render
    }

  implicit def show[F[_]]: Show[QueryResult[F]] =
    RenderTree.toShow
}
