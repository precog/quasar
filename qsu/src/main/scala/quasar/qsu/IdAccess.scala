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
import quasar.RenderTree
import quasar.fp.symbolOrder

import matryoshka._
import monocle.{Prism, PPrism, Traversal}
import scalaz.{Applicative, Equal, Order, Show, Traverse}
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.applicative._
import scalaz.syntax.either._
import scalaz.syntax.show._

/** Describes access to the various forms of ids. */
sealed abstract class IdAccess[D]

object IdAccess extends IdAccessInstances {
  final case class Bucket[D](of: Symbol, idx: Int) extends IdAccess[D]
  final case class GroupKey[D](of: Symbol, idx: Int) extends IdAccess[D]
  final case class Identity[D](of: Symbol) extends IdAccess[D]
  final case class Static[D](data: D) extends IdAccess[D]

  def bucket[D]: Prism[IdAccess[D], (Symbol, Int)] =
    Prism.partial[IdAccess[D], (Symbol, Int)] {
      case Bucket(s, i) => (s, i)
    } { case (s, i) => Bucket(s, i) }

  def groupKey[D]: Prism[IdAccess[D], (Symbol, Int)] =
    Prism.partial[IdAccess[D], (Symbol, Int)] {
      case GroupKey(s, i) => (s, i)
    } { case (s, i) => GroupKey(s, i) }

  def identity[D]: Prism[IdAccess[D], Symbol] =
    Prism.partial[IdAccess[D], Symbol] {
      case Identity(s) => s
    } (Identity(_))

  def static[D]: Prism[IdAccess[D], D] =
    staticP[D, D]

  def staticP[D, E]: PPrism[IdAccess[D], IdAccess[E], D, E] =
    PPrism[IdAccess[D], IdAccess[E], D, E] {
      case Bucket(s, i)   => bucket[E](s, i).left
      case GroupKey(s, i) => groupKey[E](s, i).left
      case Identity(s)    => identity[E](s).left
      case Static(d)      => d.right
    } (Static(_))

  def symbols[D]: Traversal[IdAccess[D], Symbol] =
    new Traversal[IdAccess[D], Symbol] {
      def modifyF[F[_]: Applicative](f: Symbol => F[Symbol])(s: IdAccess[D]) =
        s match {
          case Bucket(s, i)   => f(s) map (bucket(_, i))
          case GroupKey(s, i) => f(s) map (groupKey(_, i))
          case Identity(s)    => f(s) map (identity(_))
          case Static(d)      => static(d).point[F]
        }
    }
}

sealed abstract class IdAccessInstances extends IdAccessInstances0 {
  import IdAccess._

  implicit def traverse: Traverse[IdAccess] =
    new Traverse[IdAccess] {
      def traverseImpl[F[_]: Applicative, A, B](a: IdAccess[A])(f: A => F[B]) =
        staticP[A, B].modifyF(f)(a)
    }

  implicit def order[D: Order]: Order[IdAccess[D]] =
    Order.orderBy(generic(_))

  implicit def renderTree[D: Show]: RenderTree[IdAccess[D]] =
    RenderTree.fromShowAsType("IdAccess")

  implicit def show[D: Show]: Show[IdAccess[D]] =
    Show.shows {
      case Bucket(s, i)   => s"Bucket($s[$i])"
      case GroupKey(s, i) => s"GroupKey($s[$i])"
      case Identity(s)    => s"Identity($s)"
      case Static(d)      => s"Static(${d.shows})"
    }
}

sealed abstract class IdAccessInstances0 {
  import IdAccess.{bucket, groupKey, identity, static}

  implicit def equal[D: Equal]: Equal[IdAccess[D]] =
    Equal.equalBy(generic(_))

  protected def generic[D](a: IdAccess[D]) =
    (bucket.getOption(a), groupKey.getOption(a), identity.getOption(a), static.getOption(a))
}
