/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.qscript.qsu

import slamdata.Predef._
import quasar.RenderTree
import quasar.fp.symbolOrder

import monocle.{PLens, Prism, Traversal}
import scalaz.{Applicative, Apply, Equal, Order, Show, Traverse1}
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.applicative._
import scalaz.syntax.show._

/** Describes access to the value and identity of `A`. */
sealed abstract class Access[A] {
  /** Surfaces symbols accessed, using the provided function in the `Value`
    * case.
    */
  def symbolic(value: A => Symbol): Access[Symbol] =
    this match {
      case Access.Bucket(s, i, _) => Access.bucket(s, i, s)
      case Access.Identity(s, _)  => Access.identity(s, s)
      case Access.Value(a)        => Access.value(value(a))
    }
}

object Access extends AccessInstances {
  final case class Bucket[A](of: Symbol, idx: Int, src: A) extends Access[A]
  final case class Identity[A](of: Symbol, src: A) extends Access[A]
  final case class Value[A](src: A) extends Access[A]

  def bucket[A]: Prism[Access[A], (Symbol, Int, A)] =
    Prism.partial[Access[A], (Symbol, Int, A)] {
      case Bucket(s, i, a) => (s, i, a)
    } { case (s, i, a) => Bucket(s, i, a) }

  def identity[A]: Prism[Access[A], (Symbol, A)] =
    Prism.partial[Access[A], (Symbol, A)] {
      case Identity(s, a) => (s, a)
    } { case (s, a) => Identity(s, a) }

  def src[A, B]: PLens[Access[A], Access[B], A, B] =
    PLens[Access[A], Access[B], A, B] {
      case Bucket(_, _, a) => a
      case Identity(_, a)  => a
      case Value(a)        => a
    } { b => {
      case Bucket(s, i, _) => Bucket(s, i, b)
      case Identity(s, _)  => Identity(s, b)
      case Value(_)        => Value(b)
    }}

  def symbols[A]: Traversal[Access[A], Symbol] =
    new Traversal[Access[A], Symbol] {
      def modifyF[F[_]: Applicative](f: Symbol => F[Symbol])(a: Access[A]) =
        a match {
          case Bucket(s, i, a) => f(s) map (Bucket(_, i, a))
          case Identity(s, a)  => f(s) map (Identity(_, a))
          case Value(a)        => value(a).point[F]
        }
    }

  def value[A]: Prism[Access[A], A] =
    Prism.partial[Access[A], A] {
      case Value(a) => a
    } (Value(_))

  implicit def renderTree[A: Show]: RenderTree[Access[A]] =
    RenderTree.fromShowAsType("Access")
}

sealed abstract class AccessInstances extends AccessInstances0 {
  import Access._

  implicit val traverse1: Traverse1[Access] =
    new Traverse1[Access] {
      def foldMapRight1[A, B](fa: Access[A])(z: A => B)(f: (A, => B) => B) =
        z(src[A, B].get(fa))

      def traverse1Impl[F[_]: Apply, A, B](fa: Access[A])(f: A => F[B]) =
        src[A, B].modifyF(f)(fa)
    }

  implicit def order[A: Order]: Order[Access[A]] =
    Order.orderBy(generic(_))

  implicit def show[A: Show]: Show[Access[A]] =
    Show.shows {
      case Bucket(s, i, a) => s"Bucket($s[$i], ${a.shows})"
      case Identity(s, a)  => s"Identity($s, ${a.shows})"
      case Value(a)        => s"Value(${a.shows})"
    }
}

sealed abstract class AccessInstances0 {
  import Access._

  implicit def equal[A: Equal]: Equal[Access[A]] =
    Equal.equalBy(generic(_))

  protected def generic[A](a: Access[A]) =
    (bucket.getOption(a), identity.getOption(a), value.getOption(a))
}
