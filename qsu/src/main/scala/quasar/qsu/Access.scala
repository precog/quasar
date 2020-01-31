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

import slamdata.Predef.{StringContext, Symbol}
import quasar.RenderTree

import monocle.{Lens, PLens, Prism}
import scalaz.{Apply, Equal, Order, Show, Traverse1}
import scalaz.syntax.show._
import scalaz.std.option._
import scalaz.std.tuple._

sealed abstract class Access[A] {
  def symbolic(value: A => Symbol): Access[Symbol] =
    this match {
      case Access.Id(i, a) =>
        IdAccess.symbols
          .headOption(i)
          .fold(Access.id(i, value(a)))(Access.id(i, _))

      case Access.Value(a) =>
        Access.value(value(a))
    }
}

object Access extends AccessInstances {

  final case class Id[A](idAccess: IdAccess, src: A) extends Access[A]
  final case class Value[A](src: A) extends Access[A]

  def id[A]: Prism[Access[A], (IdAccess, A)] =
    Prism.partial[Access[A], (IdAccess, A)] {
      case Id(i, a) => (i, a)
    } { case (i, a) => Id(i, a) }

  def value[A]: Prism[Access[A], A] =
    Prism.partial[Access[A], A] {
      case Value(a) => a
    } (Value(_))

  def src[A]: Lens[Access[A], A] =
    srcP[A, A]

  def srcP[A, B]: PLens[Access[A], Access[B], A, B] =
    PLens[Access[A], Access[B], A, B] {
      case Id(_, a) => a
      case Value(a) => a
    } { b => {
      case Id(i, _) => id(i, b)
      case Value(_) => value(b)
    }}
}

sealed abstract class AccessInstances extends AccessInstances0 {
  implicit val traverse1: Traverse1[Access] =
    new Traverse1[Access] {
      def traverse1Impl[G[_]: Apply, A, B](fa: Access[A])(f: A => G[B]) =
        Access.srcP[A, B].modifyF(f)(fa)

      def foldMapRight1[A, B](fa: Access[A])(z: A => B)(f: (A, => B) => B) =
        z(Access.src[A] get fa)
    }

  implicit def order[A: Order]: Order[Access[A]] =
    Order.orderBy(generic)

  implicit def renderTree[A: Show]: RenderTree[Access[A]] =
    RenderTree.fromShowAsType("Access")

  implicit def show[A: Show]: Show[Access[A]] =
    Show.shows {
      case Access.Id(i, a) => s"Id(${i.shows}, ${a.shows})"
      case Access.Value(a) => s"Value(${a.shows})"
    }
}

sealed abstract class AccessInstances0 {
  implicit def equal[A: Equal]: Equal[Access[A]] =
    Equal.equalBy(generic)

  protected def generic[A](a: Access[A]) =
    (Access.id.getOption(a), Access.value.getOption(a))
}
