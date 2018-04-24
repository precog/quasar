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

import slamdata.Predef.{StringContext, Symbol}
import quasar.RenderTree

import monocle.{Lens, PLens, Prism}
import scalaz.{Apply, Equal, Order, Show, Traverse1}
import scalaz.syntax.show._
import scalaz.std.option._
import scalaz.std.tuple._
import quasar.qscript.{Hole, JoinSide}

sealed abstract class Access[D, A] {
  def symbolic(value: A => Symbol): Access[D, Symbol] =
    this match {
      case Access.Id(i, a) =>
        IdAccess.symbols[D]
          .headOption(i)
          .fold(Access.id(i, value(a)))(Access.id(i, _))

      case Access.Value(a) =>
        Access.value(value(a))
    }
}

object Access extends AccessInstances {
  def identityHole[D]: Prism[Access[D, Hole], (IdAccess[D], Hole)] = id[D, Hole]
  def identitySymbol[D]: Prism[Access[D, Symbol], (IdAccess[D], Symbol)] = id[D, Symbol]
  def identityJoinSide[D]: Prism[Access[D, JoinSide], (IdAccess[D], JoinSide)] = id[D, JoinSide]
  def valueHole[D]: Prism[Access[D, Hole], Hole] = value[D, Hole]
  def valueSymbol[D]: Prism[Access[D, Symbol], Symbol] = value[D, Symbol]
  def valueJoinSide[D]: Prism[Access[D, JoinSide], JoinSide] = value[D, JoinSide]

  final case class Id[D, A](idAccess: IdAccess[D], src: A) extends Access[D, A]
  final case class Value[D, A](src: A) extends Access[D, A]

  def id[D, A]: Prism[Access[D, A], (IdAccess[D], A)] =
    Prism.partial[Access[D, A], (IdAccess[D], A)] {
      case Id(i, a) => (i, a)
    } { case (i, a) => Id(i, a) }

  def value[D, A]: Prism[Access[D, A], A] =
    Prism.partial[Access[D, A], A] {
      case Value(a) => a
    } (Value(_))

  def src[D, A]: Lens[Access[D, A], A] =
    srcP[D, A, A]

  def srcP[D, A, B]: PLens[Access[D, A], Access[D, B], A, B] =
    PLens[Access[D, A], Access[D, B], A, B] {
      case Id(_, a) => a
      case Value(a) => a
    } { b => {
      case Id(i, _) => id(i, b)
      case Value(_) => value(b)
    }}
}

sealed abstract class AccessInstances extends AccessInstances0 {
  implicit def traverse1[D]: Traverse1[Access[D, ?]] =
    new Traverse1[Access[D, ?]] {
      def traverse1Impl[G[_]: Apply, A, B](fa: Access[D, A])(f: A => G[B]) =
        Access.srcP[D, A, B].modifyF(f)(fa)

      def foldMapRight1[A, B](fa: Access[D, A])(z: A => B)(f: (A, => B) => B) =
        z(Access.src[D, A] get fa)
    }

  implicit def order[D: Order, A: Order]: Order[Access[D, A]] =
    Order.orderBy(generic)

  implicit def renderTree[D: Show, A: Show]: RenderTree[Access[D, A]] =
    RenderTree.fromShowAsType("Access")

  implicit def show[D: Show, A: Show]: Show[Access[D, A]] =
    Show.shows {
      case Access.Id(i, a) => s"Id(${i.shows}, ${a.shows})"
      case Access.Value(a) => s"Value(${a.shows})"
    }
}

sealed abstract class AccessInstances0 {
  implicit def equal[D: Equal, A: Equal]: Equal[Access[D, A]] =
    Equal.equalBy(generic)

  protected def generic[D, A](a: Access[D, A]) =
    (Access.id.getOption(a), Access.value.getOption(a))
}
