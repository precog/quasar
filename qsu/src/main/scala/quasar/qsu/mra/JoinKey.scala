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

package quasar.qsu.mra

import slamdata.Predef.{Product, Serializable, StringContext}

import cats.{Eq, Order, Show}
import cats.instances.option._
import cats.instances.tuple._
import cats.syntax.show._

import monocle.{Prism, Traversal}

import scalaz.Applicative

sealed trait JoinKey[S, V] extends Product with Serializable {
  def flip: JoinKey[S, V] =
    this match {
      case JoinKey.Dynamic(l, r) => JoinKey.Dynamic(r, l)
      case JoinKey.StaticL(l, r) => JoinKey.StaticR(r, l)
      case JoinKey.StaticR(l, r) => JoinKey.StaticL(r, l)
    }
}

object JoinKey extends JoinKeyInstances {
  final case class Dynamic[S, V](l: V, r: V) extends JoinKey[S, V]
  final case class StaticL[S, V](s: S, v: V) extends JoinKey[S, V]
  final case class StaticR[S, V](v: V, s: S) extends JoinKey[S, V]

  def dynamic[S, V]: Prism[JoinKey[S, V], (V, V)] =
    Prism.partial[JoinKey[S, V], (V, V)] {
      case Dynamic(l, r) => (l, r)
    } { case (l, r) => Dynamic(l, r) }

  def staticL[S, V]: Prism[JoinKey[S, V], (S, V)] =
    Prism.partial[JoinKey[S, V], (S, V)] {
      case StaticL(s, v) => (s, v)
    } { case (s, v) => StaticL(s, v) }

  def staticR[S, V]: Prism[JoinKey[S, V], (V, S)] =
    Prism.partial[JoinKey[S, V], (V, S)] {
      case StaticR(v, s) => (v, s)
    } { case (v, s) => StaticR(v, s) }

  def vectorIds[S, V]: Traversal[JoinKey[S, V], V] =
    new Traversal[JoinKey[S, V], V] {
      import scalaz.syntax.applicative._
      def modifyF[F[_]: Applicative](f: V => F[V])(jk: JoinKey[S, V]) =
        jk match {
          case Dynamic(l, r) => (f(l) |@| f(r))(JoinKey.dynamic(_, _))
          case StaticL(s, v) => f(v).map(JoinKey.staticL(s, _))
          case StaticR(v, s) => f(v).map(JoinKey.staticR(_, s))
        }
    }
}

sealed abstract class JoinKeyInstances extends JoinKeyInstances0 {
  implicit def order[S: Order, V: Order]: Order[JoinKey[S, V]] =
    Order.by(generic(_))

  implicit def show[S: Show, V: Show]: Show[JoinKey[S, V]] =
    Show show {
      case JoinKey.Dynamic(l, r) => s"Dynamic(${l.show}, ${r.show})"
      case JoinKey.StaticL(s, v) => s"StaticL(${s.show}, ${v.show})"
      case JoinKey.StaticR(v, s) => s"StaticR(${v.show}, ${s.show})"
    }
}

sealed abstract class JoinKeyInstances0 {
  implicit def equal[S: Eq, V: Eq]: Eq[JoinKey[S, V]] =
    Eq.by(generic(_))

  protected def generic[S, V](k: JoinKey[S, V]) =
    (
      JoinKey.dynamic.getOption(k),
      JoinKey.staticL.getOption(k),
      JoinKey.staticR.getOption(k)
    )
}
