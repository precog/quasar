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

package quasar.qsu.mra

import slamdata.Predef._
import quasar.fp.ski.κ

import cats.{Eq, Order, Show}
import cats.instances.int._
import cats.syntax.order._
import cats.syntax.show._

import monocle.{Prism, Traversal}

import scalaz.Applicative

/** A single dimension of an identity vector.
  *
  * @tparam S type of scalar identities
  * @tparam V type of vector identities
  * @tparam T sorts of identities
  */
sealed abstract class Dim[S, V, T]

object Dim extends DimInstances {
  final case class Fresh[S, V, T]() extends Dim[S, V, T]
  final case class Project[S, V, T](id: S, sort: T) extends Dim[S, V, T]
  final case class Inject[S, V, T](id: S, sort: T) extends Dim[S, V, T]
  final case class Inflate[S, V, T](id: V, sort: T) extends Dim[S, V, T]

  final class Optics[S, V, T] {
    val fresh: Prism[Dim[S, V, T], Unit] =
      Prism.partial[Dim[S, V, T], Unit] {
        case Fresh() => ()
      } (κ(Fresh()))

    val inflate: Prism[Dim[S, V, T], (V, T)] =
      Prism.partial[Dim[S, V, T], (V, T)] {
        case Inflate(v, t) => (v, t)
      } {
        case (v, t) => Inflate(v, t)
      }

    val inject: Prism[Dim[S, V, T], (S, T)] =
      Prism.partial[Dim[S, V, T], (S, T)] {
        case Inject(s, t) => (s, t)
      } {
        case (s, t) => Inject(s, t)
      }

    val project: Prism[Dim[S, V, T], (S, T)] =
      Prism.partial[Dim[S, V, T], (S, T)] {
        case Project(s, t) => (s, t)
      } {
        case (s, t) => Project(s, t)
      }

    val scalar: Traversal[Dim[S, V, T], (S, T)] =
      new Traversal[Dim[S, V, T], (S, T)] {
        import scalaz.syntax.applicative._

        val I = inject
        val P = project

        def modifyF[F[_]: Applicative](f: ((S, T)) => F[(S, T)])(d: Dim[S, V, T]) =
          d match {
            case I(s, t) => f((s, t)).map(I(_))
            case P(s, t) => f((s, t)).map(P(_))
            case other => other.point[F]
          }
      }
  }

  object Optics {
    def apply[S, V, T]: Optics[S, V, T] = new Optics[S, V, T]
  }
}

sealed abstract class DimInstances extends DimInstances0 {
  import Dim._

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  implicit def order[S: Order, V: Order, T: Order]: Order[Dim[S, V, T]] =
    Order from {
      case (l@Fresh(), r@Fresh()) =>
        if (l eq r) 0 else l.hashCode compare r.hashCode

      case (Fresh(), _) => -1
      case (_, Fresh()) => 1

      case (Inflate(v1, t1), Inflate(v2, t2)) =>
        (v1 compare v2) match {
          case 0 => t1 compare t2
          case x => x
        }

      case (Inflate(_, _), _) => -1
      case (_, Inflate(_, _)) => 1

      case (Inject(s1, t1), Inject(s2, t2)) =>
        (s1 compare s2) match {
          case 0 => t1 compare t2
          case x => x
        }

      case (Inject(_, _), _) => -1
      case (_, Inject(_, _)) => 1

      case (Project(s1, t1), Project(s2, t2)) =>
        (s1 compare s2) match {
          case 0 => t1 compare t2
          case x => x
        }
    }

  implicit def show[S: Show, V: Show, T: Show]: Show[Dim[S, V, T]] =
    Show show {
      case Fresh() => "∃"
      case Project(s, t) => s"∏[${s.show} :: ${t.show}]"
      case Inject(s, t) => s"∐[${s.show} :: ${t.show}]"
      case Inflate(v, t) => s"∆[${v.show} :: ${t.show}]"
    }
}

sealed abstract class DimInstances0 {
  import Dim._

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  implicit def equal[S: Eq, V: Eq, T: Eq]: Eq[Dim[S, V, T]] =
    Eq instance {
      case (l@Fresh(), r@Fresh()) => l eq r
      case (Inflate(v1, t1), Inflate(v2, t2)) => v1 === v2 && t1 === t2
      case (Inject(s1, t1), Inject(s2, t2)) => s1 === s2 && t1 === t2
      case (Project(s1, t1), Project(s2, t2)) => s1 === s2 && t1 === t2
      case _ => false
    }
}
