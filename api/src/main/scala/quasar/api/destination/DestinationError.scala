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

package quasar.api.destination

import slamdata.Predef._

import monocle.Prism
import scalaz.std.string._
import scalaz.syntax.equal._
import scalaz.syntax.show._
import scalaz.{Cord, Equal, ISet, NonEmptyList, Show}

sealed trait DestinationError[+I, +C] extends Product with Serializable

object DestinationError {
  sealed trait CreateError[+C] extends DestinationError[Nothing, C]
  final case class DestinationUnsupported(kind: DestinationType, supported: ISet[DestinationType])
      extends CreateError[Nothing]
  final case class DestinationNameExists(name: DestinationName)
      extends CreateError[Nothing]

  sealed trait InitializationError[+C] extends CreateError[C]
  final case class MalformedConfiguration[C](kind: DestinationType, config: C, reason: String)
      extends InitializationError[C]
  final case class InvalidConfiguration[C](kind: DestinationType, config: C, reasons: NonEmptyList[String])
      extends InitializationError[C]
  final case class ConnectionFailed[C](kind: DestinationType, config: C, cause: Exception)
      extends InitializationError[C]
  final case class AccessDenied[C](kind: DestinationType, config: C, reason: String)
      extends InitializationError[C]

  sealed trait ExistentialError[+I] extends DestinationError[I, Nothing]
  final case class DestinationNotFound[I](destinationId: I)
      extends ExistentialError[I]

  def destinationUnsupported[E >: CreateError[Nothing] <: DestinationError[_, _]]
      : Prism[E, (DestinationType, ISet[DestinationType])] =
    Prism.partial[E, (DestinationType, ISet[DestinationType])] {
      case DestinationUnsupported(k, s) => (k, s)
    } (DestinationUnsupported.tupled)

  def destinationNameExists[E >: CreateError[Nothing] <: DestinationError[_, _]]
      : Prism[E, DestinationName] =
    Prism.partial[E, DestinationName] {
      case DestinationNameExists(n) => n
    } (DestinationNameExists(_))

  def malformedConfiguration[C, E >: InitializationError[C] <: DestinationError[_, C]]
      : Prism[E, (DestinationType, C, String)] =
    Prism.partial[E, (DestinationType, C, String)] {
      case MalformedConfiguration(d, c, r) => (d, c, r)
    } ((MalformedConfiguration[C] _).tupled)

  def invalidConfiguration[C, E >: InitializationError[C] <: DestinationError[_, C]]
      : Prism[E, (DestinationType, C, NonEmptyList[String])] =
    Prism.partial[E, (DestinationType, C, NonEmptyList[String])] {
      case InvalidConfiguration(d, c, rs) => (d, c, rs)
    } ((InvalidConfiguration[C] _).tupled)

  def connectionFailed[C, E >: InitializationError[C] <: DestinationError[_, C]]
      : Prism[E, (DestinationType, C, Exception)] =
    Prism.partial[E, (DestinationType, C, Exception)] {
      case ConnectionFailed(d, c, e) => (d, c, e)
    } ((ConnectionFailed[C] _).tupled)

  def accessDenied[C, E >: InitializationError[C] <: DestinationError[_, C]]
      : Prism[E, (DestinationType, C, String)] =
    Prism.partial[E, (DestinationType, C, String)] {
      case AccessDenied(d, c, e) => (d, c, e)
    } ((AccessDenied[C] _).tupled)

  def destinationNotFound[I, E >: ExistentialError[I] <: DestinationError[I, _]]
      : Prism[E, I] =
    Prism.partial[E, I] {
      case DestinationNotFound(i) => i
    } (DestinationNotFound[I](_))

  implicit def equalCreateError[C: Equal]: Equal[CreateError[C]] =
    Equal.equal {
      case (DestinationUnsupported(k1, s1), DestinationUnsupported(k2, s2)) =>
        k1 === k2 && s1 === s2
      case (DestinationNameExists(n1), DestinationNameExists(n2)) =>
        n1 === n2
      case (e1: InitializationError[C], e2: InitializationError[C]) =>
        Equal[InitializationError[C]].equal(e1, e2)
      case _ =>
        false
    }

  implicit def equalInitializationError[C: Equal]: Equal[InitializationError[C]] =
    Equal.equal {
      case (MalformedConfiguration(k1, c1, r1), MalformedConfiguration(k2, c2, r2)) =>
        k1 === k2 && c1 === c2 && r1 === r2
      case (InvalidConfiguration(k1, c1, rs1), InvalidConfiguration(k2, c2, rs2)) =>
        k1 === k2 && c1 === c2 && rs1 === rs2
      case (ConnectionFailed(k1, c1, _), ConnectionFailed(k2, c2, _)) =>
        // ignore exceptions
        k1 === k2 && c1 === c2
      case (AccessDenied(k1, c1, r1), AccessDenied(k2, c2, r2)) =>
        k1 === k2 && c1 === c2 && r1 === r2
      case _ =>
        false
    }

  implicit def equalExistentialError[I: Equal]: Equal[ExistentialError[I]] =
    Equal.equal {
      case (DestinationNotFound(i1), DestinationNotFound(i2)) =>
        i1 === i2
    }

  implicit def equal[I: Equal, C: Equal]: Equal[DestinationError[I, C]] =
    Equal.equal {
      case (e1: CreateError[C], e2: CreateError[C]) =>
        (e1, e2) match {
          case (e1: InitializationError[C], e2: InitializationError[C]) =>
            Equal[InitializationError[C]].equal(e1, e2)
          case (e1: CreateError[C], e2: CreateError[C]) =>
            Equal[CreateError[C]].equal(e1, e2)
        }
      case (e1: ExistentialError[I], e2: ExistentialError[I]) =>
        Equal[ExistentialError[I]].equal(e1, e2)
      case _ =>
        false
    }

  implicit def showCreateError[C: Show]: Show[CreateError[C]] =
    Show.show {
      case e: InitializationError[C] =>
        Show[InitializationError[C]].show(e)

      case DestinationUnsupported(kind, supported) =>
        Cord("DestinationUnsupported(") ++ kind.show ++ Cord(", ") ++ supported.show ++ Cord(")")

      case DestinationNameExists(name) =>
        Cord("DestinationNameExists(") ++ name.show ++ Cord(")")
    }

  implicit def showInitializationError[C: Show]: Show[InitializationError[C]] =
    Show.show {
      case MalformedConfiguration(kind, config, reason) =>
        Cord("MalformedConfiguration(") ++ kind.show ++ Cord(", ") ++ config.show ++ Cord(", ") ++ Cord(reason) ++ Cord(")")

      case InvalidConfiguration(kind, config, reasons) =>
        Cord("InvalidConfiguration(") ++ kind.show ++ Cord(", ") ++ config.show ++ Cord(", ") ++ reasons.show ++ Cord(")")

      case ConnectionFailed(kind, config, ex) =>
        Cord("ConnectionFailed(") ++ kind.show ++ Cord(", ") ++ config.show ++ Cord(s")\n$ex")

      case AccessDenied(kind, config, reason) =>
        Cord("AccessDenied(") ++ kind.show ++ Cord(", ") ++ config.show ++ Cord(", ") ++ reason.show ++ Cord(")")
    }

  implicit def showExistentialError[I: Show]: Show[ExistentialError[I]] =
    Show.show {
      case DestinationNotFound(id) =>
        Cord("DestinationNotFound(") ++ id.show ++ Cord(")")
    }

  implicit def show[I: Show, C: Show]: Show[DestinationError[I, C]] =
    Show.show {
      case e: CreateError[C] => e match {
        case e: InitializationError[C] =>
          Show[InitializationError[C]].show(e)
        case e =>
          Show[CreateError[C]].show(e)
      }
      case e: ExistentialError[I] => e match {
        case DestinationNotFound(id) =>
          Cord("DestinationNotFound(") ++ id.show ++ Cord(")")
      }
    }
}
