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

package quasar.api

import slamdata.Predef._

import scalaz.{Cord, Equal, ISet, NonEmptyList, Show}
import scalaz.std.option._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.show._

sealed trait DataSourceError[+C] extends Product with Serializable

object DataSourceError extends DataSourceErrorInstances {
  sealed trait CreateError[+C] extends DataSourceError[C]

  final case class DataSourceUnsupported(kind: DataSourceType, supported: ISet[DataSourceType])
      extends CreateError[Nothing]

  sealed trait InitializationError[C] extends CreateError[C]

  final case class MalformedConfiguration[C](kind: DataSourceType, config: C, reason: String)
      extends InitializationError[C]

  final case class InvalidConfiguration[C](kind: DataSourceType, config: C, reasons: NonEmptyList[String])
      extends InitializationError[C]

  final case class UnprocessableEntity[C](kind: DataSourceType, config: C, reason: String)
    extends InitializationError[C]

  final case class ConnectionFailed[C](kind: DataSourceType, config: C, cause: Exception)
    extends InitializationError[C]

  sealed trait ExistentialError extends CreateError[Nothing]

  final case class DataSourceExists(name: ResourceName)
      extends ExistentialError

  sealed trait CommonError extends ExistentialError

  final case class DataSourceNotFound(name: ResourceName)
      extends CommonError
}

sealed abstract class DataSourceErrorInstances {
  import DataSourceError._

  implicit def equal[C: Equal]: Equal[DataSourceError[C]] =
    Equal.equalBy {
      case DataSourceExists(n) =>
        (some(n), none, none, none, none, none, none)

      case DataSourceNotFound(n) =>
        (none, some(n), none, none, none, none, none)

      case DataSourceUnsupported(k, s) =>
        (none, none, some((k, s)), none, none, none, none)

      case MalformedConfiguration(k, c, r) =>
        (none, none, none, some((k, c, r)), none, none, none)

      case UnprocessableEntity(k, c, r) =>
        (none, none, none, none, some((k, c, r)), none, none)

      case ConnectionFailed(k, c, _) =>
        (none, none, none, none, none, some((k, c)), none)

      case InvalidConfiguration(k, c, rs) =>
        (none, none, none, none, none, none, some((k, c, rs)))
    }

  implicit def showCommonError: Show[CommonError] =
    Show.show {
      case DataSourceNotFound(n) =>
        Cord("DataSourceNotFound(") ++ n.show ++ Cord(")")
    }

  implicit def showCreateError[C: Show]: Show[CreateError[C]] =
    Show.show {
      case DataSourceExists(n) =>
        Cord("DataSourceExists(") ++ n.show ++ Cord(")")

      case e: CommonError => showCommonError.shows(e)

      case DataSourceUnsupported(k, s) =>
        Cord("DataSourceUnsupported(") ++ k.show ++ Cord(", ") ++ s.show ++ Cord(")")

      case MalformedConfiguration(k, c, r) =>
        Cord("MalformedConfiguration(") ++ k.show ++ Cord(", ") ++ c.show ++ Cord(", ") ++ r.show ++ Cord(")")

      case UnprocessableEntity(k, c, r) =>
        Cord("UnprocessableEntity(") ++ k.show ++ Cord(", ") ++ c.show ++ Cord(", ") ++ r.show ++ Cord(")")

      case ConnectionFailed(k, c, e) =>
        Cord("ConnectionFailed(") ++ k.show ++ Cord(", ") ++ c.show ++ Cord(s")\n\n$e")

      case InvalidConfiguration(k, c, rs) =>
        Cord("InvalidConfiguration(") ++ k.show ++ Cord(", ") ++ c.show ++ Cord(", ") ++ rs.show ++ Cord(")")
    }

  implicit def showDataSourceError[C: Show]: Show[DataSourceError[C]] =
    Show.show {
      case e: CreateError[C] => showCreateError[C].shows(e)
    }
}
