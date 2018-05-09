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

import slamdata.Predef.{Nothing, Option, Product, Serializable, String, Throwable}

import scalaz.{Cord, ISet, NonEmptyList, Show}
import scalaz.std.string._
import scalaz.std.option._
import scalaz.syntax.show._

sealed trait DataSourceError[+C] extends QuasarErrorNG
    with Product
    with Serializable

object DataSourceError extends DataSourceErrorInstances {
  sealed trait ExternalError[C] extends DataSourceError[C]

  final case class DataSourceUnsupported[C](kind: DataSourceType, supported: ISet[DataSourceType])
      extends ExternalError[C]

  sealed trait InitializationError[C] extends ExternalError[C]

  final case class MalformedConfiguration[C](kind: DataSourceType, config: C, reason: String)
      extends InitializationError[C]

  final case class InvalidConfiguration[C](kind: DataSourceType, config: C, reasons: NonEmptyList[String])
      extends InitializationError[C]

  final case class ConnectionFailed[C](message: String, cause: Option[Throwable])
      extends InitializationError[C]

  sealed trait StaticError extends DataSourceError[Nothing]

  final case class MalformedContent(reason: String)
      extends StaticError

  sealed trait CreateError[C] extends ExternalError[C] with StaticError

  final case class DataSourceExists[C](name: ResourceName)
      extends CreateError[C]

  sealed trait CommonError[C] extends CreateError[C]

  final case class DataSourceNotFound[C](name: ResourceName)
      extends CommonError[C]
}

sealed abstract class DataSourceErrorInstances {
  import DataSourceError._

  implicit def show[C: Show]: Show[DataSourceError[C]] =
    Show.show {
      case DataSourceExists(n) =>
        Cord("DataSourceExists(") ++ n.show ++ Cord(")")

      case DataSourceNotFound(n) =>
        Cord("DataSourceNotFound(") ++ n.show ++ Cord(")")

      case DataSourceUnsupported(k, s) =>
        Cord("DataSourceUnsupported(") ++ k.show ++ Cord(", ") ++ s.show ++ Cord(")")

      case MalformedConfiguration(k, c, r) =>
        Cord("MalformedConfiguration(") ++ k.show ++ Cord(", ") ++ c.show ++ Cord(", ") ++ r.show ++ Cord(")")

      case InvalidConfiguration(k, c, rs) =>
        Cord("InvalidConfiguration(") ++ k.show ++ Cord(", ") ++ c.show ++ Cord(", ") ++ rs.show ++ Cord(")")

      case MalformedContent(r) =>
        Cord("MalformedContent(") ++ r.show ++ Cord(")")

      case ConnectionFailed(m, t) =>
        Cord("ConnectionFailed(") ++ m.show ++ Cord(", ") ++ t.map(_.getMessage).show ++ Cord(")")
    }
}
