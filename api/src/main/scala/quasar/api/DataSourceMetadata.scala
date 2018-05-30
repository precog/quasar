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

import slamdata.Predef.{Exception, Option}
import quasar.Condition

import monocle.macros.Lenses
import scalaz.{Cord, Show}
import scalaz.syntax.show._

@Lenses
final case class DataSourceMetadata(
    kind: DataSourceType,
    status: Condition[Exception])

object DataSourceMetadata extends DataSourceMetadataInstances {
  def fromOption(kind: DataSourceType, optErr: Option[Exception]): DataSourceMetadata =
    DataSourceMetadata(kind, Condition.optionIso.reverseGet(optErr))
}

sealed abstract class DataSourceMetadataInstances {
  implicit val show: Show[DataSourceMetadata] = {
    implicit val exShow: Show[Exception] =
      Show.shows(_.getMessage)

    Show.show {
      case DataSourceMetadata(k, s) =>
        Cord("DataSourceMetadata(") ++ k.show ++ Cord(", ") ++ s.show ++ Cord(")")
    }
  }
}
