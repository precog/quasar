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

import slamdata.Predef.Exception
import quasar.Condition

import monocle.macros.Lenses
import scalaz.{Cord, Show}
import scalaz.syntax.show._

@Lenses
final case class ExternalMetadata(
    kind: DataSourceType,
    condition: Condition[Exception])

object ExternalMetadata extends ExternalMetadataInstances

sealed abstract class ExternalMetadataInstances {
  implicit val show: Show[ExternalMetadata] = {
    implicit val exShow: Show[Exception] =
      Show.shows(_.getMessage)

    Show.show {
      case ExternalMetadata(k, s) =>
        Cord("ExternalMetadata(") ++ k.show ++ Cord(", ") ++ s.show ++ Cord(")")
    }
  }
}
