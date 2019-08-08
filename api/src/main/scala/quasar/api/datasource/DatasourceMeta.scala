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

package quasar.api.datasource

import slamdata.Predef._
import quasar.Condition

import monocle.macros.Lenses
import scalaz.Show
import scalaz.syntax.show._

@Lenses
final case class DatasourceMeta(
    kind: DatasourceType,
    name: DatasourceName,
    status: Condition[Exception])

object DatasourceMeta extends DatasourceMetaInstances {
  def fromOption(
      kind: DatasourceType,
      name: DatasourceName,
      optErr: Option[Exception])
      : DatasourceMeta =
    DatasourceMeta(kind, name, Condition.optionIso.reverseGet(optErr))
}

sealed abstract class DatasourceMetaInstances {
  implicit val show: Show[DatasourceMeta] = {
    implicit val exShow: Show[Exception] =
      Show.shows(_.getMessage)

    Show.shows {
      case DatasourceMeta(n, k, s) =>
        "DatasourceMeta(" ++ k.shows ++ ", " ++ n.shows ++ ", " ++ s.shows ++ ")"
    }
  }
}
