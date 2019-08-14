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

import slamdata.Predef.{Byte, List}

import quasar.api.table.TableColumn

import scalaz.{Equal, Show}
import fs2.Stream

sealed trait ResultType[F[_]] {
  type T
}

object ResultType {
  type Aux[F[_], T0] = ResultType[F] {
    type T = T0
  }

  final case class Csv[F[_]]() extends ResultType[F] {
    type T = (List[TableColumn], Stream[F, Byte])
  }

  implicit def equal[F[_]]: Equal[ResultType[F]] =
    Equal.equalA[ResultType[F]]

  implicit def show[F[_]]: Show[ResultType[F]] =
    Show.shows[ResultType[F]] {
      case ResultType.Csv() => "csv"
    }
}
