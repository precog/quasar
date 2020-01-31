/*
 * Copyright 2014–2020 SlamData Inc.
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

import slamdata.Predef.StringContext

import quasar.qscript.OnUndefined

import cats.{Eq, Order, Show}
import cats.instances.tuple._
import cats.syntax.show._

import monocle.macros.Lenses

import shims.{equalToCats, showToCats}

/** Description of an MRA autojoin.
  *
  * @tparam S the type of scalar identities
  * @tparam V the type of vector identities
  *
  * @param keys the identity comparisons that form the join condition
  * @param onUndefined how to handle a row when the identities are undefined
  */
@Lenses
final case class AutoJoin[S, V](keys: JoinKeys[S, V], onUndefined: OnUndefined)

object AutoJoin {
  implicit def equal[S: Order, V: Order]: Eq[AutoJoin[S, V]] =
    Eq.by(aj => (aj.keys, aj.onUndefined))

  implicit def show[S: Show, V: Show]: Show[AutoJoin[S, V]] =
    Show show {
      case AutoJoin(keys, onundef) => s"AutoJoin(${keys.show}, ${onundef.show})"
    }
}
