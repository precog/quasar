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

package quasar.qsu
package minimizers

import slamdata.Predef._

import quasar.IdStatus

import scalaz.{Equal, Show}

sealed trait Output extends Product with Serializable {
  def toIdStatus: IdStatus =
    this match {
      case Output.Id => IdStatus.IdOnly
      case Output.Value => IdStatus.ExcludeId
      case Output.IdAndValue(_) => IdStatus.IncludeId
    }
}

object Output {
  case object Id extends Output
  case object Value extends Output
  final case class IdAndValue(idName: Symbol) extends Output

  val id: Output = Id
  val value: Output = Value
  def idAndValue(name: Symbol): Output = IdAndValue(name)

  implicit val outputEqual: Equal[Output] =
    Equal.equalA

  implicit val outputShow: Show[Output] =
    Show.showFromToString
}
