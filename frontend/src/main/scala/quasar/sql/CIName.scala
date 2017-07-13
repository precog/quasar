/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.sql

import slamdata.Predef._

import argonaut._
import scalaz._, Scalaz._

final case class CIName(value: String) {
  override def equals(other: Any) = other match {
    case CIName(otherValue) => otherValue.toLowerCase === value.toLowerCase
    case _                  => false
  }

  override def hashCode: Int = value.toLowerCase.hashCode
}

object CIName {
  implicit val equal: Equal[CIName] = Equal.equalA
  implicit val shows: Show[CIName] = Show.shows(s => s.value)
  implicit val codec: EncodeJson[CIName] = EncodeJson.jencode1(_.value)
}
