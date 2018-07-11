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

package quasar

import slamdata.Predef.{Map, Option, String}

import scalaz.Equal
import scalaz.std.tuple._
import scalaz.syntax.bifunctor._

final case class Variables(value: Map[VarName, VarValue]) {
  def lookup(name: VarName): Option[VarValue] =
    value.get(name)

  def +(nv: (VarName, VarValue)): Variables =
    Variables(value + nv)

  def -(n: VarName): Variables =
    Variables(value - n)
}

final case class VarName(value: String) {
  override def toString = ":" + value
}

final case class VarValue(value: String)

object Variables {
  val empty: Variables = Variables(Map())

  def fromMap(value: Map[String, String]): Variables =
    Variables(value.map(_.bimap(VarName(_), VarValue(_))))

  implicit val equal: Equal[Variables] = Equal.equalA
}
