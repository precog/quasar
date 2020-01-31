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

import scalaz.{Equal, Show}
import scalaz.std.anyVal._
import scalaz.std.either._
import scalaz.std.string._

sealed trait Index extends Product with Serializable {
  def toEither: Either[Int, String] =
    this match {
      case Index.Field(n) => Right(n)
      case Index.Position(i) => Left(i)
    }
}

object Index {
  final case class Field(name: String) extends Index
  final case class Position(ord: Int) extends Index

  def field(n: String): Index = Field(n)
  def position(i: Int): Index = Position(i)

  implicit val indexEqual: Equal[Index] =
    Equal.equalBy(_.toEither)

  implicit val indexShow: Show[Index] =
    Show shows {
      case Field(n) => s".$n"
      case Position(i) => s"[$i]"
    }
}
