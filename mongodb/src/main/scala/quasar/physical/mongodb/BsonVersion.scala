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

package quasar.physical.mongodb

import slamdata.Predef._

import scalaz._, Ordering._

sealed abstract class BsonVersion(val s: String)

object BsonVersion {
  case object `1.0` extends BsonVersion("1.0")
  case object `1.1` extends BsonVersion("1.1")

  implicit val showBsonVersion: Show[BsonVersion] = new Show[BsonVersion] {
    override def shows(v: BsonVersion) = v.s
  }

  implicit val equalBsonVersion: Equal[BsonVersion] = Equal.equalA

  implicit val orderBsonVersion: Order[BsonVersion] = new Order[BsonVersion] {
    def order(x: BsonVersion, y: BsonVersion): Ordering = (x, y) match {
      case (`1.0`, `1.0`) => EQ
      case (`1.1`, `1.1`) => EQ
      case (`1.0`, _) => LT
      case _ => GT
    }
  }
}
