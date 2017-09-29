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

package quasar.physical.mongodb

import slamdata.Predef._

import scalaz._, Scalaz._, Ordering._

sealed abstract class MongoQueryModel(val s: String)

object MongoQueryModel {

  /** The oldest supported version. */
  case object `2.6` extends MongoQueryModel("2.6")

  /** Adds a few operators. */
  case object `3.0` extends MongoQueryModel("3.0")

  /** Adds \$lookup and \$distinct, several new operators, and makes
   * accumulation operators available in \$project. */
  case object `3.2` extends MongoQueryModel("3.2")
  case object `3.4` extends MongoQueryModel("3.4")

  def apply(version: ServerVersion): MongoQueryModel =
    if (version >= ServerVersion.MongoDb3_4) MongoQueryModel.`3.4`
    else if (version >= ServerVersion.MongoDb3_2) MongoQueryModel.`3.2`
    else if (version >= ServerVersion.MongoDb3_0) MongoQueryModel.`3.0`
    else MongoQueryModel.`2.6`

  def toBsonVersion(v: MongoQueryModel): BsonVersion =
    if (v lt `3.4`) BsonVersion.`1.0`
    else BsonVersion.`1.1`

  implicit val showMongoQueryModel: Show[MongoQueryModel] = new Show[MongoQueryModel] {
    override def shows(v: MongoQueryModel) = v.s
  }

  implicit val orderMongoQueryModel: Order[MongoQueryModel] = new Order[MongoQueryModel] {
    def order(x: MongoQueryModel, y: MongoQueryModel): Ordering = (x, y) match {
      case (`2.6`, `2.6`) => EQ
      case (`3.0`, `3.0`) => EQ
      case (`3.2`, `3.2`) => EQ
      case (`3.4`, `3.4`) => EQ
      case (`2.6`, _) => LT
      case (`3.0`, `3.2` | `3.4`) => LT
      case (`3.2`, `3.4`) => LT
      case _ => GT
    }
  }
}
