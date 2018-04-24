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

import scalaz._, Scalaz._

sealed abstract class MongoQueryModel(val s: String)

object MongoQueryModel {
  case object `3.2` extends MongoQueryModel("3.2")
  case object `3.4` extends MongoQueryModel("3.4")
  case object `3.4.4` extends MongoQueryModel("3.4.4")
  case object `3.6` extends MongoQueryModel("3.6")

  def apply(version: ServerVersion): MongoQueryModel =
    if (version >= ServerVersion.MongoDb3_6)        MongoQueryModel.`3.6`
    else if (version >= ServerVersion.MongoDb3_4_4) MongoQueryModel.`3.4.4`
    else if (version >= ServerVersion.MongoDb3_4)   MongoQueryModel.`3.4`
    else                                            MongoQueryModel.`3.2`

  def toBsonVersion(v: MongoQueryModel): BsonVersion =
    if (v lt `3.4`) BsonVersion.`1.0`
    else BsonVersion.`1.1`

  implicit val showMongoQueryModel: Show[MongoQueryModel] = new Show[MongoQueryModel] {
    override def shows(v: MongoQueryModel) = v.s
  }

  private def toInt(m: MongoQueryModel): Int = m match {
    case `3.2` => 0
    case `3.4` => 1
    case `3.4.4` => 2
    case `3.6` => 3
  }

  implicit val orderMongoQueryModel: Order[MongoQueryModel] = new Order[MongoQueryModel] {
    def order(x: MongoQueryModel, y: MongoQueryModel): Ordering =
      Order[Int].order(toInt(x), toInt(y))
  }
}
