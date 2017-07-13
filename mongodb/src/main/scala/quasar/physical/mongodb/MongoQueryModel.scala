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

import scalaz.Scalaz._

sealed abstract class MongoQueryModel

object MongoQueryModel {
  /** The oldest supported version. */
  case object `2.6` extends MongoQueryModel

  /** Adds a few operators. */
  case object `3.0` extends MongoQueryModel

  /** Adds \$lookup and \$distinct, several new operators, and makes
    * accumulation operators available in \$project. */
  case object `3.2` extends MongoQueryModel

  def apply(version: ServerVersion): MongoQueryModel =
    if (version >= ServerVersion.MongoDb3_2)      MongoQueryModel.`3.2`
    else if (version >= ServerVersion.MongoDb3_0) MongoQueryModel.`3.0`
    else                                          MongoQueryModel.`2.6`
}
