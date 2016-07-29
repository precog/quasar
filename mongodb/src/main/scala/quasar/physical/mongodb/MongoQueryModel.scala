/*
 * Copyright 2014–2016 SlamData Inc.
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

import scalaz.Semigroup

sealed trait MongoQueryModel

object MongoQueryModel {
  /** The oldest supported version. */
  case object `2.6` extends MongoQueryModel

  /** Adds a few operators. */
  case object `3.0` extends MongoQueryModel

  /** Adds \$lookup and \$distinct, several new operators, and makes
    * accumulation operators available in \$project. */
  case object `3.2` extends MongoQueryModel

  implicit val semigroup: Semigroup[MongoQueryModel] =
    new Semigroup[MongoQueryModel] {
      def append(f1: MongoQueryModel, f2: => MongoQueryModel) = (f1, f2) match {
        case (`3.2`, _) => `3.2`
        case (_, `3.2`) => `3.2`
        case (`3.0`, _) => `3.0`
        case (_, `3.0`) => `3.0`
        case (`2.6`, _) => `2.6`
      }
    }
}
