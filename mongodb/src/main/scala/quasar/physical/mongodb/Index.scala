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

import scalaz.NonEmptyList

final case class Index(name: String,
                       key: NonEmptyList[(BsonField, IndexType)],
                       unique: Boolean) {
  def primary: BsonField = key.head._1
}

sealed abstract class IndexType(bson: Bson)
object IndexType {
  case object Ascending  extends IndexType(Bson.Int32(1))
  case object Descending extends IndexType(Bson.Int32(-1))
  case object Hashed     extends IndexType(Bson.Text("hashed"))
  // NB: MongoDB provides several additional index types but they're not
  // currently of interest to us.
}
