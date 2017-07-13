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

/** A subset of the information available from the collStats command. Many more
  * fields are available, some only in particular MongoDB versions or with
  * particular storage engines.
  * @param count The number of documents in the collection.
  * @param dataSize The total size "in memory" in bytes of all documents in the
  *   collection, not including headers or indexes.
  */
final case class CollectionStatistics(
  count: Long,
  dataSize: Long,
  sharded: Boolean)
