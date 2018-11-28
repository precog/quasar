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

package quasar.impl.schema

import slamdata.Predef.Boolean
import quasar.api.SchemaConfig
import quasar.fp.numeric.Natural

import eu.timepit.refined.auto._

/** Configuration for SST-based schema, allowing for control over various aspects
  * of compression.
  *
  * @param arrayMaxLength arrays larger than this will be compressed
  * @param mapMaxSize maps larger than this will be compressed
  * @param retainIndicesSize the number array indices to retain during compression
  * @param retainKeysSize the number of map keys to retain, per type, during compression
  * @param stringMaxLength all strings longer than this are compressed
  * @param stringPreserveStructure whether to preserve structure when compressing strings
  */
final case class SstConfig[J, A](
    arrayMaxLength: Natural,
    mapMaxSize: Natural,
    retainIndicesSize: Natural,
    retainKeysSize: Natural,
    stringMaxLength: Natural,
    stringPreserveStructure: Boolean)
    extends SchemaConfig {

  type Schema = SstSchema[J, A]
}

object SstConfig {
  val DefaultArrayMaxLength: Natural = 10L
  val DefaultMapMaxSize: Natural = 32L
  val DefaultRetainIndicesSize: Natural = 0L
  val DefaultRetainKeysSize: Natural = 0L
  val DefaultStringMaxLength: Natural = 64L
  val DefaultStringPreserveStructure: Boolean = false

  def Default[J, A]: SstConfig[J, A] =
    SstConfig[J, A](
      arrayMaxLength = DefaultArrayMaxLength,
      mapMaxSize = DefaultMapMaxSize,
      retainIndicesSize = DefaultRetainIndicesSize,
      retainKeysSize = DefaultRetainKeysSize,
      stringMaxLength = DefaultStringMaxLength,
      stringPreserveStructure = DefaultStringPreserveStructure)
}
