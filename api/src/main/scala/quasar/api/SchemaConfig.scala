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

package quasar.api

/** Configuration required in order to obtain a information about the structure
  * of a dataset.
  */
trait SchemaConfig {
  /** Representation describing of the structure of a dataset. */
  type Schema
}

object SchemaConfig {
  type Aux[Schema0] = SchemaConfig { type Schema = Schema0 }
}
