/*
 * Copyright 2014–2019 SlamData Inc.
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

import quasar.fp.numeric.Positive

import eu.timepit.refined.auto._
import monocle.macros.Lenses

/** Configuration parameters related to the computation of SSTs.
  *
  * @param sampleSize the number of records to sample when generating SST schemas
  * @param parallelism the number of chunks to process in parallel when generating SST schemas
  * @param chunkSize the size of each chunk when generating SST schemas
  */
@Lenses
final case class SstEvalConfig(
    sampleSize: Positive,
    parallelism: Positive,
    chunkSize: Positive)

object SstEvalConfig {
  val single: SstEvalConfig = SstEvalConfig(1L, 1L, 1L)
}
