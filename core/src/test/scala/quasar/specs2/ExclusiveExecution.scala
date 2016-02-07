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

package quasar
package specs2

import org.specs2.mutable.Specification
import org.specs2.specification.{SpecificationStructure, Fragments}

/** Trait that tags all examples in a spec for exclusive execution. Examples
  * will be executed sequentially and parallel execution will be disabled.
  *
  * Use this when you have tests that muck with global state.
  */
trait ExclusiveExecution extends SpecificationStructure { self: Specification =>
  import ExclusiveExecution._

  sequential

  override def map(fs: => Fragments) =
    section(ExclusiveExecutionTag) ^ super.map(fs) ^ section(ExclusiveExecutionTag)
}

object ExclusiveExecution {
  /** The tag that indicates an example should be executed exclusively.
    *
    * NB: Take care when modifying this to update the SBT configuration.
    * TODO: Read this tag name from SBT config
    */
  val ExclusiveExecutionTag = "exclusive"
}
