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

import quasar.Predef.Boolean
import quasar.fs.FileSystemType

import monocle.macros.Lenses
import scalaz._
import scalaz.std.tuple._

@Lenses
final case class ExternalBackendRef(ref: BackendRef, fsType: FileSystemType) {
  def supports(bc: BackendCapability): Boolean = ref.supports(bc)
  def name = ref.name
}

object ExternalBackendRef {
  implicit val order: Order[ExternalBackendRef] =
    Order.orderBy(r => (r.ref, r.fsType))

  implicit val show: Show[ExternalBackendRef] =
    Show.showFromToString
}
