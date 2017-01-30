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

package quasar.physical.marklogic.xcc

import quasar.effect.Capture

import com.marklogic.xcc.ResultSequence
import com.marklogic.xcc.types.XdmItem
import scalaz.ImmutableArray

final class QueryResults private[xcc] (private[xcc] val resultSequence: ResultSequence) {
  /** Collect all of the result items in an `ImmutableArray`.
    *
    * NB: This method should only be called once as it consumes the underlying
    *     `ResultSequence`.
    */
  def toImmutableArray[F[_]](implicit F: Capture[F]): F[ImmutableArray[XdmItem]] =
    F.delay {
      val items = resultSequence.toCached.toArray
      resultSequence.close()
      ImmutableArray.fromArray(items)
    }
}
