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

package quasar.precog

import quasar.blueeyes._
import java.util.Comparator
import scala.collection.JavaConverters._
import java.util.concurrent.atomic.AtomicInteger
import scalaz._, Scalaz._

package object util {
  /**
    * Opaque symbolic identifier (like Int, but better!).
    */
  final class Identifier extends AnyRef

  // Shared Int could easily overflow: Unshare? Extend to a Long? Different approach?
  object IdGen extends IdGen
  class IdGen {
    private[this] val currentId = new AtomicInteger(0)
    def nextInt(): Int = currentId.getAndIncrement()
  }

}
