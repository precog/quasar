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

package quasar.yggdrasil
package jdbm3

object JDBMProjection {
  final val MAX_SPINS = 20 // FIXME: This is related to the JDBM ConcurrentMod exception, and should be removed when that's cleaned up
}

// FIXME: Again, related to JDBM concurent mod exception
class VicciniException(message: String) extends java.io.IOException("Inconceivable! " + message)
