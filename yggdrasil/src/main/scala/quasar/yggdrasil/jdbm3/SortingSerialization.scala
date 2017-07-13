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

import quasar.blueeyes._
import java.util.Comparator

final case class SortingKeyComparator(rowFormat: RowFormat, ascending: Boolean) extends Comparator[Array[Byte]] {
  def compare(a: Array[Byte], b: Array[Byte]): Int =
    rowFormat.compare(a, b) |> (n => if (ascending) n else -n)
}
