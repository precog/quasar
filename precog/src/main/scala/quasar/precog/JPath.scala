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

final case class JPath(nodes: List[JPathNode]) extends ToString {
  def to_s: String = nodes match {
    case Nil => "."
    case _   => nodes mkString ""
  }
}

sealed abstract class JPathNode(val to_s: String) extends ToString
final case class JPathField(name: String) extends JPathNode("." + name)
final case class JPathIndex(index: Int) extends JPathNode(s"[$index]")
