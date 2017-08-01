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

package quasar.repl

import slamdata.Predef._

import monocle.Prism
import scalaz._, Scalaz._

sealed abstract class DebugLevel

object DebugLevel {
  final case object Silent extends DebugLevel
  final case object Normal extends DebugLevel
  final case object Verbose extends DebugLevel

  val int: Prism[Int, DebugLevel] =
    Prism.partial[Int, DebugLevel] {
      case 0 => Silent
      case 1 => Normal
      case 2 => Verbose
    } {
      case Silent  => 0
      case Normal  => 1
      case Verbose => 2
    }

  implicit val equal: Equal[DebugLevel] =
    Equal.equalBy(int(_))
}
