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

package quasar.physical.marklogic.cts

import slamdata.Predef._
import quasar.RenderTree
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._

import scalaz.{Enum, Show}
import scalaz.syntax.order._
import scalaz.std.anyVal._

sealed abstract class MatchDepth

object MatchDepth {
  final case object Children extends MatchDepth
  final case object Descendants extends MatchDepth

  val toXQuery: MatchDepth => XQuery = {
    case Children    => "1".xs
    case Descendants => "infinity".xs
  }

  implicit val enum: Enum[MatchDepth] =
    new Enum[MatchDepth] {
      def succ(md: MatchDepth) = md match {
        case Children    => Descendants
        case Descendants => Children
      }

      def pred(md: MatchDepth) = md match {
        case Children    => Descendants
        case Descendants => Children
      }

      def order(a: MatchDepth, b: MatchDepth) =
        asInt(a) ?|? asInt(b)

      private def asInt(md: MatchDepth): Int = md match {
        case Children    => 1
        case Descendants => 2
      }
    }

  implicit val show: Show[MatchDepth] =
    Show.showFromToString

  implicit val renderTree: RenderTree[MatchDepth] =
    RenderTree.fromShow("MatchDepth")
}
