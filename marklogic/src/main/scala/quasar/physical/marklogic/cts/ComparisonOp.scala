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

import scalaz.{Enum, Show}
import scalaz.syntax.order._
import scalaz.std.anyVal._

sealed abstract class ComparisonOp

object ComparisonOp {
  final object LT extends ComparisonOp
  final object LE extends ComparisonOp
  final object GT extends ComparisonOp
  final object GE extends ComparisonOp
  final object EQ extends ComparisonOp
  final object NE extends ComparisonOp

  def toXQuery(op: ComparisonOp): XQuery =
    XQuery.StringLit(op match {
      case LT => "<"
      case LE => "<="
      case GT => ">"
      case GE => ">="
      case EQ => "="
      case NE => "!="
    })

  implicit val enum: Enum[ComparisonOp] =
    new Enum[ComparisonOp] {
      def succ(op: ComparisonOp) = op match {
        case LT => LE
        case LE => GT
        case GT => GE
        case GE => EQ
        case EQ => NE
        case NE => LT
      }

      def pred(op: ComparisonOp) = op match {
        case LT => NE
        case LE => LT
        case GT => LE
        case GE => GT
        case EQ => GE
        case NE => EQ
      }

      def order(a: ComparisonOp, b: ComparisonOp) =
        asInt(a) ?|? asInt(b)

      private def asInt(op: ComparisonOp): Int = op match {
        case LT => 1
        case LE => 2
        case GT => 3
        case GE => 4
        case EQ => 5
        case NE => 6
      }
    }

  implicit val show: Show[ComparisonOp] =
    Show.showFromToString

  implicit val renderTree: RenderTree[ComparisonOp] =
    RenderTree.fromShow("ComparisonOp")
}
