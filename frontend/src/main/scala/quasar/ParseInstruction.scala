/*
 * Copyright 2014–2018 SlamData Inc.
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

import slamdata.Predef._

import quasar.common.CPath

import scalaz.{Cord, Equal, Show}
import scalaz.std.map._
import scalaz.std.set._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.equal._
import scalaz.syntax.show._

sealed abstract class ParseInstruction extends Product with Serializable

object ParseInstruction {

  /**
   * Generates one unique identity per row. Creates a top-level array with
   * the identities in the first component, the original values in the second.
   * Just like `Pivot` with `IdStatus.IncludeId`.
   */
  case object Ids extends ParseInstruction

  /**
   * Wraps the provided `path` into an object with key `name`, thus adding
   * another layer of structure. All other paths are retained.
   */
  final case class Wrap(path: CPath, name: String) extends ParseInstruction

  /**
   *`Masks` represents the disjunction of the provided `masks`. An empty map indicates
   * that all values should be dropped. Removes all values which are not in one of the
   * path/type designations. The inner set is assumed to be non-empty.
   */
  final case class Mask(masks: Map[CPath, Set[ParseType]]) extends ParseInstruction

  /**
   * Pivots the indices and keys out of arrays and objects, respectively,
   * according to the provided structure, performing a full cross of all pivoted
   * values.
   *
   * `idStatus` determines how the values is returned:
   *   - `IncludeId` wraps the key/value pair in a two element array.
   *   - `IdOnly` returns the value unwrapped.
   *   - `ExcludeId` returns the value unwrapped.
   *
   * No values outside of the pivot loci should be retained.
   */
  final case class Pivot(pivots: Map[CPath, (IdStatus, CompositeParseType)])
      extends ParseInstruction

  ////

  implicit val parseInstructionEqual: Equal[ParseInstruction] =
    Equal.equal {
      case (Ids, Ids) => true
      case (Wrap(p1, n1), Wrap(p2, n2)) => p1 === p2 && n1 === n2
      case (Mask(m1), Mask(m2)) => m1 === m2
      case (Pivot(p1), Pivot(p2)) => p1 === p2
      case (_, _) => false
    }

  implicit val parseInstructionShow: Show[ParseInstruction] =
    Show.show {
      case Ids => Cord("Ids")
      case Wrap(p, n) => Cord("Wrap(") ++ p.show ++ Cord(", ") ++ n.show ++ Cord(")")
      case Mask(m) => Cord("Mask(") ++ m.show ++ Cord(")")
      case Pivot(p) => Cord("Pivot(") ++ p.show ++ Cord(")")
    }
}
