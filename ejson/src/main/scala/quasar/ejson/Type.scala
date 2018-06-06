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

package quasar.ejson

import slamdata.Predef._
import quasar.contrib.iota.copkTraverse

import matryoshka._
import scalaz.std.option._

object Type {
  import EJson._

  val TypeKey = "_ejson.type"

  def apply[T](tag: TypeTag)(implicit T: Corecursive.Aux[T, EJson]): T =
    fromExt(Map(List(fromCommon[T](Str(TypeKey)) -> fromCommon[T](Str(tag.value)))))

  /** Extracts the type tag from a metadata map, if present. */
  def unapply[T](ejs: EJson[T])(implicit T: Recursive.Aux[T, EJson]): Option[TypeTag] =
    ejs match {
      case ExtEJson(Map(xs)) =>
        xs collectFirst {
          case (Embed(CommonEJson(Str(TypeKey))), Embed(CommonEJson(Str(t)))) => TypeTag(t)
        }
      case _ => none
    }
}
