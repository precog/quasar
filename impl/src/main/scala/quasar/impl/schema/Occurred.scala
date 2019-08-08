/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.impl.schema

import slamdata.Predef._
import quasar.ejson.{EJson, EncodeEJson, Fixed}
import quasar.ejson.implicits._

import matryoshka.{Corecursive, Recursive}
import monocle.macros.Lenses
import scalaz.{Equal, IMap, Show}
import scalaz.std.tuple._
import scalaz.syntax.show._

@Lenses
final case class Occurred[N, A](occurrence: N, value: A)

object Occurred {
  implicit def encodeEJson[N: EncodeEJson, A: EncodeEJson]: EncodeEJson[Occurred[N, A]] =
    new EncodeEJson[Occurred[N, A]] {
      def encode[J](o: Occurred[N, A])(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): J = {
        val J = Fixed[J]
        val v = o.value.asEJson[J]

        J.imap
          .modifyOption(_.insert(J.str(OccurrenceKey), o.occurrence.asEJson[J]))
          .apply(v)
          .getOrElse(J.imap(IMap(
            J.str(OccurrenceKey) -> o.occurrence.asEJson[J],
            J.str(ValueKey) -> v)))
      }
    }

  implicit def equal[N: Equal, A: Equal]: Equal[Occurred[N, A]] =
    Equal.equalBy {
      case Occurred(n, a) => (n, a)
    }

  implicit def show[N: Show, A: Show]: Show[Occurred[N, A]] =
    Show.shows {
      case Occurred(n, a) =>
        "Occurred(" ++ n.shows ++ ", " ++ a.shows ++ ")"
    }

  ////

  private val OccurrenceKey = "occurrence"
  private val ValueKey = "value"
}
