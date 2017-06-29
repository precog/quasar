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

package quasar.contrib.contextual

import slamdata.Predef._

import contextual._
import scalaz._

trait StaticInterpolator[A] extends Interpolator {
  def parse(s: String): String \/ A

  def contextualize(interpolation: StaticInterpolation) = {
    interpolation.parts.foreach {
      case lit@Literal(_, string) =>
        parse(string) match {
          case -\/(msg) => interpolation.abort(lit, 0, msg)
          case _        => ()
        }

      case hole@Hole(_, _) =>
        interpolation.abort(hole, "substitutions are not supported")
    }

    Nil
  }

  // A String that would not parse should not have made it past compile time
  def evaluate(interpolation: RuntimeInterpolation): A =
    parse(interpolation.parts.mkString).valueOr(errMsg =>
      scala.sys.error(s"Something terribly wrong happened. Failed to parse something at runtime that we checked at compile time. Reason: $errMsg. This is either because the `parse` function of this `StaticInterpolator` is not pure or could (less likely) be a bug with `StaticInterpolator`"))
}