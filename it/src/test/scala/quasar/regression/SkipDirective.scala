/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.regression

import quasar.Predef._

import argonaut._, Argonaut._

sealed trait SkipDirective

object SkipDirective {
  final case object Skip    extends SkipDirective
  final case object Pending extends SkipDirective

  import DecodeResult.{ok, fail}

  implicit val SkipDirectiveDecodeJson: DecodeJson[SkipDirective] =
    DecodeJson(c => c.as[String].flatMap {
      case "skip"     => ok(Skip)
      case "pending"  => ok(Pending)
      case str        => fail("skip, pending; found: \"" + str + "\"", c.history)
    })
}
