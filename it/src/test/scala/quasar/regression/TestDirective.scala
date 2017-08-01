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

package quasar.regression

import slamdata.Predef._

import argonaut._, Argonaut._

sealed abstract class TestDirective

object TestDirective {
  final case object Skip extends TestDirective
  final case object SkipCI extends TestDirective
  final case object Timeout extends TestDirective
  final case object Pending extends TestDirective
  final case object PendingIgnoreFieldOrder extends TestDirective
  final case object IgnoreAllOrder extends TestDirective
  final case object IgnoreFieldOrder extends TestDirective
  final case object IgnoreResultOrder extends TestDirective

  import DecodeResult.{ok, fail}

  implicit val TestDirectiveDecodeJson: DecodeJson[TestDirective] =
    DecodeJson(c => c.as[String].flatMap {
      case "skip" => ok(Skip)
      case "skipCI" => ok(SkipCI)
      case "timeout" => ok(Timeout)
      case "pending" => ok(Pending)
      case "pendingIgnoreFieldOrder" => ok(PendingIgnoreFieldOrder)
      case "ignoreAllOrder" => ok(IgnoreAllOrder)
      case "ignoreFieldOrder" => ok(IgnoreFieldOrder)
      case "ignoreResultOrder" => ok(IgnoreResultOrder)
      case str => fail("\"" + str + "\" is not a valid backend directive.", c.history)
    })
}
