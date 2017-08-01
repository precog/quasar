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

package quasar.config

import slamdata.Predef._
import quasar.db.DbConnectionConfig
import quasar.db.DbConnectionConfigArbitrary._

import eu.timepit.refined.scalacheck.numeric._
import org.scalacheck.Arbitrary
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz._, Scalaz._

trait WebConfigArbitrary {
  implicit val webConfigArbitrary: Arbitrary[WebConfig] =
    Arbitrary(
      (Arbitrary.arbitrary[Int] ⊛ Arbitrary.arbitrary[DbConnectionConfig])((p, c) =>
        WebConfig(ServerConfig(p), MetaStoreConfig(c).some)))
}

object WebConfigArbitrary extends WebConfigArbitrary
