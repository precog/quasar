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

package quasar.db

import slamdata.Predef._

import scala.Predef.$conforms

import org.scalacheck.{Arbitrary, Gen}, Arbitrary.arbitrary

trait DbConnectionConfigArbitrary {
  import DbConnectionConfig._

  implicit val dbConnectionConfigArbitrary: Arbitrary[DbConnectionConfig] =
    Arbitrary(
      Gen.oneOf(
        arbitrary[String].map(H2),
        for {
          host       <- arbitrary[Option[String]]
          port       <- Gen.option(Gen.choose(0, 65535))
          database   <- arbitrary[Option[String]]
          userName   <- arbitrary[String]
          password   <- arbitrary[String]
          parameters <- arbitrary[Map[String, String]]
        } yield PostgreSql(
          host.map(name => HostInfo(name, port)),
          database, userName, password, parameters)))
}

object DbConnectionConfigArbitrary extends DbConnectionConfigArbitrary
