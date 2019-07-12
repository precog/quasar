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

package quasar.run.store

import slamdata.Predef._

import quasar.api.destination.{DestinationName, DestinationRef, DestinationType}

import argonaut.{CodecJson, Json}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import eu.timepit.refined.scalacheck.numeric.chooseRefinedNum
import org.scalacheck._
import org.specs2.mutable.SpecificationLike
import org.typelevel.discipline.specs2.mutable.Discipline

final class StoreCodecSpec extends SpecificationLike with Discipline {
  val destinationTypeGen: Gen[DestinationType] =
    for {
      ident0 <- Gen.identifier
      ident = Refined.unsafeApply[String, DestinationType.NameP](ident0)
      version <- chooseRefinedNum[Refined, Long, Positive](1L, 100L)
    } yield DestinationType(ident, version)

  val destinationNameGen: Gen[DestinationName] =
    Gen.identifier.map(DestinationName(_))

  val jsonGen: Gen[Json] =
    Gen.oneOf(
      List(
        Json.obj("foo" -> Json.jString("bar")),
        Json.jString("quux"),
        Json.jNumber(42L),
        Json.array(Json.jString("one"), Json.jString("two"))))

  implicit def destinationRefArbitrary: Arbitrary[DestinationRef[Json]] =
    Arbitrary(for {
      typ <- destinationTypeGen
      name <- destinationNameGen
      config <- jsonGen
    } yield DestinationRef(typ, name, config))

  "destinationRefCodec is lawful" >> prop { (destRef: DestinationRef[Json]) =>
    CodecJson.codecLaw(StoreCodec.destinationRefCodec[Json])(destRef) must beTrue
  }
}
