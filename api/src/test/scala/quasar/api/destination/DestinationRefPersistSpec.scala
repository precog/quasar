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

package quasar.api.destination

import slamdata.Predef._

import argonaut.Json
import argonaut.JsonScalaz._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import eu.timepit.refined.scalacheck.numeric.chooseRefinedNum
import monocle.law.discipline.PrismTests
import org.scalacheck._
import org.typelevel.discipline.specs2.mutable.Discipline
import org.specs2.mutable.SpecificationLike

final class DestinationRefPersistSpec extends SpecificationLike with Discipline {
  private implicit def destinationTypeArbitrary: Arbitrary[DestinationType] =
    Arbitrary(for {
      ident0 <- Gen.identifier
      ident = Refined.unsafeApply[String, DestinationType.NameP](ident0)
      version <- chooseRefinedNum[Refined, Long, Positive](1L, 100L)
    } yield DestinationType(ident, version))

  private implicit def destinationNameArbitary: Arbitrary[DestinationName] =
    Arbitrary(Gen.identifier.map(DestinationName(_)))

  private implicit val arbitraryJsonConfig: Arbitrary[Json] =
    Arbitrary(
      Gen.oneOf(
        List(
          Json.obj("foo" -> Json.jString("bar")),
          Json.jString("quux"),
          Json.jNumber(42L),
          Json.array(Json.jString("one"), Json.jString("two")))))

  private implicit def destinationRefArbitrary: Arbitrary[DestinationRef[Json]] =
    Arbitrary(for {
      typ <- Arbitrary.arbitrary[DestinationType]
      name <- Arbitrary.arbitrary[DestinationName]
      config <- Arbitrary.arbitrary[Json]
    } yield DestinationRef(typ, name, config))

  private implicit def destinationRefModifyArbitrary: Arbitrary[DestinationRef[Json] => DestinationRef[Json]] =
    Arbitrary(for {
      typ <- Arbitrary.arbitrary[DestinationType]
      name <- Arbitrary.arbitrary[DestinationName]
      config <- Arbitrary.arbitrary[Json]
      modify <- Gen.oneOf(List[DestinationRef[Json] => DestinationRef[Json]](
        DestinationRef.kind.set(typ), DestinationRef.name.set(name), DestinationRef.config.set(config)))
    } yield modify)


  checkAll("Prism[DestinationRef[Json]]", PrismTests(DestinationRef.persistJsonP[Json]))
}
