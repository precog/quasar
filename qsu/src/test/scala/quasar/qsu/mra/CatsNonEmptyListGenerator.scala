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

package quasar.qsu.mra

import slamdata.Predef.List

import quasar.pkg.tests._

import scala.Predef.$conforms

import cats.data.NonEmptyList

import org.scalacheck.Cogen

// Copied from cats-laws due to the scalacheck version incompatibility between
// cats-1.5.0 and quasar, using the instances from `cats` results in
// NoSuchMethodErrors at runtime.
//
// FIXME: Remove once cats upgrades to scalacheck-1.14.x
trait CatsNonEmptyListGenerator {
  implicit def catsArbitraryNel[A: Arbitrary]: Arbitrary[NonEmptyList[A]] =
    Arbitrary(for {
      h <- arbitrary[A]
      t <- arbitrary[List[A]]
    } yield NonEmptyList(h, t))

  implicit def catsCogenNel[A: Cogen]: Cogen[NonEmptyList[A]] =
    Cogen[List[A]].contramap(_.toList)
}

object CatsNonEmptyListGenerator extends CatsNonEmptyListGenerator
