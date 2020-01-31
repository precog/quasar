/*
 * Copyright 2014–2020 SlamData Inc.
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

import slamdata.Predef.{Char, Int}

import quasar.Qspec

import cats.instances.char._
import cats.instances.int._
import cats.instances.option._
import cats.kernel.laws.discipline.OrderTests

import org.specs2.mutable.SpecificationLike

import org.typelevel.discipline.specs2.mutable.Discipline

object JoinKeySpec extends Qspec
    with SpecificationLike
    with JoinKeyGenerator
    with Discipline {

  checkAll("Order[JoinKey[Char, Int]]", OrderTests[JoinKey[Char, Int]].order)
}
