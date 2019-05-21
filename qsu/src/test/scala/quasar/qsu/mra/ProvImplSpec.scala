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

package quasar.qsu.mra

import slamdata.Predef.{Boolean, Char, Int}

import scala.Predef.implicitly

import cats.{Eq, Show}
import cats.instances.boolean._
import cats.instances.char._
import cats.instances.int._

import org.scalacheck.Arbitrary

import org.specs2.scalacheck.Parameters

object ProvImplSpec extends {

  val prov: Provenance.Aux[Char, Int, Boolean, Uop[Identities[Dim[Char, Int, Boolean]]]] =
    ProvImpl[Char, Int, Boolean]

  val params = Parameters(maxSize = 8, workers = 2)

}   with ProvenanceSpec[Char, Int, Boolean]
    with UopGenerator
    with IdentitiesGenerator
    with DimGenerator {

  type Dims = Uop[Identities[Dim[Char, Int, Boolean]]]

  def ts = (true, false)
  def ss = ('a', 'b', 'c')
  def vs = (1, 2, 3)

  def PArbitrary = implicitly[Arbitrary[Dims]]
  def PEq = Eq[Dims]
  def PShow = Show[Dims]
}
