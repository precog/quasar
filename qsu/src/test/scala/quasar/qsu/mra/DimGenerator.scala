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

import slamdata.Predef._

import quasar.pkg.tests._

import java.lang.System

import org.scalacheck.Cogen

trait DimGenerator {
  implicit def arbitraryDim[S: Arbitrary, V: Arbitrary, T: Arbitrary]: Arbitrary[Dim[S, V, T]] = {
    val D = Dim.Optics[S, V, T]

    Arbitrary(Gen.frequency(
      1 -> Gen.const(D.fresh()),
      5 -> arbitrary[(V, T)].map(D.inflate(_)),
      5 -> arbitrary[(S, T)].map(D.inject(_)),
      5 -> arbitrary[(S, T)].map(D.project(_))))
  }

  implicit def cogenDim[S: Cogen, V: Cogen, T: Cogen]: Cogen[Dim[S, V, T]] =
    Cogen((k, dim) => dim match {
      case d @ Dim.Fresh() => Cogen[Int].perturb(k, System.identityHashCode(d))
      case Dim.Inflate(v, t) => Cogen[(Int, V, T)].perturb(k, (1, v, t))
      case Dim.Inject(s, t) => Cogen[(Int, S, T)].perturb(k, (2, s, t))
      case Dim.Project(s, t) => Cogen[(Int, S, T)].perturb(k, (3, s, t))
    })
}

object DimGenerator extends DimGenerator
