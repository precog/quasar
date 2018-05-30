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

package quasar.fp.numeric

import slamdata.Predef._
import quasar.contrib.algebra._
import quasar.contrib.specs2.Spec

import org.scalacheck.{Arbitrary, Gen}, Arbitrary.arbitrary
import scalaz.scalacheck.{ScalazProperties => propz}
import scalaz.{NonEmptyList, Show}
import scalaz.std.option._
import scalaz.syntax.foldable1._
import scalaz.syntax.equal._
import scalaz.syntax.std.boolean._
import spire.laws.arb._
import spire.math.Real

final class SampleStatsSpec extends Spec with SampleStatsArbitrary {
  implicit val showReal: Show[Real] =
    Show.showFromToString

  /** A list of moderately sized reals, to ensure they don't grow so big that
    * computation gets prohibitively slow.
    */
  case class ModerateReals(rs: NonEmptyList[Real])

  object ModerateReals {
    implicit val arbitraryModerateReals: Arbitrary[ModerateReals] =
      Arbitrary(for {
        h <- arbitrary[Double]
        t <- Gen.listOf(Gen.choose(-100000000.0, 100000000.0))
      } yield ModerateReals(NonEmptyList(Real(h), t.map(Real(_)): _*)))
  }

  def μ(xs: NonEmptyList[Real]): Real =
    xs.foldLeft1(_ + _) / Real(xs.length)

  // The k-th moment about the mean.
  def `mₖ`(k: Int, xs: NonEmptyList[Real]): Real = {
    val  n   = Real(xs.length)
    val `m₁` = μ(xs)
    xs.map(x => (x - `m₁`) ** k).foldLeft1(_ + _)
  }

  checkAll(propz.equal.laws[SampleStats[Real]])
  checkAll(propz.monoid.laws[SampleStats[Real]])

  "commutative monoid" >> {
    "merge is commutative" >> prop { (x: SampleStats[Real], y: SampleStats[Real]) =>
      (x + y) must equal(y + x)
    }
  }

  "correctness" >> {
    "mean" >> prop { mr: ModerateReals =>
      val xs = mr.rs
      val ss = SampleStats.fromFoldable(xs)

      ss.mean must equal(μ(xs))
    }

    // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Na.C3.AFve_algorithm
    "variance" >> prop { mr: ModerateReals =>
      val xs   = mr.rs
      val ss   = SampleStats.fromFoldable(xs)
      val n    = Real(xs.length)
      val ssq  = xs.map(x => x * x).foldLeft1(_ + _)
      val sum  = xs.foldLeft1(_ + _)
      val `σ²` = (ssq - ((sum * sum) / n)) / n

      ss.variance must beSome(equal(`σ²`))
    }

    // https://en.wikipedia.org/wiki/Skewness#Pearson.27s_moment_coefficient_of_skewness
    "skewness" >> prop { mr: ModerateReals =>
      val xs   = mr.rs
      val ss   = SampleStats.fromFoldable(xs)
      val n    = Real(xs.length)
      val `m₂` = `mₖ`(2, xs)
      val `m₃` = `mₖ`(3, xs)
      val  sk  = (`m₂` ≠ Real(0)).option(n.sqrt * `m₃` /  `m₂`.fpow(1.5))

      ss.skewness must equal(sk)
    }

    // https://en.wikipedia.org/wiki/Kurtosis#Sample_kurtosis
    "kurtosis" >> prop { mr: ModerateReals =>
      val  xs  = mr.rs
      val  ss  = SampleStats.fromFoldable(xs)
      val  n   = Real(xs.length)
      val `m₂` = `mₖ`(2, xs)
      val `m₄` = `mₖ`(4, xs)
      val  k   = (`m₂` ≠ Real(0)).option(n * `m₄` /  (`m₂` ** 2))

      ss.kurtosis must equal(k)
    }

    "population variance" >> prop { mr: ModerateReals =>
      val xs   = mr.rs
      val ss   = SampleStats.fromFoldable(xs)
      val n    = Real(xs.length)
      val ssq  = xs.map(x => x * x).foldLeft1(_ + _)
      val sum  = xs.foldLeft1(_ + _)
      val `popσ²` = (n ≠ Real(1)).option((ssq - ((sum * sum) / n)) / (n - 1))

      ss.populationVariance must equal(`popσ²`)
    }

    "population skewness" >> prop { mr: ModerateReals =>
      val xs   = mr.rs
      val n    = Real(xs.length)
      val ss   = SampleStats.fromFoldable(xs)
      val `m₂` = `mₖ`(2, xs)
      val `m₃` = `mₖ`(3, xs)
      val den  = (n - 2) * `m₂`.fpow(1.5)
      val psk  = (den ≠ Real(0)).option((n * (n - Real(1)).sqrt * `m₃`) / den)

      ss.populationSkewness must equal(psk)
    }

    "population kurtosis" >> prop { mr: ModerateReals =>
      val  xs  = mr.rs
      val  ss  = SampleStats.fromFoldable(xs)
      val  n   = Real(xs.length)
      val `m₂` = `mₖ`(2, xs)
      val `m₄` = `mₖ`(4, xs)
      val den  = (n - 2) * (n - 3) * (`m₂` ** 2)
      val  pk  = (den ≠ Real(0)).option((n * (n + 1) * (n - 1) * `m₄`) / den)

      ss.populationKurtosis must equal(pk)
    }
  }
}
