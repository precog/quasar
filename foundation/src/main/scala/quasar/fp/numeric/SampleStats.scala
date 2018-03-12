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

import scalaz._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.equal._
import scalaz.syntax.foldable._
import scalaz.syntax.show._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import spire.algebra.{AdditiveMonoid, Field, NRoot, Rig}
import spire.implicits._

/** Statistics based on a sample from a population.
  *
  * @param size the number of observations
  * @param m1 the first central moment
  * @param m2 the second central moment
  * @param m3 the third central moment
  * @param m4 the fourth central moment
  */
final case class SampleStats[A](
    size: A,
    m1: A,
    m2: A,
    m3: A,
    m4: A) {

  // Sample Statistics

  def mean: A = m1

  def variance(implicit E: Equal[A], F: Field[A]): Option[A] =
    (size ≠ F.zero).option(m2 / size)

  def stddev(implicit E: Equal[A], F: Field[A], R: NRoot[A]): Option[A] =
    variance map (R.sqrt)

  def skewness(implicit E: Equal[A], F: Field[A], R: NRoot[A]): Option[A] =
    (m2 ≠ F.zero).option((R.sqrt(size) * m3) / R.fpow(m2, ℝ(1.5)))

  def kurtosis(implicit E: Equal[A], F: Field[A]): Option[A] =
    (m2 ≠ F.zero).option((size * m4) / (m2 * m2))

  def excessKurtosis(implicit E: Equal[A], F: Field[A]): Option[A] =
    kurtosis map (_ - ℤ(3))


  // Population Estimates

  /** Unbiased estimated variance of the population sampled. */
  def populationVariance(implicit E: Equal[A], F: Field[A]): Option[A] =
    (size ≠ F.one).option(m2 / (size - F.one))

  /** Estimated standard deviation of the population sampled. */
  def populationStddev(implicit E: Equal[A], F: Field[A], R: NRoot[A]): Option[A] =
    populationVariance map (R.sqrt)

  /** Estimated skewness of the population sampled. */
  def populationSkewness(implicit E: Equal[A], F: Field[A], R: NRoot[A]): Option[A] =
    ((m2 ≠ F.zero) && (size ≠ ℤ(2)))
      .option((size * R.sqrt(size - F.one) * m3) / ((size - ℤ(2)) * R.fpow(m2, ℝ(1.5))))

  /** Estimated kurtosis of the population sampled. */
  def populationKurtosis(implicit E: Equal[A], F: Field[A]): Option[A] = {
    val denom = (size - ℤ(2)) * (size - ℤ(3)) * m2 * m2
    (denom ≠ F.zero).option((size * (size + F.one) * (size - F.one) * m4) / denom)
  }

  /** Estimated excess kurtosis of the population sampled. */
  def populationExcessKurtosis(implicit E: Equal[A], F: Field[A]): Option[A] =
    populationKurtosis map (_ - ℤ(3))


  // Operations

  /** Returns new stats that include the given observation.
    *
    * NB: This is equivalent to `merge`, simplified for the case when one of
    *     the stats has exactly one observation.
    */
  def observe(x: A)(implicit F: Field[A]): SampleStats[A] = {
    val n       = size + F.one
    val δ       = x - m1
    val `δ/n`   = δ / n
    val `δ²/n²` = `δ/n` * `δ/n`
    val t1      = δ * `δ/n` * size

    SampleStats(
      n,

      m1 + `δ/n`,

      m2 + t1,

      m3 + (t1 * `δ/n` * (n - ℤ(2))) -
           (ℤ(3) * `δ/n` * m2),

      m4 + (t1 * `δ²/n²` * ((n * n) - (ℤ(3) * n) + ℤ(3))) +
           (ℤ(6) * `δ²/n²` * m2) -
           (ℤ(4) * `δ/n` * m3)
    )
  }

  /** Combine with another `SampleStats` to produce stats about the union of
    * their observations.
    *
    * Implementation via Chan et al.
    * https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
    */
  def merge(b: SampleStats[A])(implicit E: Equal[A], F: Field[A]): SampleStats[A] = {
    def doMerge: SampleStats[A] = {
      val `n₁`  = size
      val `n₁²` = `n₁` * `n₁`

      val `n₂`  = b.size
      val `n₂²` = `n₂` * `n₂`

      val `n₁₂` = `n₁` * `n₂`

      val  n    = `n₁` + `n₂`
      val `n²`  =  n   * n
      val `n³`  = `n²` * n

      val  δ    = b.m1 - m1
      val `δ²`  =  δ   *  δ
      val `δ³`  = `δ²` *  δ
      val `δ⁴`  = `δ²` * `δ²`

      SampleStats(
        n,

        ((`n₁` * m1) + (`n₂` * b.m1)) / n,

        m2 + b.m2 + ((`δ²` * `n₁₂`) / n),

        m3 + b.m3 + ((`δ³` * `n₁₂` * (`n₁` - `n₂`)) / `n²`) +
                    ((ℤ(3) * δ * ((`n₁` * b.m2) - (`n₂` * m2))) / n),

        m4 + b.m4 + ((`δ⁴` * `n₁₂` * (`n₁²` - `n₁₂` + `n₂²`)) / `n³`) +
                    ((ℤ(6) * `δ²` * ((`n₁²` * b.m2) + (`n₂²` * m2))) / `n²`) +
                    ((ℤ(4) * δ * ((`n₁` * b.m3) - (`n₂` * m3))) / n)
      )
    }

         if (b.size ≟ F.zero) this
    else if (size   ≟ F.zero) b
    else if (b.size ≟  F.one) observe(b.mean)
    else if (size   ≟  F.one) b.observe(mean)
    else    doMerge
  }

  /** Alias for `merge`. */
  def + (b: SampleStats[A])(implicit E: Equal[A], F: Field[A]): SampleStats[A] =
    merge(b)

  ////

  private def ℝ (d: Double)(implicit F: Field[A]): A =
    F.fromDouble(d)

  private def ℤ (i: Int)(implicit F: Field[A]): A =
    F.fromInt(i)
}

object SampleStats extends SampleStatsInstances {
  /** Stats over zero observations. */
  def empty[A](implicit A: AdditiveMonoid[A]): SampleStats[A] =
    freq(A.zero, A.zero)

  /** Stats over the frequency of an observation. */
  def freq[A](count: A, a: A)(implicit A: AdditiveMonoid[A]): SampleStats[A] =
    SampleStats[A](count, a, A.zero, A.zero, A.zero)

  /** Stats over a `Foldable` of samples. */
  def fromFoldable[F[_]: Foldable, A: Field](fa: F[A]): SampleStats[A] =
    fa.foldLeft(empty[A])(_ observe _)

  /** Stats over a single observation. */
  def one[A](a: A)(implicit R: Rig[A]): SampleStats[A] =
    freq(R.one, a)
}

sealed abstract class SampleStatsInstances {
  implicit def equal[A: Equal]: Equal[SampleStats[A]] =
    Equal.equalBy(ss => (ss.size, ss.m1, ss.m2, ss.m3, ss.m4))

  // TODO{cats}: CommutativeMonoid
  implicit def monoid[A: Equal: Field]: Monoid[SampleStats[A]] =
    Monoid.instance((a, b) => a + b, SampleStats.empty[A])

  implicit def show[A: Show: Equal: Field: NRoot]: Show[SampleStats[A]] =
    Show.shows(ss => "SampleStats" + IList(
        s"n = ${ss.size.shows}"
      , s"μ = ${ss.mean.shows}"
      , s"σ² = ${ss.populationVariance.map(_.shows) | "?"}"
      , s"σ = ${ss.populationStddev.map(_.shows) | "?"}"
    ).shows)
}
