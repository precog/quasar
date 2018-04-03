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

package quasar.fs.mount.cache

import quasar.contrib.pathy._
import quasar.fs.mount._
import slamdata.Predef._

import java.time.{Instant, LocalDateTime, ZoneOffset}

import org.scalacheck.Arbitrary.{arbitrary => arb}
import org.scalacheck._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._

trait ViewCacheArbitrary {

  implicit val arbPostgresInstant: Arbitrary[Instant] = {
    // postgres defines the Timestamp range to be 4713 BC to 294276 AD
    val minTimestamp: Long =
      LocalDateTime.of(-4713, 1, 1, 0, 0, 0).toEpochSecond(ZoneOffset.UTC)

    val maxTimestamp: Long =
      LocalDateTime.of(294276, 12, 31, 23, 59, 59).toEpochSecond(ZoneOffset.UTC)

    val time: Gen[Long] = Gen.choose(minTimestamp, maxTimestamp)

    Arbitrary(time.map(Instant.ofEpochSecond))
  }

  implicit val arbViewCacheStatus: Arbitrary[ViewCache.Status] =
    Arbitrary(Gen.oneOf(
      ViewCache.Status.Pending,
      ViewCache.Status.Successful,
      ViewCache.Status.Failed))

  implicit val arbViewCache: Arbitrary[ViewCache] =
    Arbitrary(
      (MountConfigArbitrary.genViewConfig ⊛ arb[Option[Instant]] ⊛ arb[Option[Long]] ⊛
       arb[Int] ⊛ arb[Option[String]] ⊛ arb[Option[Instant]] ⊛ arb[Long] ⊛ arb[Instant] ⊛
       arb[ViewCache.Status] ⊛ arb[Option[String]] ⊛ arb[AFile] ⊛ arb[Option[AFile]])(
        ViewCache.apply))
}

object ViewCacheArbitrary extends ViewCacheArbitrary
