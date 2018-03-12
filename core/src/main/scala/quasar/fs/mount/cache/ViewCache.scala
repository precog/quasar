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

import slamdata.Predef._
import quasar.contrib.pathy.AFile
import quasar.fs.mount.MountConfig

import java.time.{Duration => JDuration, Instant}
import scala.concurrent.duration._

import scalaz._, Scalaz._

final case class ViewCache(
  viewConfig: MountConfig.ViewConfig,
  lastUpdate: Option[Instant],
  executionMillis: Option[Long],
  cacheReads: Int,
  assignee: Option[String],
  assigneeStart: Option[Instant],
  maxAgeSeconds: Long,
  refreshAfter: Instant,
  status: ViewCache.Status,
  errorMsg: Option[String],
  dataFile: AFile,
  tmpDataFile: Option[AFile])

object ViewCache {
  sealed trait Status
  object Status {
    final case object Pending    extends Status
    final case object Successful extends Status
    final case object Failed     extends Status

    implicit val equal: Equal[Status] = Equal.equalRef
  }

  def mk(
    viewConfig: MountConfig.ViewConfig,
    maxAgeSeconds: Long,
    refreshAfter: Instant,
    dataFile: AFile)
      : ViewCache =
    ViewCache(
      viewConfig = viewConfig,
      lastUpdate = none,
      executionMillis = none,
      cacheReads = 0,
      assignee = none,
      assigneeStart = none,
      maxAgeSeconds = maxAgeSeconds,
      refreshAfter = refreshAfter,
      status = Status.Pending,
      errorMsg = none,
      dataFile = dataFile,
      tmpDataFile = none)

  // Hard coded to 80% of maxAge for now
  def expireAt(ts: Instant, maxAge: Duration): Throwable \/ Instant =
    \/.fromTryCatchNonFatal(ts.plus(JDuration.ofMillis((maxAge.toMillis.toDouble * 0.8).toLong)))

  implicit val equal: Equal[ViewCache] = {
    implicit val equalInstant: Equal[Instant] = Equal.equalA

    Equal.equal {
      case (ViewCache(l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l11, l12),
            ViewCache(r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12)) =>
        (l1: MountConfig) ≟ (r1: MountConfig) && l2 ≟ r2 && l3 ≟ r3 && l4 ≟ r4 && l5 ≟ r5 && l6 ≟ r6 &&
        l7 ≟ r7 && l8 ≟ r8 && l9 ≟ r9 && l10 ≟ r10 && l11 ≟ r11 && l12 ≟ r12
    }
  }

  implicit val show: Show[ViewCache] = Show.showFromToString
}
