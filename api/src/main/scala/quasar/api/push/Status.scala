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

package quasar.api.push

import slamdata.Predef._

import java.time.Instant

import scalaz.{Equal, Show}

sealed trait Status extends Product with Serializable

object Status {
  final case class Finished(startedAt: Instant, finishedAt: Instant) extends Status
  final case class Running(startedAt: Instant) extends Status
  final case class Canceled(startedAt: Instant, canceledAt: Instant) extends Status
  final case class Failed(th: Throwable, startedAt: Instant, failedAt: Instant) extends Status

  implicit val equal: Equal[Status] =
    Equal.equalA

  implicit val show: Show[Status] = Show.shows {
    case Finished(startedAt, finishedAt) =>
      s"Finished($startedAt, $finishedAt)"
    case Running(startedAt) =>
      s"Running($startedAt)"
    case Canceled(startedAt, canceledAt) =>
      s"Canceled($startedAt, $canceledAt)"
    case Failed(ex, startedAt, finishedAt) =>
      s"Failed(${ex.getMessage}, $startedAt, $finishedAt)" + "\n\n" + s"$ex"
  }

  def finished(startedAt: Instant, finishedAt: Instant): Status =
    Finished(startedAt, finishedAt)

  def running(startedAt: Instant): Status =
    Running(startedAt)

  def canceled(startedAt: Instant, canceledAt: Instant): Status =
    Canceled(startedAt, canceledAt)

  def failed(ex: Throwable, startedAt: Instant, failedAt: Instant): Status =
    Failed(ex, startedAt, failedAt)
}
