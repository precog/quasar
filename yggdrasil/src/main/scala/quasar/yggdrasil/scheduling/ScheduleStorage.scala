/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.yggdrasil.scheduling

import java.util.UUID

import scalaz.EitherT

trait ScheduleStorage[M[+_]] {
  def addTask(task: ScheduledTask): EitherT[M, String, ScheduledTask]

  def deleteTask(id: UUID): EitherT[M, String, Option[ScheduledTask]]

  def reportRun(report: ScheduledRunReport): M[Unit]

  def statusFor(id: UUID, lastLimit: Option[Int]): M[Option[(ScheduledTask, Seq[ScheduledRunReport])]]

  def listTasks: M[Seq[ScheduledTask]]
}
