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

import scala.concurrent.Future

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout

import quasar.precog.common.Path
import quasar.precog.common.security._
import quasar.precog.util.PrecogUnit
import quasar.yggdrasil._
import quasar.yggdrasil.execution.EvaluationContext

import java.util.UUID

import org.quartz.CronExpression

import scalaz._

trait Scheduler[M[+_]] {
  def enabled: Boolean

  def addTask(repeat: Option[CronExpression], apiKey: APIKey, authorities: Authorities, context: EvaluationContext, source: Path, sink: Path, timeoutMillis: Option[Long]): EitherT[M, String, UUID]

  def deleteTask(id: UUID): EitherT[M, String, PrecogUnit]

  def statusForTask(id: UUID, limit: Option[Int]): EitherT[M, String, Option[(ScheduledTask, Seq[ScheduledRunReport])]]
}

class ActorScheduler(scheduler: ActorRef, timeout: Timeout) extends Scheduler[Future] {
  implicit val requestTimeout = timeout
  val enabled = true

  def addTask(repeat: Option[CronExpression], apiKey: APIKey, authorities: Authorities, context: EvaluationContext, source: Path, sink: Path, timeoutMillis: Option[Long]): EitherT[Future, String, UUID] = EitherT {
    (scheduler ? AddTask(repeat, apiKey, authorities, context, source, sink, timeoutMillis)).mapTo[String \/ UUID]
  }

  def deleteTask(id: UUID) = EitherT {
    (scheduler ? DeleteTask(id)).mapTo[String \/ PrecogUnit]
  }

  def statusForTask(id: UUID, limit: Option[Int]) = EitherT {
    (scheduler ? StatusForTask(id, limit)).mapTo[String \/ Option[(ScheduledTask, Seq[ScheduledRunReport])]]
  }
}

object NoopScheduler {
  def apply[M[+_]: Monad] = new NoopScheduler[M]
}

class NoopScheduler[M[+_]](implicit M: Monad[M]) extends Scheduler[M] {
  val enabled = false

  def addTask(repeat: Option[CronExpression], apiKey: APIKey, authorities: Authorities, context: EvaluationContext, source: Path, sink: Path, timeoutMillis: Option[Long]) = sys.error("No scheduling available")

  def deleteTask(id: UUID) = sys.error("No scheduling available")

  def statusForTask(id: UUID, limit: Option[Int]) = sys.error("No scheduling available")
}
