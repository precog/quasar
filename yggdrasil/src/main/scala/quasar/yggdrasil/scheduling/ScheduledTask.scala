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

import scala.concurrent.duration.Duration

import quasar.blueeyes.json._
import quasar.blueeyes.json.serialization._
import quasar.blueeyes.json.serialization.Versioned._
import quasar.blueeyes.json.serialization.DefaultSerialization._

import quasar.precog.common.Path
import quasar.precog.common.security.{APIKey, Authorities}

import quasar.yggdrasil.execution.EvaluationContext

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.quartz.CronExpression

import scalaz._

import shapeless._

object CronExpressionSerialization {
  implicit val cronExpressionDecomposer = new Decomposer[CronExpression] {
    def decompose(expr: CronExpression) = JString(expr.toString)
  }

  implicit val cronExpressionExtractor = new Extractor[CronExpression] {
    def validated(jv: JValue) = jv match {
      case JString(expr) => Validation.fromTryCatchThrowable[CronExpression, java.text.ParseException](new CronExpression(expr)).leftMap(Extractor.Error.thrown)
      case invalid => Failure(Extractor.Error.invalid("Could not parse CRON expression from " + invalid))
    }
  }
}

case class ScheduledTask(id: UUID, repeat: Option[CronExpression], apiKey: APIKey, authorities: Authorities, context: EvaluationContext, source: Path, sink: Path, timeoutMillis: Option[Long]) {
  def taskName = "Scheduled %s -> %s".format(source, sink)
  def timeout = timeoutMillis map { to => Duration(to, TimeUnit.MILLISECONDS) }
}

object ScheduledTask {
  import CronExpressionSerialization._

  val schemaV1 = "id" :: "repeat" :: "apiKey" :: "authorities" :: "prefix" :: "source" :: "sink" :: "timeout" :: HNil

  implicit val decomposer: Decomposer[ScheduledTask] = decomposerV(schemaV1, Some("1.0".v))
  implicit val extractor:  Extractor[ScheduledTask]  = extractorV(schemaV1, Some("1.0".v))
}
