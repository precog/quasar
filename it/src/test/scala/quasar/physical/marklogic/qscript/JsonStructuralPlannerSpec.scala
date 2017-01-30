/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.marklogic.qscript

import quasar.Predef._
import quasar.fp.eitherT._
import quasar.physical.marklogic.DocType
import quasar.physical.marklogic.xml.QName
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._

import matryoshka._
import scalaz._, Scalaz._

final class JsonStructuralPlannerSpec
  extends StructuralPlannerSpec[JsonStructuralPlannerSpec.JsonPlan, DocType.Json] {

  import JsonStructuralPlannerSpec.JsonPlan

  val toM = λ[JsonPlan ~> M](xp => EitherT(WriterT.writer(xp.leftMap(_.shows.wrapNel).run.run.eval(1))))
  def asMapKey(qn: QName) = qn.xs.point[JsonPlan]
}

object JsonStructuralPlannerSpec {
  type JsonPlan[A] = MarkLogicPlanErrT[WriterT[State[Long, ?], Prologs, ?], A]
}
