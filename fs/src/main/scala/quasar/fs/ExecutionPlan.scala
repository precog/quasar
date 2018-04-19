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

package quasar.fs

import slamdata.Predef._
import quasar.contrib.pathy.APath

import argonaut._, Argonaut._, ArgonautScalaz._
import monocle.macros.Lenses
import scalaz._, Scalaz._

@Lenses
final case class ExecutionPlan(typ: FileSystemType, physicalPlan: String, inputs: ISet[APath])

object ExecutionPlan {
  import APath._

  implicit val equal: Equal[ExecutionPlan] =
    Equal.equalBy(p => (p.typ, p.physicalPlan, p.inputs))

  implicit val show: Show[ExecutionPlan] =
    Show.shows(p =>
      s"ExecutionPlan[${p.typ}](inputs = ${p.inputs.shows})\n\n${p.physicalPlan}")

  implicit val codecJson: CodecJson[ExecutionPlan] =
    casecodec3(ExecutionPlan.apply, ExecutionPlan.unapply)("type", "physicalPlan", "inputs")
}
