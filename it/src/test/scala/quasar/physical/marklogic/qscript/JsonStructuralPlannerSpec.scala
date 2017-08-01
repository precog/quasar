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

package quasar.physical.marklogic.qscript

import slamdata.Predef._
import quasar.physical.marklogic.{DocType, ErrorMessages}
import quasar.physical.marklogic.xquery._

import scalaz._, Scalaz._

// TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
import WriterT.writerTMonadListen

final class JsonStructuralPlannerSpec
  extends StructuralPlannerSpec[JsonStructuralPlannerSpec.JsonPlan, DocType.Json] {

  import JsonStructuralPlannerSpec.JsonPlan

  val toM = λ[JsonPlan ~> M] { xp =>
    WriterT.writer(xp.run.eval(1)).liftM[EitherT[?[_], ErrorMessages, ?]]
  }
}

object JsonStructuralPlannerSpec {
  type JsonPlan[A] = PrologT[State[Long, ?], A]
}
