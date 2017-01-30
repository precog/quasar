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
import quasar.effect._
import quasar.fp.eitherT._
import quasar.physical.marklogic.DocType
import quasar.physical.marklogic.xquery._

import matryoshka._
import scalaz._, Scalaz._

import JsonMarkLogicStdLibSpec.SLib

final class JsonMarkLogicStdLibSpec extends MarkLogicStdLibSpec[SLib, DocType.Json] {
  def toMain[G[_]: Monad: Capture](xqy: SLib[XQuery]): RunT[G, MainModule] = {
    val (prologs, q) = xqy.leftMap(e => ko(e.shows).toResult).run.run.eval(1)
    EitherT.fromDisjunction[G](q map (MainModule(Version.`1.0-ml`, prologs, _)))
  }
}

object JsonMarkLogicStdLibSpec {
  type SLib[A] = MarkLogicPlanErrT[WriterT[State[Long, ?], Prologs, ?], A]
}
