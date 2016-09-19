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

package quasar.qscript

import quasar.fp._
import quasar.qscript.MapFuncs._

import matryoshka._, FunctorT.ops._
import pathy.Path._
import scalaz._

class ShiftReadSpec extends quasar.Qspec with QScriptHelpers {
  "shiftRead" should {
    "eliminate Read nodes from a simple query" in {
      val sampleFile = rootDir </> file("bar")
      val optimize = new Optimize[Fix]

      val qScript =
        chain(
          ReadR(sampleFile),
          QC.inj(LeftShift((), HoleF, Free.point(RightSide))))

      val newQScript = transFutu(qScript)(ShiftRead[Fix, QScriptTotal[Fix, ?], QScriptTotal[Fix, ?]].shiftRead(idPrism.reverseGet)((_: QScriptTotal[Fix, Fix[QScriptTotal[Fix, ?]]])))

      // TODO: Optimize away the `IncludeId`.
      newQScript.transCata(optimize.applyAll) must_=
        Fix(QC.inj(Map(
          Fix(SR.inj(Const(ShiftedRead(sampleFile, IncludeId)))),
          Free.roll(ProjectIndex(HoleF, IntLit(1))))))
    }
  }
}
