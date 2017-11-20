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

package quasar.qscript

import slamdata.Predef._
import quasar.{Data, TreeMatchers}
import quasar.contrib.pathy.AFile
import quasar.fp._
import quasar.std.StdLib._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._
import scalaz._, Scalaz._

class ShiftReadSpec extends quasar.Qspec with QScriptHelpers with TreeMatchers {
  val rewrite = new Rewrite[Fix]

  "shiftRead" should {
    "eliminate Read nodes from a simple query" in {
      val sampleFile = rootDir </> file("bar")

      val qScript =
        chainQS(
          qsdsl.fix.Read[AFile](sampleFile),
          qsdsl.fix.LeftShift(_, qsdsl.func.Hole, ExcludeId, qsdsl.func.RightSide))

      val newQScript =
        qScript.codyna(
          rewrite.normalizeTJ[QST] >>> (_.embed),
          ((_: Fix[QS]).project) >>> (ShiftRead[Fix, QS, QST].shiftRead(idPrism.reverseGet)(_)))

      newQScript must
        beTreeEqual(
          qstdsl.fix.Map(
            qstdsl.fix.ShiftedRead[AFile](sampleFile, ExcludeId),
            qstdsl.func.ProjectIndexI(qstdsl.func.Hole, 1)))
    }

    "shift a simple aggregated read" in {
      import qstdsl._
      convert(lc.some,
        structural.MakeMap(
          lpf.constant(Data.Str("0")),
          agg.Count(lpRead("/foo/bar")).embed).embed).map(
        _.codyna(
          rewrite.normalizeTJ[QST] >>> (_.embed),
          ((_: Fix[QS]).project) >>> (ShiftRead[Fix, QS, QST].shiftRead(idPrism.reverseGet)(_)))) must
        beTreeEqual(
          fix.Reduce(
            fix.ShiftedRead[AFile](rootDir </> dir("foo") </> file("bar"), IncludeId),
            Nil,
            List(ReduceFuncs.Count(func.ProjectIndexI(func.Hole, 1))),
            func.MakeMapS("0", func.ReduceIndex(0.right))).some)
    }
  }
}
