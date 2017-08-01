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
import quasar.ejson.implicits._
import quasar.fp._
import quasar.qscript.MapFuncsCore._
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
        chain(
          ReadR(sampleFile),
          QC.inj(LeftShift((), HoleF, ExcludeId, Free.point(RightSide))))

      val newQScript =
        qScript.codyna(
          rewrite.normalize[QST] >>> (_.embed),
          ((_: Fix[QS]).project) >>> (ShiftRead[Fix, QS, QST].shiftRead(idPrism.reverseGet)(_)))

      newQScript must
      beTreeEqual(
        Fix(QCT.inj(Map(
          Fix(SRTF.inj(Const[ShiftedRead[AFile], Fix[QST]](ShiftedRead(sampleFile, ExcludeId)))),
          Free.roll(MFC(ProjectIndex(HoleF, IntLit(1))))))))
    }

    "shift a simple aggregated read" in {
      convert(lc.some,
        structural.MakeObject(
          lpf.constant(Data.Str("0")),
          agg.Count(lpRead("/foo/bar")).embed).embed).map(
        _.codyna(
          rewrite.normalize[QST] >>> (_.embed),
          ((_: Fix[QS]).project) >>> (ShiftRead[Fix, QS, QST].shiftRead(idPrism.reverseGet)(_)))) must
      beTreeEqual(chain(
        SRTF.inj(Const[ShiftedRead[AFile], Fix[QST]](
          ShiftedRead(rootDir </> dir("foo") </> file("bar"), IncludeId))),
        QCT.inj(Reduce((),
          NullLit(),
          List(ReduceFuncs.Count(Free.roll(MFC(ProjectIndex(HoleF, IntLit[Fix, Hole](1)))))),
          Free.roll(MFC(MakeMap(StrLit("0"), Free.point(ReduceIndex(0.some)))))))).some)
    }
  }
}
