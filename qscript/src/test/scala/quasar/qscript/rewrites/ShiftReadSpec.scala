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

package quasar.qscript.rewrites

import slamdata.Predef.{List, Nil}

import quasar.TreeMatchers
import quasar.contrib.pathy.AFile
import quasar.fp._
import quasar.contrib.iota._
import quasar.qscript._

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

      val qScript: Fix[QS] =
        chainQS(
          qsdsl.fix.Read[AFile](sampleFile),
          qsdsl.fix.LeftShift(_, qsdsl.recFunc.Hole, ExcludeId, ShiftType.Array, OnUndefined.Omit, qsdsl.func.RightSide))

      val newQScript: Fix[QST] =
        qScript.codyna(
          rewrite.normalizeTJ[QST] >>> (_.embed),
          ((_: Fix[QS]).project) >>> (ShiftRead[Fix, QS, QST].shiftRead(idPrism.reverseGet)(_)))

      newQScript must
        beTreeEqual(
          qstdsl.fix.Map(
            qstdsl.fix.ShiftedRead[AFile](sampleFile, ExcludeId),
            qstdsl.recFunc.ProjectIndexI(qstdsl.recFunc.Hole, 1)))
    }

    "shift a simple aggregated read" in {
      val qScript: Fix[QS] = qsdsl.fix.Reduce(
        qsdsl.fix.LeftShift(
          qsdsl.fix.Read[AFile](rootDir </> dir("foo") </> file("bar")),
          qsdsl.recFunc.Hole,
          ExcludeId,
          ShiftType.Array,
          OnUndefined.Omit,
          qsdsl.func.RightSide),
        Nil,
        List(ReduceFuncs.Count(qsdsl.func.Hole)),
        qsdsl.func.MakeMapS("0", qsdsl.func.ReduceIndex(0.right)))

      val newQScript: Fix[QST] =
        qScript.codyna(
          rewrite.normalizeTJ[QST] >>> (_.embed),
          ((_: Fix[QS]).project) >>> (ShiftRead[Fix, QS, QST].shiftRead(idPrism.reverseGet)(_)))

      newQScript must
        beTreeEqual(
          qstdsl.fix.Reduce(
            qstdsl.fix.ShiftedRead[AFile](rootDir </> dir("foo") </> file("bar"), IncludeId),
            Nil,
            List(ReduceFuncs.Count(qstdsl.func.ProjectIndexI(qstdsl.func.Hole, 1))),
            qstdsl.func.MakeMapS("0", qstdsl.func.ReduceIndex(0.right))))
    }
  }
}
