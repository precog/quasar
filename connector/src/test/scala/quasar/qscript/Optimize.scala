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

import quasar.fp._
import quasar.qscript.MapFuncsCore._

import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._

class QScriptOptimizeSpec extends quasar.Qspec with QScriptHelpers {
  val optimize = new Optimize[Fix]

  def optimizeExpr(expr: Fix[QS]): Fix[QS] =
    expr.transCata[Fix[QS]](optimize.optimize[QS, QS](reflNT))

  "optimizer" should {
    "move subset before map" in {
      val from: QS[FreeQS] =
        QC.inj(Map(
          Free.roll(QST[QS].inject(QC.inj(Filter(HoleQS, BoolLit(true))))),
          ProjectFieldR(HoleF, StrLit("foo"))))

      val count: QS[FreeQS] =
        QC.inj(Map(HoleQS, ProjectIndexR(HoleF, IntLit(2))))

      val input: QS[Fix[QS]] =
        QC.inj(Subset(
          RootR.embed,
          Free.roll(QST[QS].inject(from)),
          Take,
          Free.roll(QST[QS].inject(count))))

      val output: QS[Fix[QS]] =
        QC.inj(Map(
          QC.inj(Subset(
            RootR.embed,
            Free.roll(QST[QS].inject(QC.inj(Filter(HoleQS, BoolLit(true))))),
            Take,
            Free.roll(QST[QS].inject(count)))).embed,
          ProjectFieldR(HoleF, StrLit("foo"))))

      optimizeExpr(input.embed) must equal(output.embed)
    }

    "move filter before union" in {
      val lBranch: QS[FreeQS] =
        QC.inj(Map(HoleQS, ProjectFieldR(HoleF, StrLit("foo"))))

      val rBranch: QS[FreeQS] =
        QC.inj(Map(HoleQS, ProjectIndexR(HoleF, IntLit(2))))

      val input: QS[Fix[QS]] =
        QC.inj(Filter(
          QC.inj(Union(
            RootR.embed,
            Free.roll(QST[QS].inject(lBranch)),
            Free.roll(QST[QS].inject(rBranch)))).embed,
          EqR(AddR(HoleF, IntLit(1)), IntLit(5))))

      val output: QS[Fix[QS]] =
        QC.inj(Union(
          RootR.embed,
          Free.roll(QST[QS].inject(QC.inj(Filter(
            Free.roll(QST[QS].inject(lBranch)),
            EqR(AddR(HoleF, IntLit(1)), IntLit(5)))))),
          Free.roll(QST[QS].inject(QC.inj(Filter(
            Free.roll(QST[QS].inject(rBranch)),
            EqR(AddR(HoleF, IntLit(1)), IntLit(5))))))))

      optimizeExpr(input.embed) must equal(output.embed)
    }
  }
}
