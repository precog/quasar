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

import quasar.Qspec
import quasar.contrib.iota._
import quasar.contrib.pathy._
import quasar.ejson.{EJson, Fixed}
import quasar.fp._
import quasar.qscript.{ExcludeId, QScriptHelpers}

import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._

object OptimizeSpec extends Qspec with QScriptHelpers {
  import qstdsl.{fix, recFunc}

  val ejs = Fixed[Fix[EJson]]

  def optimize(expr: Fix[QST]): Fix[QST] =
    expr.transCata[Fix[QST]](Optimize[Fix, QST])

  "optimize" >> {

    "elide outer no-op map" >> {
      val src: Fix[QST] =
        fix.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId)

      optimize(fix.Map(src, recFunc.Hole)) must equal(src)
    }

    "elide nested no-op map" >> {
      val src: Fix[QST] =
        fix.Map(
          fix.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
          recFunc.ProjectKeyS(recFunc.Hole, "bar"))

      val qs: Fix[QST] =
        fix.Filter(
          fix.Map(src, recFunc.Hole),
          recFunc.ProjectKeyS(recFunc.Hole, "baz"))

      val expected: Fix[QST] =
        fix.Filter(
          src,
          recFunc.ProjectKeyS(recFunc.Hole, "baz"))

      optimize(qs) must equal(expected)
    }

    "elide double no-op map" >> {
      val src: Fix[QST] =
        fix.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId)

      optimize(fix.Map(fix.Map(src, recFunc.Hole), recFunc.Hole)) must equal(src)
    }
  }
}
