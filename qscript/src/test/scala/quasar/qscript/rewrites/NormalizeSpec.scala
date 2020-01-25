/*
 * Copyright 2014–2019 SlamData Inc.
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

import quasar._
import quasar.api.resource.ResourcePath
import quasar.fp._
import quasar.contrib.iota._
import quasar.contrib.iota.SubInject
import quasar.qscript._

import matryoshka.data.Fix
import pathy.Path._
import scalaz._, Scalaz._

object NormalizeSpec extends quasar.Qspec {
  import IdStatus.ExcludeId

  type QST[A] = QScriptTotal[Fix, A]
  type QSNorm[A] = QScriptNormalized[Fix, A]

  def normalizeExpr(expr: Fix[QSNorm]): Fix[QSNorm] =
    NormalizeQScriptFreeMap(NormalizeQScript[Fix](expr))

  implicit def normalizedToTotal: Injectable[QSNorm, QST] =
    SubInject[QSNorm, QST]

  val qsdsl = construction.mkDefaults[Fix, QSNorm]

  "rewriter" should {

    import qsdsl._

    // select b[*] + c[*] from intArrays.data
    "normalize static projections in shift coalescing" in {

      val educated =
        fix.Map(
          fix.Map(
            fix.LeftShift(
              fix.LeftShift(
                fix.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("intArrays")), ExcludeId),
                recFunc.ProjectKeyS(recFunc.Hole, "b"),
                ExcludeId,
                ShiftType.Array,
                OnUndefined.Emit,
                func.ConcatMaps(
                  func.MakeMapS("original", func.LeftSide),
                  func.MakeMapS("0", func.RightSide))),
              recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "original"), "c"),
              ExcludeId,
              ShiftType.Array,
              OnUndefined.Emit,
              func.ConcatMaps(
                func.LeftSide,
                func.MakeMapS("1", func.RightSide))),
            recFunc.ConcatMaps(
              recFunc.MakeMapS("0", recFunc.ProjectKeyS(recFunc.Hole, "0")),
              recFunc.MakeMapS("1", recFunc.ProjectKeyS(recFunc.Hole, "1")))),
          recFunc.Add(
            recFunc.ProjectKeyS(recFunc.Hole, "0"),
            recFunc.ProjectKeyS(recFunc.Hole, "1")))

      val normalized =
        fix.LeftShift(
          fix.LeftShift(
            fix.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("intArrays")), ExcludeId),
            recFunc.ProjectKeyS(recFunc.Hole, "b"),
            ExcludeId,
            ShiftType.Array,
            OnUndefined.Emit,
            func.ConcatMaps(
              func.MakeMapS("original", func.LeftSide),
              func.MakeMapS("0", func.RightSide))),
          recFunc.ProjectKeyS(recFunc.ProjectKeyS(recFunc.Hole, "original"), "c"),
          ExcludeId,
          ShiftType.Array,
          OnUndefined.Emit,
          func.Add(
            func.ProjectKeyS(func.LeftSide, "0"),
            func.RightSide))

      normalizeExpr(educated) must equal(normalized)
    }

    // select (select a, b from zips).a + (select a, b from zips).b
    "normalize static projections in a contrived example" in {

      val educated =
        fix.Map(
          fix.Map(
            fix.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("zips")), ExcludeId),
            recFunc.ConcatMaps(
              recFunc.MakeMapS("a", recFunc.ProjectKeyS(recFunc.Hole, "a")),
              recFunc.MakeMapS("b", recFunc.ProjectKeyS(recFunc.Hole, "b")))),
          recFunc.Add(
            recFunc.ProjectKeyS(recFunc.Hole, "a"),
            recFunc.ProjectKeyS(recFunc.Hole, "b")))

      val normalized =
        fix.Map(
          fix.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("zips")), ExcludeId),
            recFunc.Add(
              recFunc.ProjectKeyS(recFunc.Hole, "a"),
              recFunc.ProjectKeyS(recFunc.Hole, "b")))

      normalizeExpr(educated) must equal(normalized)
    }

    "normalize within a Union" in {

      val educated =
        fix.Union(
          fix.Unreferenced,
          free.Map(
            free.Map(
              free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("zips1")), ExcludeId),
                recFunc.ConcatMaps(
                  recFunc.MakeMapS("a", recFunc.ProjectKeyS(recFunc.Hole, "a")),
                  recFunc.MakeMapS("b", recFunc.ProjectKeyS(recFunc.Hole, "b")))),
            recFunc.Add(
              recFunc.ProjectKeyS(recFunc.Hole, "a"),
              recFunc.ProjectKeyS(recFunc.Hole, "b"))),
          free.Map(
            free.Map(
              free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("zips2")), ExcludeId),
                recFunc.ConcatMaps(
                  recFunc.MakeMapS("c", recFunc.ProjectKeyS(recFunc.Hole, "c")),
                  recFunc.MakeMapS("d", recFunc.ProjectKeyS(recFunc.Hole, "d")))),
            recFunc.Add(
              recFunc.ProjectKeyS(recFunc.Hole, "c"),
              recFunc.ProjectKeyS(recFunc.Hole, "d"))))

      val normalized =
        fix.Union(
          fix.Unreferenced,
          free.Map(
            free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("zips1")), ExcludeId),
              recFunc.Add(
                recFunc.ProjectKeyS(recFunc.Hole, "a"),
                recFunc.ProjectKeyS(recFunc.Hole, "b"))),
          free.Map(
            free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("zips2")), ExcludeId),
              recFunc.Add(
                recFunc.ProjectKeyS(recFunc.Hole, "c"),
                recFunc.ProjectKeyS(recFunc.Hole, "d"))))

      normalizeExpr(educated) must equal(normalized)
    }

    "elide no-op Map" >> {

      val path = ResourcePath.leaf(rootDir </> file("foo"))

      "elide outer no-op Map" >> {
        val src =
          fix.Read[ResourcePath](path, ExcludeId)

        normalizeExpr(fix.Map(src, recFunc.Hole)) must equal(src)
      }

      "elide nested no-op Map" >> {
        val src =
          fix.Map(
            fix.Read[ResourcePath](path, ExcludeId),
            recFunc.ProjectKeyS(recFunc.Hole, "bar"))

        val qs =
          fix.Filter(
            fix.Map(src, recFunc.Hole),
            recFunc.ProjectKeyS(recFunc.Hole, "baz"))

        val expected =
          fix.Filter(
            src,
            recFunc.ProjectKeyS(recFunc.Hole, "baz"))

        normalizeExpr(qs) must equal(expected)
      }

      "elide double no-op Map" >> {
        val src =
          fix.Read[ResourcePath](path, ExcludeId)

        normalizeExpr(fix.Map(fix.Map(src, recFunc.Hole), recFunc.Hole)) must equal(src)
      }
    }
  }
}
