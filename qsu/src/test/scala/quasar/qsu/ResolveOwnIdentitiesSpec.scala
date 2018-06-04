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

package quasar.qsu

import slamdata.Predef._

import quasar.{Qspec, TreeMatchers}
import quasar.contrib.pathy.AFile
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.contrib.iota.{copkEqual, copkTraverse}
import quasar.fp.ski.κ
import quasar.qscript.{construction, ExcludeId, Hole, IdOnly, IncludeId, OnUndefined, SrcHole}

import matryoshka.data.Fix
import matryoshka.data.free._
import monocle.syntax.fields._1
import pathy.Path._

object ResolveOwnIdentitiesSpec extends Qspec with QSUTTypes[Fix] with TreeMatchers {
  import QScriptUniform.{Rotation, ShiftTarget}
  import QSUGraph.Extractors._

  type J = Fix[EJson]

  val qsu = QScriptUniform.AnnotatedDsl[Fix, Symbol]
  val func = construction.Func[Fix]
  val recFunc = construction.RecFunc[Fix]

  val foo: AFile = rootDir </> file("foo")

  val resolveIds = ResolveOwnIdentities[Fix]_

  def renameAccess(qg: QSUGraph, rns: QSUGraph.Renames): QSUGraph =
    qg rewrite {
      case g @ LeftShift(src, struct, id, undef, rep, rot) =>
        val renamedRep = rep map {
          case ShiftTarget.AccessLeftTarget(access) =>
            val renamedAccess =
              Access.identityHole[J]
                .composeLens(_1)
                .composePrism(IdAccess.identity[J])
                .modify(rns)

            ShiftTarget.AccessLeftTarget[Fix](renamedAccess(access))

          case other => other
        }

        g.overwriteAtRoot(QScriptUniform.LeftShift(src.root, struct, id, undef, renamedRep, rot))
    }

  "resolving own left shift identity" should {
    val ownAccess: QAccess[Hole] =
      Access.identityHole[J](IdAccess.identity[J]('ls), SrcHole)

    val initialRepair =
      func.ConcatArrays(
        func.MakeArray(AccessLeftTarget[Fix](κ(ownAccess))),
        func.MakeArray(RightTarget[Fix]))

    "replace own identity access when IdStatus = ExcludeId" >> {
      val expectedRepair =
        func.ConcatArrays(
          func.MakeArray(func.ProjectIndexI(RightTarget[Fix], 0)),
          func.MakeArray(func.ProjectIndexI(RightTarget[Fix], 1)))

      val (remap, lshift) = QSUGraph.fromAnnotatedTree[Fix](
        qsu.leftShift('ls, (
          qsu.read('r, foo),
          recFunc.ProjectKeyS(recFunc.Hole, "bar"),
          ExcludeId,
          OnUndefined.Omit,
          initialRepair,
          Rotation.ShiftArray)) map (Some(_)))

      resolveIds(renameAccess(lshift, remap)) must beLike {
        case LeftShift(_, _, IncludeId, _, r, _) =>
          r must beTreeEqual(expectedRepair)
      }
    }

    "replace own identity access when IdStatus = IdOnly" >> {
      val expectedRepair =
        func.ConcatArrays(
          func.MakeArray(RightTarget[Fix]),
          func.MakeArray(RightTarget[Fix]))

      val (remap, lshift) = QSUGraph.fromAnnotatedTree[Fix](
        qsu.leftShift('ls, (
          qsu.read('r, foo),
          recFunc.ProjectKeyS(recFunc.Hole, "bar"),
          IdOnly,
          OnUndefined.Omit,
          initialRepair,
          Rotation.ShiftArray)) map (Some(_)))

      resolveIds(renameAccess(lshift, remap)) must beLike {
        case LeftShift(_, _, IdOnly, _, r, _) =>
          r must beTreeEqual(expectedRepair)
      }
    }
  }
}
