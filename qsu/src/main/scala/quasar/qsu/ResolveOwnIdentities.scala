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

import slamdata.Predef.{Boolean, Option, Symbol}

import quasar.contrib.matryoshka._
import quasar.ejson.EJson
import quasar.fp._
import quasar.contrib.iota._
import quasar.qscript.{construction, Hole, IdOnly, IdStatus, IncludeId}
import quasar.qsu.{QScriptUniform => QSU}, QSU.ShiftTarget
import QSUGraph.Extractors._

import matryoshka.{Hole => _, _}
import monocle.syntax.fields._1
import scalaz.syntax.equal._
import scalaz.syntax.foldable._
import scalaz.syntax.functor._

final class ResolveOwnIdentities[T[_[_]]: BirecursiveT: EqualT] private () extends QSUTTypes[T] {

  val func = construction.Func[T]

  object ShiftId {
    def unapply(qa: QAccess[Hole]): Option[Symbol] =
      Access.identityHole[T[EJson]]
        .composeLens(_1)
        .composePrism(IdAccess.identity[T[EJson]])
        .getOption(qa)
  }

  def accessesShiftIdOf(node: Symbol): ShiftTarget[T] => Boolean = {
    case ShiftTarget.AccessLeftTarget(ShiftId(sym)) => sym === node
    case _ => false
  }

  def apply(qgraph: QSUGraph): QSUGraph =
    qgraph rewrite {
      case qg @ LeftShift(source, struct, idStatus, onUndefined, repair, rotation)
          if repair.any(accessesShiftIdOf(qg.root)) =>
        val newRepair = repair flatMap {
          case ShiftTarget.AccessLeftTarget(ShiftId(sym)) if sym === qg.root =>
            if (idStatus === IdOnly)
              RightTarget[T]
            else
              func.ProjectIndexI(RightTarget[T], 0)

          case ShiftTarget.RightTarget() =>
            if (idStatus === IdOnly)
              RightTarget[T]
            else
              func.ProjectIndexI(RightTarget[T], 1)

          case other => func.Hole as other
        }

        val newIdStatus: IdStatus =
          if (idStatus === IdOnly) IdOnly else IncludeId

        qg.overwriteAtRoot(QSU.LeftShift(source.root, struct, newIdStatus, onUndefined, newRepair, rotation))
    }
}

object ResolveOwnIdentities {
  def apply[T[_[_]]: BirecursiveT: EqualT](qgraph: QSUGraph[T]): QSUGraph[T] =
    new ResolveOwnIdentities[T].apply(qgraph)
}
