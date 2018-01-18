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

package quasar.qscript.qsu

import matryoshka.{Hole => _, _}
import quasar.qscript.{construction, ExcludeId, IdOnly, IdStatus, IncludeId, SrcHole}
import quasar.fp._
import quasar.fp.ski._
import quasar.contrib.matryoshka._
import ApplyProvenance.AuthenticatedQSU
import quasar.qscript.qsu.{QScriptUniform => QSU}
import QSUGraph.Extractors._
import scalaz.Equal
import scalaz.syntax.equal._
import scalaz.syntax.foldable._

final class ResolveOwnIdentities[T[_[_]]: BirecursiveT: ShowT: EqualT] private () extends QSUTTypes[T] {

  val func = construction.Func[T]

  def apply(aqsu: AuthenticatedQSU[T]): AuthenticatedQSU[T] = {
    implicit val extEqual: Delay[Equal, quasar.ejson.Extension] = quasar.ejson.Extension.structuralEqual
    aqsu.copy(graph = aqsu.graph.rewrite {
      // TODO
      case qg @ LeftShift(source, struct, idStatus, onUndefined, repair, rotation)
        if repair.element(QSU.AccessLeftTarget(Access.Id(IdAccess.Identity(source.root), SrcHole))) =>
        val newRepair = repair.flatMap {
          case QSU.AccessLeftTarget(Access.Id(IdAccess.Identity(symbol), _)) if symbol == qg.root =>
            func.ProjectIndexI(func.RightTarget, 0)
          case QSU.AccessLeftTarget(access) =>
            func.AccessLeftTarget(κ(access))
          case QSU.RightTarget() =>
            if (idStatus === ExcludeId)
              func.ProjectIndexI(func.RightTarget, 1)
            else
              func.RightTarget
          case QSU.LeftTarget() =>
            scala.sys.error("QSU.LeftTarget in ResolveOwnIdentities")
        }
        val newIdStatus: IdStatus =
          if (idStatus === IdOnly) IdOnly else IncludeId
        qg.overwriteAtRoot(QSU.LeftShift(source.root, struct, newIdStatus, onUndefined, newRepair, rotation))
    })
  }
}

object ResolveOwnIdentities {
  import ApplyProvenance.AuthenticatedQSU
  def apply[T[_[_]]: BirecursiveT: ShowT: EqualT](aqsu: AuthenticatedQSU[T]): AuthenticatedQSU[T] = new ResolveOwnIdentities[T].apply(aqsu)
}
