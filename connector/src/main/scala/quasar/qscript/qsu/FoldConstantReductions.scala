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

package quasar.qscript.qsu

import slamdata.Predef._
import ApplyProvenance.AuthenticatedQSU
import quasar._
// import quasar.contrib.matryoshka._
import quasar.ejson.EJson
import quasar.fp._
import quasar.qscript._
import ReduceFuncs.{Arbitrary, First, Last}

import matryoshka.{Hole => _, _}
import matryoshka.data.free._
import matryoshka.implicits._
import scalaz._, Scalaz._

final class FoldConstantReductions[T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT] private () extends QSUTTypes[T] {
  import QSUGraph.Extractors
  private val func = qscript.construction.Func[T]
  private val json = ejson.Fixed[T[EJson]]

  def apply(aqsu: AuthenticatedQSU[T])
      : AuthenticatedQSU[T] = {
    val graph = aqsu.graph.rewrite(Function.unlift(extract))
    ApplyProvenance.AuthenticatedQSU(graph, aqsu.auth)
  }

  private def extract
      : QSUGraph => Option[QSUGraph] = {
    case qsr@Extractors.LPReduce(m@Extractors.Map(_, fm), Arbitrary(_) | First(_) | Last(_)) =>
      val normalizedFM = fm.transCata[FreeMap](MapFuncCore.normalize[T, Hole])
      normalizedFM.project.run match {
        case \/-(MFC(MapFuncsCore.Constant(_))) =>
          Some(qsr.overwriteAtRoot(m.unfold.map(_.root)))
        case _ =>
          None
      }
    case _ =>
      None
  }

}

object FoldConstantReductions {
  def apply[
      T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT]
      (aqsu: AuthenticatedQSU[T])
      : AuthenticatedQSU[T] =
    new FoldConstantReductions[T].apply(aqsu)
}