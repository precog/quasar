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

package quasar.connector

import slamdata.Predef._
import quasar.Data
import quasar.Data._int
import quasar.contrib.pathy._
import quasar.frontend.logicalplan.{Read => LPRead, LogicalPlan}
import quasar.fs._
import quasar.fs.PathError.invalidPath
import quasar.qscript._, analysis._
import quasar.std.StdLib.agg

import pathy.Path.{file, peel, file1}
import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import scalaz._, Scalaz._

trait DefaultAnalyzeModule { self: BackendModule =>

  private final implicit def _CardinalityQSM: Cardinality[QSM[Fix, ?]] = CardinalityQSM
  private final implicit def _CostQSM: Cost[QSM[Fix, ?]] = CostQSM
  private final implicit def _TraverseQSM[T[_[_]]] = TraverseQSM[T]
  private final implicit def _MonadM = MonadM

  def CardinalityQSM: Cardinality[QSM[Fix, ?]]
  def CostQSM: Cost[QSM[Fix, ?]]
  def TraverseQSM[T[_[_]]]: Traverse[QSM[T, ?]]

  object AnalyzeModule extends AnalyzeModule {

    private def pathCard: APath => Backend[Int] = (apath: APath) => {
      val afile: Option[AFile] = peel(apath).map {
        case (dirPath, \/-(fileName)) => dirPath </> file1(fileName)
        case (dirPath, -\/(dirName))  => dirPath </> file(dirName.value)
      }

      def lpFromPath[LP](p: FPath)(implicit LP: Corecursive.Aux[LP, LogicalPlan]): LP =
        LP.embed(agg.Count(LP.embed(LPRead(p))))

      def first(plan: Fix[LogicalPlan]): Backend[Option[Data]] = for {
        pp <- lpToRepr[Fix](plan)
        h  <- QueryFileModule.evaluatePlan(pp.repr)
        vs <- QueryFileModule.more(h)
        _  <- QueryFileModule.close(h).liftB
      } yield vs.headOption

      val lp: Backend[Fix[LogicalPlan]] =
        EitherT.fromDisjunction(afile.map(p => lpFromPath[Fix[LogicalPlan]](p)) \/> FileSystemError.pathErr(invalidPath(apath, "Cardinality unsupported")))

      (lp >>= (first _)) map (_.flatMap(d => _int.getOption(d).map(_.toInt)) | 0)
    }

    def queryCost(qs: Fix[QSM[Fix, ?]]): Backend[Int] =
      qs.zygoM(CardinalityQSM.calculate[Backend](pathCard), CostQSM.evaluate[Backend](pathCard))
  }

}
