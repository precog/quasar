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

package quasar.physical.marklogic.fs

import slamdata.Predef._
import quasar.{Planner => QPlanner, RenderTreeT}
import quasar.common._
import quasar.contrib.matryoshka._
import quasar.contrib.pathy._
import quasar.contrib.scalaz.{toMonadError_Ops => _, _}
import quasar.effect.{Kvs, MonoSeq}
import quasar.ejson.implicits._
import quasar.fp._, eitherT._
import quasar.fp.numeric.Positive
import quasar.fs._
import quasar.fs.impl.queryFileFromProcess
import quasar.frontend.logicalplan.LogicalPlan
import quasar.physical.marklogic.{qscript => mlqscript}, mlqscript._
import quasar.physical.marklogic.xcc._, Xcc.ops._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._

import com.marklogic.xcc.types.XSString
import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import scalaz._, Scalaz._

object queryfile {
  import QueryFile._
  import FileSystemError._, PathError._

  type QKvs[F[_], G[_]] = Kvs[G, QueryFile.ResultHandle, impl.DataStream[F]]

  type MLQScriptCP[T[_[_]]] = (
    QScriptCore[T, ?]           :\:
    ThetaJoin[T, ?]             :\:
    Const[ShiftedRead[ADir], ?] :/:
    Const[Read[AFile], ?]
  )

  type MLQScript[T[_[_]], A] = MLQScriptCP[T]#M[A]

  implicit def mlQScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[MLQScript[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::\::[ThetaJoin[T, ?]](::/::[T, Const[ShiftedRead[ADir], ?], Const[Read[AFile], ?]]))

  def interpret[
    F[_]: Monad: Catchable: Xcc,
    G[_]: Monad: Xcc: PrologW: PrologL: MonoSeq: QKvs[F, ?[_]],
    FMT: SearchOptions
  ](
    chunkSize: Positive, fToG: F ~> G
  )(implicit
    P : Planner[G, FMT, MLQScript[Fix, ?]],
    SP: StructuralPlanner[G, FMT]
  ): QueryFile ~> G = {
    type QG[A] = FileSystemErrT[PhaseResultT[G, ?], A]

    def liftQG[A](ga: G[A]): QG[A] =
      ga.liftM[PhaseResultT].liftM[FileSystemErrT]

    def exec(lp: Fix[LogicalPlan], out: AFile) = {
      import MainModule._

      def saveResults(mm: MainModule): QG[MainModule] =
        MonadListen_[QG, Prologs].listen(saveTo[QG, FMT](out, mm.queryBody)) map {
          case (body, plogs) =>
            (prologs.modify(_ union plogs) >>> queryBody.set(body))(mm)
        }

      val deleteOutIfExists =
        ops.pathHavingFormatExists[G, FMT](out)
          .flatMap(_.whenM(ops.deleteFile[G](out)))
          .transact

      val resultFile = for {
        xqy <- lpToXQuery[QG, Fix, FMT](lp) map (_._1)
        mm  <- saveResults(xqy)
        _   <- liftQG(deleteOutIfExists)
        _   <- liftQG(Xcc[G].execute(mm))
      } yield out

      resultFile.run.run
    }

    def eval(lp: Fix[LogicalPlan]) =
      lpToXQuery[QG, Fix, FMT](lp).map({ case (main, _) =>
        Xcc[F].evaluate(main)
          .chunk(chunkSize.value.toInt)
          .map(_ traverse xdmitem.decodeForFileSystem)
      }).run.run

    def explain(lp: Fix[LogicalPlan]) =
      lpToXQuery[QG, Fix, FMT](lp)
        .map({ case (main, ipt) => ExecutionPlan(FsType, main.render, ipt) })
        .run.run

    def listContents(dir: ADir) =
      ops.pathHavingFormatExists[G, FMT](dir).ifM(
        ops.directoryContents[G, FMT](dir).map(_.right[FileSystemError]),
        pathErr(pathNotFound(dir)).left[Set[PathSegment]].point[G])

    queryFileFromProcess[F, G](fToG, exec, eval, explain, listContents, ops.pathHavingFormatExists[G, FMT])
  }

  private def logPhase[F[_]](pr: PhaseResult)(implicit
    PRT: PhaseResultTell[F]
  ): F[Unit] = PRT.tell(Vector(pr))

  def lpToQScript[
    F[_]   : Monad: MonadFsErr: PhaseResultTell: Xcc,
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    FMT: SearchOptions
  ](lp: T[LogicalPlan]): F[T[MLQScript[T, ?]]] = {
    type MLQ[A]  = MLQScript[T, A]
    type QSR[A]  = QScriptRead[T, A]
    val O = new Optimize[T]
    val R = new Rewrite[T]

    for {
      qs        <- convertToQScriptRead[T, F, QSR](ops.directoryContents[F, FMT])(lp)
      shifted   <- Unirewrite[T, MLQScriptCP[T], F](R, ops.directoryContents[F, FMT]).apply(qs)
      _         <- logPhase(PhaseResult.tree("QScript (ShiftRead)", shifted))
      optimized =  shifted.transHylo(
        O.optimize(reflNT[MLQ]),
        Unicoalesce[T, MLQScriptCP[T]])
      _         <- logPhase(PhaseResult.tree("QScript (Optimized)", optimized))
    } yield optimized
  }

  def lpToXQuery[
    F[_]   : Monad: MonadFsErr: PhaseResultTell: PrologL: Xcc,
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    FMT: SearchOptions
  ](
    lp: T[LogicalPlan]
  )(implicit
    planner: Planner[F, FMT, MLQScript[T, ?]]
  ): F[(MainModule, ISet[APath])] = {

    type MLQ[A]  = MLQScript[T, A]

    def plan(qs: T[MLQ]): F[MainModule] =
      MainModule.fromWritten(qs.cataM(planner.plan) strengthL Version.`1.0-ml`)

    for {
      optimized <- lpToQScript[F, T, FMT](lp)
      main      <- plan(optimized)
      inputs    =  optimized.cata(ExtractPath[MLQ, APath].extractPath[DList])
      pp        <- prettyPrint[F](main.queryBody)
      xqyLog    =  MainModule.queryBody.modify(pp getOrElse _)(main).render
      _         <- logPhase(PhaseResult.detail("XQuery", xqyLog))
      // NB: While it would be nice to use the pretty printed body in the module
      //     returned for nicer error messages, we cannot as xdmp:pretty-print has
      //     a bug that reorders `where` and `order by` clauses in FLWOR expressions,
      //     causing them to be malformed.
    } yield (main, ISet fromFoldable inputs)
  }

  def prettyPrint[F[_]: Monad: MonadFsErr: Xcc](xqy: XQuery): F[Option[XQuery]] = {
    val prettyPrinted =
      Xcc[F].queryResults(xdmp.prettyPrint(XQuery(s"'$xqy'")))
        .map(_.headOption collect { case s: XSString => XQuery(s.asString) })

    Xcc[F].handleWith(prettyPrinted) {
      case err @ XccError.QueryError(_, cause) =>
        MonadFsErr[F].raiseError(
          FileSystemError.qscriptPlanningFailed(QPlanner.InternalError(
            err.shows, Some(cause))))

      // NB: As this is only for pretty printing, if we fail for some other reason
      //     just return an empty result.
      case _ => none[XQuery].point[F]
    }
  }

  ////

  private def saveTo[F[_]: Bind: PrologW, T](
    dst: AFile, results: XQuery
  )(implicit SP: StructuralPlanner[F, T]): F[XQuery] =
    SP.seqToArray(results) map (xdmp.documentInsert(pathUri(dst).xs, _))
}
