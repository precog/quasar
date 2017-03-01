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

import quasar.Predef._
import quasar.{Planner => QPlanner, RenderTreeT}
import quasar.common._
import quasar.contrib.pathy._
import quasar.contrib.scalaz.{toMonadError_Ops => _, _}
import quasar.effect.{Kvs, MonoSeq}
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

  type MLQScript[T[_[_]], A] = (
    QScriptCore[T, ?]           :\:
    ThetaJoin[T, ?]             :\:
    Const[ShiftedRead[ADir], ?] :/:
    Const[Read[AFile], ?]
  )#M[A]

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
        xqy <- lpToXQuery[QG, Fix, FMT](lp)
        mm  <- saveResults(xqy)
        _   <- liftQG(deleteOutIfExists)
        _   <- liftQG(Xcc[G].execute(mm))
      } yield out

      resultFile.run.run
    }

    def eval(lp: Fix[LogicalPlan]) =
      lpToXQuery[QG, Fix, FMT](lp).map(main =>
        Xcc[F].evaluate(main)
          .chunk(chunkSize.get.toInt)
          .map(_ traverse xdmitem.decodeForFileSystem)
      ).run.run

    def explain(lp: Fix[LogicalPlan]) =
      lpToXQuery[QG, Fix, FMT](lp)
        .map(main => ExecutionPlan(FsType, main.render))
        .run.run

    def listContents(dir: ADir) =
      ops.pathHavingFormatExists[G, FMT](dir).ifM(
        ops.directoryContents[G, FMT](dir).map(_.right[FileSystemError]),
        pathErr(pathNotFound(dir)).left[Set[PathSegment]].point[G])

    queryFileFromProcess[F, G](fToG, exec, eval, explain, listContents, ops.pathHavingFormatExists[G, FMT])
  }

  def lpToXQuery[
    F[_]   : Monad: MonadFsErr: PhaseResultTell: PrologL: Xcc,
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    FMT: SearchOptions
  ](
    lp: T[LogicalPlan]
  )(implicit
    planner: Planner[F, FMT, MLQScript[T, ?]]
  ): F[MainModule] = {
    type MLQ[A]  = MLQScript[T, A]
    type QSR[A]  = QScriptRead[T, A]

    val C = Coalesce[T, MLQ, MLQ]
    val N = Normalizable[MLQ]
    val R = new Rewrite[T]

    def logPhase(pr: PhaseResult): F[Unit] =
      MonadTell_[F, PhaseResults].tell(Vector(pr))

    def plan(qs: T[MLQ]): F[MainModule] =
      MainModule.fromWritten(qs.cataM(planner.plan) strengthL Version.`1.0-ml`)

    for {
      qs      <- convertToQScriptRead[T, F, QSR](ops.directoryContents[F, FMT])(lp)
      shifted =  R.shiftReadDir[QSR, MLQ].apply(qs)
      _       <- logPhase(PhaseResult.tree("QScript (ShiftRead)", shifted))
      optmzed =  shifted.transHylo(
                   R.optimize(reflNT[MLQ]),
                   repeatedly(R.applyTransforms(
                     C.coalesceQC[MLQ](idPrism),
                     C.coalesceTJ[MLQ](idPrism.get),
                     C.coalesceSR[MLQ, ADir](idPrism),
                     N.normalizeF(_: MLQ[T[MLQ]]))))
      _       <- logPhase(PhaseResult.tree("QScript (Optimized)", optmzed))
      main    <- plan(optmzed)
      pp      <- prettyPrint[F](main.queryBody)
      xqyLog  =  MainModule.queryBody.modify(pp getOrElse _)(main).render
      _       <- logPhase(PhaseResult.detail("XQuery", xqyLog))
      // NB: While it would be nice to use the pretty printed body in the module
      //     returned for nicer error messages, we cannot as xdmp:pretty-print has
      //     a bug that reorders `where` and `order by` clauses in FLWOR expressions,
      //     causing them to be malformed.
    } yield main
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
