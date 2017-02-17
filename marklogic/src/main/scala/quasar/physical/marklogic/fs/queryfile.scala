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
import quasar.contrib.scalaz._
import quasar.effect.{Capture, Kvs, MonoSeq}
import quasar.fp._, eitherT._
import quasar.fp.numeric.Positive
import quasar.fp.ski.κ
import quasar.fs._
import quasar.fs.impl.queryFileFromDataCursor
import quasar.frontend.logicalplan.LogicalPlan
import quasar.physical.marklogic.qscript._
import quasar.physical.marklogic.xcc._
import quasar.physical.marklogic.xml.NCName
import quasar.physical.marklogic.xquery._, expr._
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
  import FunctionDecl.{FunctionDecl1, FunctionDecl3}

  type PrologL[F[_]]         = MonadListen_[F, Prologs]
  type MLQScript[T[_[_]], A] = QScriptShiftRead[T, AFile, A]

  implicit def mlQScriptToQScriptTotal[T[_[_]]]
      : Injectable.Aux[MLQScript[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::/::[T, ThetaJoin[T, ?], Const[ShiftedRead[AFile], ?]])

  def interpret[F[_]: Monad: Capture: CSourceReader: SessionReader: XccErr: MonoSeq: PrologW: PrologL, FMT](
    resultsChunkSize: Positive
  )(implicit
    K : Kvs[F, QueryFile.ResultHandle, ResultCursor],
    P : Planner[F, FMT, MLQScript[Fix, ?]],
    SP: StructuralPlanner[F, FMT]
  ): QueryFile ~> F = {
    type QF[A] = FileSystemErrT[PhaseResultT[F, ?], A]

    def liftQF[A](fa: F[A]): QF[A] =
      fa.liftM[PhaseResultT].liftM[FileSystemErrT]

    def exec(lp: Fix[LogicalPlan], out: AFile) = {
      import MainModule._

      def saveResults(mm: MainModule): QF[MainModule] =
        MonadListen_[QF, Prologs].listen(saveTo[QF, FMT](out, mm.queryBody)) map {
          case (body, plogs) =>
            (prologs.modify(_ union plogs) >>> queryBody.set(body))(mm)
        }

      val resultFile = for {
        xqy <- lpToXQuery[QF, Fix, FMT](lp)
        mm  <- saveResults(xqy)
        _   <- liftQF(ops.deleteFile[F](out))
        _   <- liftQF(session.executeModule_[F](mm))
      } yield out

      resultFile.run.run
    }

    def eval(lp: Fix[LogicalPlan]) =
      lpToXQuery[QF, Fix, FMT](lp).flatMap(main => liftQF(
        contentsource.resultCursor[F](resultsChunkSize) { s =>
          SessionReader[F].scope(s)(session.evaluateModule_[F](main))
        }
      )).run.run

    def explain(lp: Fix[LogicalPlan]) =
      lpToXQuery[QF, Fix, FMT](lp)
        .map(main => ExecutionPlan(FsType, main.render))
        .run.run

    def listContents(dir: ADir): F[FileSystemError \/ Set[PathSegment]] =
      ops.exists[F](dir).ifM(
        ops.ls[F](dir).map(_.right[FileSystemError]),
        pathErr(pathNotFound(dir)).left[Set[PathSegment]].point[F])

    queryFileFromDataCursor[ResultCursor, F](exec, eval, explain, listContents, ops.exists[F])
  }

  def lpToXQuery[
    F[_]   : Monad: MonadFsErr: PhaseResultTell: PrologL: Capture: SessionReader: XccErr,
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    FMT
  ](
    lp: T[LogicalPlan]
  )(implicit
    planner: Planner[F, FMT, MLQScript[T, ?]]
  ): F[MainModule] = {
    type MLQ[A]  = MLQScript[T, A]
    type QSR[A]  = QScriptRead[T, APath, A]
    type QSSR[A] = QScriptShiftRead[T, APath, A]

    val C = Coalesce[T, MLQ, MLQ]
    val N = Normalizable[MLQ]
    val R = new Rewrite[T]

    def logPhase(pr: PhaseResult): F[Unit] =
      MonadTell_[F, PhaseResults].tell(Vector(pr))

    def plan(qs: T[MLQ]): F[MainModule] =
      MonadListen_[F, Prologs].listen(qs.cataM(planner.plan)) map {
        case (xqy, prologs) => MainModule(Version.`1.0-ml`, prologs, xqy)
      }

    for {
      qs      <- convertToQScriptRead[T, F, QSR](ops.ls[F])(lp)
      shifted <- R.shiftRead[QSR, QSSR].apply(qs)
                   // TODO: Eliminate this once the filesystem implementation is updated
                   .transCataM(ExpandDirs[T, QSSR, MLQ].expandDirs(idPrism.reverseGet, ops.ls[F]))
      _       <- logPhase(PhaseResult.tree("QScript (ShiftRead)", shifted))
      optmzed =  shifted.transHylo(
                   R.optimize(reflNT[MLQ]),
                   repeatedly(R.applyTransforms(
                     C.coalesceQC[MLQ](idPrism),
                     C.coalesceTJ[MLQ](idPrism.get),
                     C.coalesceSR[MLQ, AFile](idPrism),
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

  def prettyPrint[F[_]: Monad: MonadFsErr: Capture: SessionReader](xqy: XQuery): F[Option[XQuery]] =
    session.resultsOf_[EitherT[F, XccError, ?]](xdmp.prettyPrint(XQuery(s"'$xqy'"))).run flatMap {
      case -\/(err @ XccError.XQueryError(_, cause)) =>
        MonadFsErr[F].raiseError(
          FileSystemError.qscriptPlanningFailed(QPlanner.InternalError(
            (err: XccError).shows, Some(cause))))

      // NB: As this is only for pretty printing, if we fail for some other reason
      //     just return an empty result.
      case -\/(_)  => none[XQuery].point[F]
      case \/-(xs) => (xs.headOption collect { case s: XSString => XQuery(s.asString) }).point[F]
    }

  ////

  private val lpadToLength: FunctionDecl3 =
    declareLocal(NCName("lpad-to-length"))(
      $("padchar") as SequenceType("xs:string"),
      $("length")  as SequenceType("xs:integer"),
      $("str")     as SequenceType("xs:string")
    ).as(SequenceType("xs:string")) { (padchar, length, str) =>
      val (slen, padct, prefix) = ($("slen"), $("padct"), $("prefix"))
      let_(
        slen   := fn.stringLength(str),
        padct  := fn.max(mkSeq_("0".xqy, length - (~slen))),
        prefix := fn.stringJoin(for_($("_") in (1.xqy to ~padct)) return_ padchar, "".xs))
      .return_(
        fn.concat(~prefix, str))
    }

  private def wrapUnlessNode[F[_]: Monad, T](implicit SP: StructuralPlanner[F, T]): F[FunctionDecl1] =
    declareLocal(NCName("wrap-unless-node"))(
      $("item") as SequenceType("item()")
    ).as(SequenceType("node()")) { item: XQuery =>
      SP.singletonObject("value".xs, item) map { obj =>
        typeswitch(item)(
          SequenceType("node()") return_ item
        ) default obj
      }
    }

  private def saveTo[F[_]: Monad: QNameGenerator: PrologW, T](
    dst: AFile, results: XQuery
  )(implicit SP: StructuralPlanner[F, T]): F[XQuery] = {
    val dstDirUri = pathUri(asDir(dst))

    for {
      ts     <- freshName[F]
      i      <- freshName[F]
      result <- freshName[F]
      fname  <- freshName[F]
      now    <- lib.secondsSinceEpoch[F].apply(fn.currentDateTime)
      dpart  <- lpadToLength[F]("0".xs, 8.xqy, xdmp.integerToHex(~i))
      node   <- wrapUnlessNode[F, T].apply(~result)
    } yield {
      let_(
        ts     := xdmp.integerToHex(xs.integer(now * 1000.xqy)),
        $("_") := try_(xdmp.directoryCreate(dstDirUri.xs))
                  .catch_($("e"))(κ(emptySeq)))
      .return_ {
        for_(
          result at i in mkSeq_(results))
        .let_(
          fname := fn.concat(dstDirUri.xs, ~ts, dpart))
        .return_(
          xdmp.documentInsert(~fname, node))
      }
    }
  }
}
