/*
 * Copyright 2014–2016 SlamData Inc.
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
import quasar.NameGenerator
import quasar.{Data, Planner => QPlanner}
import quasar.common.{PhaseResult, PhaseResults, PhaseResultT}
import quasar.contrib.matryoshka._
import quasar.contrib.pathy._
import quasar.effect.MonotonicSeq
import quasar.fp._, eitherT._
import quasar.fp.free.lift
import quasar.fp.numeric.Positive
import quasar.fs._
import quasar.fs.impl.queryFileFromDataCursor
import quasar.logicalplan.{constant, LogicalPlan}
import quasar.physical.marklogic.qscript._
import quasar.physical.marklogic.xcc._
import quasar.physical.marklogic.xml.NCName
import quasar.physical.marklogic.xquery._, expr._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._

import eu.timepit.refined.auto._
import matryoshka._, Recursive.ops._, FunctorT.ops._
import scalaz._, Scalaz._, concurrent._

object queryfile {
  import QueryFile._
  import FileSystemError._, PathError._
  import MarkLogicPlanner._, MarkLogicPlannerError._
  import FunctionDecl.FunctionDecl3

  def interpret[S[_]](
    resultsChunkSize: Positive
  )(implicit
    S0: SessionIO :<: S,
    S1: ContentSourceIO :<: S,
    S2: Task :<: S,
    S3: MLResultHandles :<: S,
    S4: MonotonicSeq :<: S
  ): QueryFile ~> Free[S, ?] = {
    type QPlan[A]          = FileSystemErrT[PhaseResultT[Free[S, ?], ?], A]
    type PrologsT[F[_], A] = WriterT[F, Prologs, A]

    def liftQP[F[_], A](fa: F[A])(implicit ev: F :<: S): QPlan[A] =
      lift(fa).into[S].liftM[PhaseResultT].liftM[FileSystemErrT]

    def lpToXQuery(lp: Fix[LogicalPlan]): QPlan[MainModule] = {
      type MLQScript[A] = QScriptShiftRead[Fix, A]
      type MLPlan[A]    = PrologsT[MarkLogicPlanErrT[PhaseResultT[Free[S, ?], ?], ?], A]
      type QSR[A]       = QScriptRead[Fix, A]

      // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
      import WriterT.writerTMonad
      val rewrite = new Rewrite[Fix]
      val C = Coalesce[Fix, MLQScript, MLQScript]

      def logPhase(pr: PhaseResult): QPlan[Unit] =
        MonadTell[QPlan, PhaseResults].tell(Vector(pr))

      def plan(qs: Fix[MLQScript]): MarkLogicPlanErrT[PhaseResultT[Free[S, ?], ?], MainModule] =
        qs.cataM(MarkLogicPlanner[MLPlan, MLQScript].plan).run map {
          case (prologs, xqy) => MainModule(Version.`1.0-ml`, prologs, xqy)
        }

      val linearize: Algebra[MLQScript, List[MLQScript[ExternallyManaged]]] =
        qsr => qsr.as[ExternallyManaged](Extern) :: Foldable[MLQScript].fold(qsr)

      for {
        qs      <- convertToQScriptRead[Fix, QPlan, QSR](d => liftQP(ops.ls(d)))(lp)
        shifted =  shiftRead[Fix](qs)
        _       <- logPhase(PhaseResult.tree("QScript (ShiftRead)", shifted.cata(linearize).reverse))
        optmzed =  shifted
                     .transAna(
                       repeatedly(C.coalesceQC[MLQScript](idPrism)) ⋙
                       repeatedly(C.coalesceTJ[MLQScript](idPrism.get)) ⋙
                       repeatedly(C.coalesceSR[MLQScript](idPrism)) ⋙
                       repeatedly(Normalizable[MLQScript].normalizeF(_: MLQScript[Fix[MLQScript]])))
                     .transCata(rewrite.optimize(reflNT))

        _       <- logPhase(PhaseResult.tree("QScript (Optimized)", optmzed.cata(linearize).reverse))
        main    <- plan(optmzed).leftMap(mlerr => mlerr match {
                     case InvalidQName(s) =>
                       FileSystemError.planningFailed(lp, QPlanner.UnsupportedPlan(
                         // TODO: Change to include the QScript context when supported
                         constant(Data.Str(s)), Some(mlerr.shows)))

                     case UnrepresentableEJson(ejs, _) =>
                       FileSystemError.planningFailed(lp, QPlanner.NonRepresentableEJson(ejs.shows))
                   })
        _       <- logPhase(PhaseResult.detail("XQuery", main.render))
      } yield main
    }

    val lpadToLength: FunctionDecl3 =
      declareLocal(NCName("lpad-to-length"))(
        $("padchar") as SequenceType("xs:string"),
        $("length")  as SequenceType("xs:integer"),
        $("str")     as SequenceType("xs:string")
      ).as(SequenceType("xs:string")) { (padchar, length, str) =>
        val (slen, padct, prefix) = ("$slen", "$padct", "$prefix")
        let_(
          slen   -> fn.stringLength(str),
          padct  -> fn.max(mkSeq_("0".xqy, length - slen.xqy)),
          prefix -> fn.stringJoin(for_("$_" -> (1.xqy to padct.xqy)) return_ padchar, "".xs)
        ) return_ fn.concat(prefix.xqy, str)
      }

    def saveTo[F[_]: NameGenerator: PrologW](dst: AFile, results: XQuery): F[XQuery] = {
      val dstDirUri = pathUri(asDir(dst))

      for {
        ts     <- freshVar[F]
        i      <- freshVar[F]
        result <- freshVar[F]
        fname  <- freshVar[F]
        now    <- qscript.secondsSinceEpoch[F].apply(fn.currentDateTime)
        dpart  <- lpadToLength[F]("0".xs, 8.xqy, xdmp.integerToHex(i.xqy))
      } yield {
        let_(
          ts   -> xdmp.integerToHex(xs.integer(now * 1000.xqy)),
          "$_" -> xdmp.directoryCreate(dstDirUri.xs)
        ) return_ {
          for_(
            // FIXME: Need to properly add positional variables to FLWOR
            s"$result at $i" -> mkSeq_(results))
          .let_(
            fname -> fn.concat(dstDirUri.xs, ts.xqy, dpart))
          .return_(xdmp.documentInsert(fname.xqy, result.xqy))
        }
      }
    }

    def exec(lp: Fix[LogicalPlan], out: AFile) = {
      import MainModule._

      def saveResults(mm: MainModule): QPlan[MainModule] =
        saveTo[PrologsT[QPlan, ?]](out, mm.queryBody).run map {
          case (plogs, body) =>
            (prologs.modify(_ union plogs) >>> queryBody.set(body))(mm)
        }

      (lpToXQuery(lp) >>= saveResults >>= (mm => liftQP(SessionIO.executeModule_(mm))))
        .as(out).run.run
    }

    def eval(lp: Fix[LogicalPlan]) =
      lpToXQuery(lp).flatMap(main => liftQP(
        ContentSourceIO.resultCursor(
          SessionIO.evaluateModule_(main),
          resultsChunkSize)
      )).run.run

    def explain(lp: Fix[LogicalPlan]) =
      lpToXQuery(lp)
        .map(main => ExecutionPlan(FsType, main.render))
        .run.run

    def exists(file: AFile): Free[S, Boolean] =
      lift(ops.exists(file)).into[S]

    def listContents(dir: ADir): Free[S, FileSystemError \/ Set[PathSegment]] =
      lift(ops.exists(dir).ifM(
        ops.ls(dir).map(_.right[FileSystemError]),
        pathErr(pathNotFound(dir)).left[Set[PathSegment]].point[SessionIO]
      )).into[S]

    queryFileFromDataCursor[S, Task, ResultCursor](exec, eval, explain, listContents, exists)
  }
}
