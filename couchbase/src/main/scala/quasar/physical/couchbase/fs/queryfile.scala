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

package quasar.physical.couchbase.fs

import slamdata.Predef._
import quasar.{Data, DataCodec, RenderTreeT}
import quasar.common.{PhaseResults, PhaseResultT}
import quasar.common.PhaseResult.{detail, tree}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.effect.{KeyValueStore, Read, MonotonicSeq}
import quasar.effect.uuid.GenUUID
import quasar.fp._
import quasar.fp.free._
import quasar.fp.ski._
import quasar.fs._
import quasar.frontend.logicalplan.LogicalPlan
import quasar.physical.couchbase._, common._, planner._, Planner._
import quasar.Planner.PlannerError
import quasar.qscript.{Read => _, _}
import quasar.RenderTree.ops._

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.transcoder.JsonTranscoder
import matryoshka._
import matryoshka.implicits._
import pathy.Path._
import rx.lang.scala._, JavaConverters._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object queryfile {
  import QueryFile._

  type Plan[S[_], A] = FileSystemErrT[PhaseResultT[Free[S, ?], ?], A]

  implicit class LiftF[F[_]: Monad, A](p: F[A]) {
    val liftF = p.liftM[PhaseResultT].liftM[FileSystemErrT]
  }

  implicit class LiftFE[A](d: FileSystemError \/ A) {
    def liftFE[S[_]]: Plan[S, A] = EitherT(d.η[Free[S, ?]].liftM[PhaseResultT])
  }

  implicit class liftPE[A](d: PlannerError \/ A) {
    def liftPE[S[_]]: Plan[S, A] = d.leftMap(FileSystemError.qscriptPlanningFailed(_)).liftFE
  }

  implicit val codec: DataCodec = CBDataCodec

  val jsonTranscoder = new JsonTranscoder

  // TODO: Streaming
  def interpret[S[_]](
    implicit
    S0: Read[ClientContext, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: KeyValueStore[ResultHandle, Cursor, ?] :<: S,
    S3: GenUUID :<: S,
    S4: Task :<: S
  ): QueryFile ~> Free[S, ?] = λ[QueryFile ~> Free[S, ?]] {
    case ExecutePlan(lp, out) => executePlan(lp, out)
    case EvaluatePlan(lp)     => evaluatePlan(lp)
    case More(h)              => more(h)
    case Close(h)             => close(h)
    case Explain(lp)          => explain(lp)
    case ListContents(dir)    => listContents(dir)
    case FileExists(file)     => fileExists(file)
  }

  def executePlan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, S[_]](
    lp: T[LogicalPlan], out: AFile
  )(implicit
    S0: Read[ClientContext, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: GenUUID :<: S,
    S3: Task :<: S
  ): Free[S, (PhaseResults, FileSystemError \/ AFile)] =
    (for {
      n1ql   <- lpToN1ql[T, S](lp) map (_._1)
      r      <- n1qlResults(n1ql)
      col    <- docTypeValueFromPath(out).η[Plan[S, ?]]
      ctx    <- Read.Ops[ClientContext, S].ask.liftF
      docs   <- r.map(DataCodec.render).unite.traverse(d => GenUUID.Ops[S].asks(uuid =>
                  JsonDocument.create(
                    uuid.toString,
                    JsonObject
                      .create()
                      .put(ctx.docTypeKey.v, col.v)
                      .put("value", jsonTranscoder.stringToJsonObject(d)))
                )).liftF
      exists <- EitherT(lift(existsWithPrefix(ctx, col.v)).into.liftM[PhaseResultT])
      _      <- exists.whenM(EitherT(lift(deleteHavingPrefix(ctx, col.v)).into[S].liftM[PhaseResultT]))
      _      <- lift(docs.nonEmpty.whenM(Task.delay(
                  Observable
                    .from(docs)
                    .flatMap(ctx.bucket.async.insert(_).asScala)
                    .toBlocking
                    .last
                ))).into.liftF
    } yield out).run.run

  def evaluatePlan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, S[_]](
    lp: T[LogicalPlan]
  )(implicit
    S0: Read[ClientContext, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: Task :<: S,
    results: KeyValueStore.Ops[ResultHandle, Cursor, S]
  ): Free[S, (PhaseResults, FileSystemError \/ ResultHandle)] =
    (for {
      n1ql <- lpToN1ql[T, S](lp) map (_._1)
      r    <- n1qlResults(n1ql)
      i    <- MonotonicSeq.Ops[S].next.liftF
      h    =  ResultHandle(i)
      _    <- results.put(h, Cursor(r)).liftF
    } yield h).run.run

  def more[S[_]](
    handle: ResultHandle
  )(implicit
    results: KeyValueStore.Ops[ResultHandle, Cursor, S]
  ): Free[S, FileSystemError \/ Vector[Data]] =
    (for {
      h <- results.get(handle).toRight(FileSystemError.unknownResultHandle(handle))
      r <- resultsFromCursor(h).η[FileSystemErrT[Free[S, ?], ?]]
      _ <- results.put(handle, r._1).liftM[FileSystemErrT]
    } yield r._2).run

  def close[S[_]](
    handle: ResultHandle
  )(implicit
    results: KeyValueStore.Ops[ResultHandle, Cursor, S]
  ): Free[S, Unit] =
    results.delete(handle)

  def explain[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, S[_]](
    lp: T[LogicalPlan]
  )(implicit
    S0: Read[ClientContext, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: Task :<: S
  ): Free[S, (PhaseResults, FileSystemError \/ ExecutionPlan)] =
    lpToN1ql[T, S](lp)
      .flatMap(_.bitraverse(RenderQuery.compact(_).liftPE, _.point[Plan[S, ?]]))
      .map({ case (ep, ipt) => ExecutionPlan(FsType, ep, ipt) })
      .run.run

  def listContents[S[_]](
    dir: APath
  )(implicit
    S0: Task :<: S,
    context: Read.Ops[ClientContext, S]
  ): Free[S, FileSystemError \/ Set[PathSegment]] =
    (for {
      ctx    <- context.ask.liftM[FileSystemErrT]
      col    <- docTypeValueFromPath(dir).η[FileSystemErrT[Free[S, ?], ?]]
      types  <- EitherT(lift(docTypeValuesFromPrefix(ctx, col.v)).into)
      _      <- EitherT((
                  if (types.isEmpty) FileSystemError.pathErr(PathError.pathNotFound(dir)).left
                  else ().right
                ).η[Free[S, ?]])
    } yield pathSegmentsFromPrefixDocTypeValues(col.v, types)).run

  def fileExists[S[_]](
    file: AFile
  )(implicit
    S0: Task :<: S,
    context: Read.Ops[ClientContext, S]
  ): Free[S, Boolean] =
    (for {
      ctx    <- context.ask.liftM[FileSystemErrT]
      col    <- docTypeValueFromPath(file).η[FileSystemErrT[Free[S, ?], ?]]
      exists <- EitherT(lift(existsWithPrefix(ctx, col.v)).into)
    } yield exists).exists(ι)

  def n1qlResults[T[_[_]]: BirecursiveT, S[_]](
    n1ql: T[N1QL]
  )(implicit
    S0: Task :<: S,
    context: Read.Ops[ClientContext, S]
  ): Plan[S, Vector[Data]] =
    for {
      ctx     <- context.ask.liftF
      bkt     <- lift(Task.delay(
                   ctx.bucket
                 )).into.liftF
      q       <- RenderQuery.compact(n1ql).liftPE
      r       <- EitherT(lift(queryData(bkt, q)).into.liftM[PhaseResultT])
      v       =  r >>= (Data._obj.getOption(_).foldMap(_.values.toVector))
    } yield v

  def lpToN1ql[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, S[_]](
    lp: T[LogicalPlan]
  )(implicit
    S0: MonotonicSeq :<: S,
    S1: Task :<: S,
    ctx: Read.Ops[ClientContext, S]
  ): Plan[S, (T[N1QL], ISet[APath])] = {
    val lc: DiscoverPath.ListContents[Plan[S, ?]] =
      (d: ADir) => EitherT(listContents(d).liftM[PhaseResultT])

    ctx.ask.liftF >>= (c => lpLcToN1ql[T, S](lp, lc, Context(BucketName(c.bucket.name), c.docTypeKey)))
  }

  def lpLcToN1ql[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, S[_]](
    lp: T[LogicalPlan],
    lc: DiscoverPath.ListContents[Plan[S, ?]],
    ctx: Context
  )(implicit
    S0: MonotonicSeq :<: S,
    S1: Task :<: S
  ): Plan[S, (T[N1QL], ISet[APath])] = {
    type CBQSCP = QScriptCore[T, ?] :\: EquiJoin[T, ?] :/: Const[ShiftedRead[AFile], ?]
    type CBQS[A]  = CBQSCP#M[A]
    type CBQS0[A] = (Const[ShiftedRead[ADir], ?] :/: CBQS)#M[A]

    implicit val couchbaseQScriptToQSTotal: Injectable.Aux[CBQS, QScriptTotal[T, ?]] =
      ::\::[QScriptCore[T, ?]](::/::[T, EquiJoin[T, ?], Const[ShiftedRead[AFile], ?]])

    val tell = MonadTell[Plan[S, ?], PhaseResults].tell _
    val rewrite = new Rewrite[T]
    val optimize = new Optimize[T]

    for {
      qs   <- convertToQScriptRead[T, Plan[S, ?], QScriptRead[T, ?]](lc)(lp)
      _    <- tell(Vector(tree("QScript (post convertToQScriptRead)", qs)))
      shft <- Unirewrite[T, CBQSCP, Plan[S, ?]](rewrite, lc).apply(qs)
      _    <- tell(Vector(tree("QScript (post shiftRead)", shft)))
      opz  =  shft.transHylo(
                optimize.optimize(reflNT[CBQS]),
                Unicoalesce[T, CBQSCP])
      _    <- tell(Vector(tree("QScript (optimized)", opz)))
      n1ql <- EitherT(WriterT(
                opz.cataM(Planner[T, Kleisli[Free[S, ?], Context, ?], CBQS].plan)
                  .leftMap(FileSystemError.qscriptPlanningFailed(_)).run.run.run(ctx)))
      ipt  =  opz.cata(ExtractPath[CBQS, APath].extractPath[DList])
      q    <- RenderQuery.compact(n1ql).liftPE
      _    <- tell(Vector(detail("N1QL AST", n1ql.render.shows)))
      _    <- tell(Vector(detail("N1QL", q)))
    } yield (n1ql, ISet fromFoldable ipt)
  }
}
