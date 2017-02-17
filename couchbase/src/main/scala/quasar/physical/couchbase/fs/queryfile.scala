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

import quasar.Predef._
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

import scala.collection.JavaConverters._

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

  implicit val codec = CBDataCodec

  val jsonTranscoder = new JsonTranscoder

  // TODO: Streaming
  def interpret[S[_]](
    implicit
    S0: Read[Context, ?] :<: S,
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
    S0: Read[Context, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: GenUUID :<: S,
    S3: Task :<: S
  ): Free[S, (PhaseResults, FileSystemError \/ AFile)] =
    (for {
      n1ql   <- lpToN1ql[T, S](lp)
      r      <- n1qlResults(n1ql)
      bktCol <- bucketCollectionFromPath(out).liftFE
      docs   <- r.map(DataCodec.render).unite.traverse(d => GenUUID.Ops[S].asks(uuid =>
                  JsonDocument.create(
                    uuid.toString,
                    JsonObject
                      .create()
                      .put("type", bktCol.collection)
                      .put("value", jsonTranscoder.stringToJsonObject(d)))
                )).liftF
      ctx    <- Read.Ops[Context, S].ask.liftF
      bkt    <- lift(Task.delay(
                  ctx.cluster.openBucket(bktCol.bucket)
                )).into.liftF
      exists <- lift(existsWithPrefix(bkt, bktCol.collection)).into[S].liftF
      _      <- lift(exists.whenM(deleteHavingPrefix(bkt, bktCol.collection))).into[S].liftF
      _      <- lift(docs.nonEmpty.whenM(Task.delay(
                  Observable
                    .from(docs)
                    .flatMap(bkt.async.insert(_).asScala)
                    .toBlocking
                    .last
                ))).into.liftF
    } yield out).run.run

  def evaluatePlan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, S[_]](
    lp: T[LogicalPlan]
  )(implicit
    S0: Read[Context, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: Task :<: S,
    results: KeyValueStore.Ops[ResultHandle, Cursor, S]
  ): Free[S, (PhaseResults, FileSystemError \/ ResultHandle)] =
    (for {
      n1ql <- lpToN1ql[T, S](lp)
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
    S0: Read[Context, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: Task :<: S
  ): Free[S, (PhaseResults, FileSystemError \/ ExecutionPlan)] =
    ((lpToN1ql[T, S](lp) >>= (RenderQuery.compact(_).liftPE)) ∘ (ExecutionPlan(FsType, _))).run.run

  def listContents[S[_]](
    dir: APath
  )(implicit
    S0: Task :<: S,
    context: Read.Ops[Context, S]
  ): Free[S, FileSystemError \/ Set[PathSegment]] =
    if (dir === rootDir)
      listRootContents
    else
      listNonRootContents(dir)

  def fileExists[S[_]](
    file: AFile
  )(implicit
    S0: Task :<: S,
    context: Read.Ops[Context, S]
  ): Free[S, Boolean] =
    (for {
      ctx    <- context.ask.liftM[FileSystemErrT]
      bktCol <- EitherT(bucketCollectionFromPath(file).η[Free[S, ?]])
      bkt    <- EitherT(getBucket(bktCol.bucket))
      exists <- lift(existsWithPrefix(bkt, bktCol.collection)).into.liftM[FileSystemErrT]
    } yield exists).exists(ι)

  def n1qlResults[T[_[_]]: BirecursiveT, S[_]](
    n1ql: T[N1QL]
  )(implicit
    S0: Task :<: S,
    context: Read.Ops[Context, S]
  ): Plan[S, Vector[Data]] =
    for {
      ctx     <- context.ask.liftF
      bkt     <- lift(Task.delay(
                   ctx.cluster.openBucket()
                 )).into.liftF
      q       <- RenderQuery.compact(n1ql).map(n1qlQuery).liftPE
      r       <- EitherT(lift(Task.delay(
                   bkt.query(q)
                     .allRows
                     .asScala
                     .toVector
                     .traverse(rowToData)
                 )).into.liftM[PhaseResultT])
    } yield r

  def lpToN1ql[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, S[_]](
    lp: T[LogicalPlan]
  )(implicit
    S0: Read[Context, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: Task :<: S
  ): Plan[S, T[N1QL]] = {
    val lc: DiscoverPath.ListContents[Plan[S, ?]] =
      (d: ADir) => EitherT(listContents(d).liftM[PhaseResultT])

    lpLcToN1ql[T, S](lp, lc)
  }

  def lpLcToN1ql[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, S[_]](
    lp: T[LogicalPlan],
    lc: DiscoverPath.ListContents[Plan[S, ?]]
  )(implicit
    S1: MonotonicSeq :<: S,
    S2: Task :<: S
  ): Plan[S, T[N1QL]] = {
    type CBQS0[A] = (QScriptCore[T, ?] :\: EquiJoin[T, ?] :/: Const[ShiftedRead[APath], ?])#M[A]
    type CBQS[A]  = (QScriptCore[T, ?] :\: EquiJoin[T, ?] :/: Const[ShiftedRead[AFile], ?])#M[A]

    implicit val couchbaseQScriptToQSTotal: Injectable.Aux[CBQS, QScriptTotal[T, ?]] =
      ::\::[QScriptCore[T, ?]](::/::[T, EquiJoin[T, ?], Const[ShiftedRead[AFile], ?]])

    val tell = MonadTell[Plan[S, ?], PhaseResults].tell _
    val rewrite = new Rewrite[T]
    val C = Coalesce[T, CBQS, CBQS]

    for {
      qs   <- convertToQScriptRead[T, Plan[S, ?], QScriptRead[T, APath, ?]](lc)(lp)
      _    <- tell(Vector(tree("QScript (post convertToQScriptRead)", qs)))
      shft <- rewrite.simplifyJoinOnShiftRead[QScriptRead[T, APath, ?], QScriptShiftRead[T, APath, ?], CBQS0]
                .apply(qs)
                .transCataM(ExpandDirs[T, CBQS0, CBQS].expandDirs(idPrism.reverseGet, lc))
      _    <- tell(Vector(tree("QScript (post shiftRead)", shft)))
      opz  =  shft.transHylo(
                rewrite.optimize(reflNT[CBQS]),
                repeatedly(C.coalesceQC[CBQS](idPrism))          >>>
                  repeatedly(C.coalesceEJ[CBQS](idPrism.get))    >>>
                  repeatedly(C.coalesceSR[CBQS, AFile](idPrism)) >>>
                  repeatedly(Normalizable[CBQS].normalizeF(_: CBQS[T[CBQS]])))
      _    <- tell(Vector(tree("QScript (optimized)", opz)))
      n1ql <- opz.cataM(
                Planner[T, Free[S, ?], CBQS].plan
              ).leftMap(FileSystemError.qscriptPlanningFailed(_))
      q    <- RenderQuery.compact(n1ql).liftPE
      _    <- tell(Vector(detail("N1QL AST", n1ql.render.shows)))
      _    <- tell(Vector(detail("N1QL", q)))
    } yield n1ql
  }

  def listRootContents[S[_]]
  (implicit
    S0: Task :<: S,
    context: Read.Ops[Context, S]
  ): Free[S, FileSystemError \/ Set[PathSegment]] =
    (for {
      ctx      <- context.ask.liftM[FileSystemErrT]
      bktNames <- lift(Task.delay(
                    ctx.manager.getBuckets.asScala.toList.map(_.name)
                  )).into.liftM[FileSystemErrT]
    } yield bktNames.map(DirName(_).left[FileName]).toSet).run

  def listNonRootContents[S[_]](
    dir: APath
  )(implicit
    S0: Task :<: S,
    context: Read.Ops[Context, S]
  ): Free[S, FileSystemError \/ Set[PathSegment]] =
    (for {
      ctx    <- context.ask.liftM[FileSystemErrT]
      bktCol <- EitherT(bucketCollectionFromPath(dir).η[Free[S, ?]])
      bkt    <- EitherT(getBucket(bktCol.bucket))
      types  <- lift(docTypesFromPrefix(bkt, bktCol.collection)).into.liftM[FileSystemErrT]
      _      <- EitherT((
                  if (types.isEmpty) FileSystemError.pathErr(PathError.pathNotFound(dir)).left
                  else ().right
                ).η[Free[S, ?]])
    } yield pathSegmentsFromPrefixTypes(bktCol.collection, types)).run

}
