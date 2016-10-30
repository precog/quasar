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

package quasar.physical.couchbase.fs

import quasar.Predef._
import quasar.Data
import quasar.common.{PhaseResults, PhaseResultT}
import quasar.common.PhaseResult.{detail, tree}
import quasar.contrib.pathy._
import quasar.contrib.matryoshka._
import quasar.effect.{KeyValueStore, Read, MonotonicSeq}
import quasar.effect.uuid.GenUUID
import quasar.fp._, eitherT._
import quasar.fp.free._
import quasar.fp.ski._
import quasar.fs._
import quasar.logicalPlan.LogicalPlan
import quasar.physical.couchbase._, common._, N1QL._, planner._
import quasar.qscript.{Read => _, _}

import scala.collection.JavaConverters._

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import matryoshka._, FunctorT.ops._, Recursive.ops._
import pathy.Path._
import rx.lang.scala._, JavaConverters._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object queryfile {
  import QueryFile._

  type Plan[S[_], A] = FileSystemErrT[PhaseResultT[Free[S, ?], ?], A]

  implicit class LiftP[F[_]: Monad, A](p: F[A]) {
    val liftP = p.liftM[PhaseResultT].liftM[FileSystemErrT]
  }

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

  def executePlan[S[_]](
    lp: Fix[LogicalPlan], out: AFile
  )(implicit
    S0: Read[Context, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: GenUUID :<: S,
    S3: Task :<: S
  ): Free[S, (PhaseResults, FileSystemError \/ AFile)] =
    (for {
      n1ql   <- lpToN1ql(lp)
      r      <- n1qlResults(n1ql)
      bktCol <- bucketCollectionFromPath(out).bimap(
                  e => EitherT(e.left[BucketCollection].point[Free[S, ?]].liftM[PhaseResultT]),
                  _.point[Free[S, ?]].liftP
                ).merge
      docs   <- r.traverse(d => GenUUID.Ops[S].asks(uuid =>
                  JsonDocument.create(
                    uuid.toString,
                    JsonObject
                      .create()
                      .put("type", bktCol.collection)
                      .put("value", d))
                )).liftP
      ctx    <- Read.Ops[Context, S].ask.liftP
      bkt    <- lift(Task.delay(
                  ctx.cluster.openBucket(bktCol.bucket)
                )).into.liftP

      _    <- lift(Task.delay(
                Observable
                  .from(docs)
                  .flatMap(bkt.async.insert(_).asScala)
                  .toBlocking
                  .last
              )).into.liftP
    } yield out).run.run

  def evaluatePlan[S[_]](
    lp: Fix[LogicalPlan]
  )(implicit
    S0: Read[Context, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: Task :<: S,
    results: KeyValueStore.Ops[ResultHandle, Cursor, S]
  ): Free[S, (PhaseResults, FileSystemError \/ ResultHandle)] =
    (for {
      n1ql <- lpToN1ql(lp)
      r    <- n1qlResults(n1ql)
      i    <- MonotonicSeq.Ops[S].next.liftP
      h    =  ResultHandle(i)
      _    <- results.put(h, Cursor(r)).liftP
    } yield h).run.run

  def more[S[_]](
    handle: ResultHandle
  )(implicit
    results: KeyValueStore.Ops[ResultHandle, Cursor, S]
  ): Free[S, FileSystemError \/ Vector[Data]] =
    (for {
      h <- results.get(handle).toRight(FileSystemError.unknownResultHandle(handle))
      r <- EitherT(resultsFromCursor(h).point[Free[S, ?]])
      _ <- results.put(handle, r._1).liftM[FileSystemErrT]
    } yield r._2).run

  def close[S[_]](
    handle: ResultHandle
  )(implicit
    results: KeyValueStore.Ops[ResultHandle, Cursor, S]
  ): Free[S, Unit] =
    results.delete(handle)

  def explain[S[_]](
    lp: Fix[LogicalPlan]
  )(implicit
    S0: Read[Context, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: Task :<: S
  ): Free[S, (PhaseResults, FileSystemError \/ ExecutionPlan)] =
    (lpToN1ql(lp) ∘ (n1ql => ExecutionPlan(FsType, outerN1ql(n1ql)))).run.run


  def listContents[S[_]](
    dir: APath
  )(implicit
    S0: Task :<: S,
    context: Read.Ops[Context, S]
  ): Free[S, FileSystemError \/ Set[PathSegment]] =
    if (dir === rootDir)
      listRootContents(dir)
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
      bktCol <- EitherT(bucketCollectionFromPath(file).point[Free[S, ?]])
      bkt    <- EitherT(getBucket(bktCol.bucket))
      exists <- lift(existsWithPrefix(bkt, bktCol.collection)).into.liftM[FileSystemErrT]
    } yield exists).exists(ι)

  def n1qlResults[S[_]](
    n1ql: N1QL
  )(implicit
    S0: Task :<: S,
    context: Read.Ops[Context, S]
  ): Plan[S, Vector[JsonObject]] = {
    val n1qlStr = outerN1ql(n1ql)

    for {
      _       <- prtell[Plan[S, ?]](Vector(detail(
                   "N1QL Results",
                   s"""  n1ql: $n1qlStr""".stripMargin('|'))))
      ctx     <- context.ask.liftP
      bkt     <- lift(Task.delay(
                   ctx.cluster.openBucket()
                 )).into.liftP
      r       <- lift(Task.delay(
                   bkt.query(n1qlQuery(n1qlStr))
                     .allRows
                     .asScala
                     .toVector
                     .map(_.value)
                 )).into.liftP
    } yield r
  }

  def lpToN1ql[S[_]](
    lp: Fix[LogicalPlan]
  )(implicit
    S0: Read[Context, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: Task :<: S
  ): Plan[S, N1QL] = {
    val lc: DiscoverPath.ListContents[Plan[S, ?]] =
      (d: ADir) => EitherT(listContents(d).liftM[PhaseResultT])

    lpLcToN1ql(lp, lc)
  }

  def lpLcToN1ql[S[_]](
    lp: Fix[LogicalPlan],
    lc: DiscoverPath.ListContents[Plan[S, ?]]
  )(implicit
    S1: MonotonicSeq :<: S,
    S2: Task :<: S
  ): Plan[S, N1QL] = {
    type CBQScript[A] = (QScriptCore[Fix, ?] :\: EquiJoin[Fix, ?] :/: Const[ShiftedRead, ?])#M[A]

    val tell = prtell[Plan[S, ?]] _

    for {
      _    <- tell(Vector(tree("lp", lp)))
      qs   <- convertToQScriptRead[
                Fix,
                Plan[S, ?],
                QScriptRead[Fix, ?]
              ](lc)(lp)
      _    <- tell(Vector(tree("QS post convertToQScriptRead", qs)))
      shft =  shiftRead(qs).transCata(
                SimplifyJoin[Fix, QScriptShiftRead[Fix, ?], CBQScript]
                  .simplifyJoin(idPrism.reverseGet))
      _    <- tell(Vector(tree("QS post shiftRead", qs)))
      n1ql <- shft.cataM(
                Planner[Free[S, ?], CBQScript].plan
              ).leftMap(FileSystemError.planningFailed(lp, _))
    } yield n1ql
  }

  def listRootContents[S[_]](
    dir: APath
  )(implicit
    S0: Task :<: S,
    context: Read.Ops[Context, S]
  ): Free[S, FileSystemError \/ Set[PathSegment]] =
    (for {
      ctx      <- context.ask.liftM[FileSystemErrT]
      bktNames <- lift(Task.delay(
                    ctx.manager.getBuckets.asScala.toList.map(_.name)
                  )).into.liftM[FileSystemErrT]
      bkts     <- bktNames.traverse(n => EitherT(getBucket(n)))
      bktCols  <- bkts.traverseM(bkt => lift(Task.delay(
                    bkt.query(n1qlQuery(s"select distinct type from `${bkt.name}`"))
                      .allRows.asScala.toList.map(r =>
                        BucketCollection(bkt.name, new String(r.byteValue)))
                  )).into.liftM[FileSystemErrT])
    } yield pathSegmentsFromBucketCollections(bktCols)).run

  def listNonRootContents[S[_]](
    dir: APath
  )(implicit
    S0: Task :<: S,
    context: Read.Ops[Context, S]
  ): Free[S, FileSystemError \/ Set[PathSegment]] =
    (for {
      ctx    <- context.ask.liftM[FileSystemErrT]
      bktCol <- EitherT(bucketCollectionFromPath(dir).point[Free[S, ?]])
      bkt    <- EitherT(getBucket(bktCol.bucket))
      docIds <- lift(docIdTypesWithTypePrefix(bkt, bktCol.collection)).into.liftM[FileSystemErrT]
      _      <- EitherT((
                  if (docIds.isEmpty) FileSystemError.pathErr(PathError.pathNotFound(dir)).left
                  else ().right
                ).point[Free[S, ?]])
    } yield pathSegmentsFromPrefixDocIds(bktCol.collection, docIds)).run

}
