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
import quasar.{Data, DataCodec}
import quasar.common.PhaseResult.detail
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.effect.uuid.GenUUID
import quasar.fp.free._
import quasar.fp.ski._
import quasar.fs._, QueryFile.ResultHandle
import quasar.physical.couchbase._, Couchbase._, common._

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import matryoshka._
import rx.lang.scala._, JavaConverters._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

abstract class queryfile {
  val results = KeyValueStore.Ops[ResultHandle, Cursor, Eff]

  def n1qlResults[T[_[_]]: BirecursiveT, S[_]](n1ql: T[N1QL]): Backend[Vector[Data]] =
    for {
      ctx <- MR.asks(_.ctx)
      q   <- ME.unattempt(
               RenderQuery.compact(n1ql).leftMap(FileSystemError.qscriptPlanningFailed(_)).η[Backend])
      _   <- MT.tell(Vector(detail("N1QL", q)))
      r   <- ME.unattempt(lift(queryData(ctx.bucket, q)).into[Eff].liftB)
      v   =  r >>= (Data._obj.getOption(_).foldMap(_.values.toVector))
    } yield v

  def executePlan(repr: Repr, out: AFile): Backend[AFile] =
    for {
      ctx    <- MR.asks(_.ctx)
      r      <- n1qlResults(repr)
      col    =  docTypeValueFromPath(out)
      docs   <- r.map(DataCodec.render).unite.traverse(d => GenUUID.Ops[Eff].asks(uuid =>
                  JsonDocument.create(
                    uuid.toString,
                    JsonObject
                      .create()
                      .put(ctx.docTypeKey.v, col.v)
                      .put("value", jsonTranscoder.stringToJsonObject(d)))
                )).liftB
      exists <- ME.unattempt(lift(existsWithPrefix(ctx, col.v)).into[Eff].liftB)
      _      <- exists.whenM(lift(deleteHavingPrefix(ctx, col.v)).into[Eff].liftB)
      _      <- lift(docs.nonEmpty.whenM(Task.delay(
                  Observable
                    .from(docs)
                    .flatMap(ctx.bucket.async.insert(_).asScala)
                    .toBlocking
                    .last
                ))).into[Eff].liftB
    } yield out

  def evaluatePlan(repr: Repr): Backend[ResultHandle] =
    for {
      r    <- n1qlResults(repr)
      i    <- MonotonicSeq.Ops[Eff].next.liftB
      h    =  ResultHandle(i)
      _    <- results.put(h, Cursor(r)).liftB
    } yield h

  def more(h: ResultHandle): Backend[Vector[Data]] =
    for {
      c       <- ME.unattempt(results.get(h).toRight(FileSystemError.unknownResultHandle(h)).run.liftB)
      (cʹ, r) =  resultsFromCursor(c)
      _       <- results.put(h, cʹ).liftB
    } yield r

  def close(h: ResultHandle): Configured[Unit] =
    results.delete(h).liftM[ConfiguredT]

  def explain(repr: Repr): Backend[String] =
    ME.unattempt(RenderQuery.compact(repr).leftMap(FileSystemError.qscriptPlanningFailed(_)).η[Backend])

  def listContents(dir: ADir): Backend[Set[PathSegment]] =
    for {
      ctx    <- MR.asks(_.ctx)
      col    =  docTypeValueFromPath(dir)
      types  <- ME.unattempt(lift(docTypeValuesFromPrefix(ctx, col.v)).into[Eff].liftB)
      _      <- types.isEmpty.whenM(
                  ME.raiseError(FileSystemError.pathErr(PathError.pathNotFound(dir))))
    } yield pathSegmentsFromPrefixDocTypeValues(col.v, types)

  def fileExists(file: AFile): Configured[Boolean] =
    (for {
      ctx    <- MR.asks(_.ctx)
      col    =  docTypeValueFromPath(file)
      exists <- ME.unattempt(lift(existsWithPrefix(ctx, col.v)).into[Eff].liftB)
    } yield exists).valueOr(κ(false)).value
}
