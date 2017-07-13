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
import quasar.contrib.pathy._
import quasar.contrib.scalaz._, eitherT._
import quasar.{Data, DataCodec}
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.effect.uuid.GenUUID
import quasar.fp.free._
import quasar.fs._, WriteFile.WriteHandle
import quasar.physical.couchbase._, common._, Couchbase._

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import rx.lang.scala._, JavaConverters._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

abstract class writefile {
  val wh = KeyValueStore.Ops[WriteHandle, Collection, Eff]

  def open(file: AFile): Backend[WriteHandle] =
    for {
      ctx    <- MR.asks(_.ctx)
      col    =  docTypeValueFromPath(file)
      i      <- MonotonicSeq.Ops[Eff].next.liftB
      handle =  WriteHandle(file, i)
      _      <- wh.put(handle, Collection(ctx.bucket, col)).liftB
    } yield handle

  def write(h: WriteHandle, chunk: Vector[Data]): Configured[Vector[FileSystemError]] =
    (for {
      ctx  <- MR.asks(_.ctx)
      col  <- ME.unattempt(wh.get(h).toRight(FileSystemError.unknownWriteHandle(h)).run.liftB)
      data <- lift(Task.delay(chunk.map(DataCodec.render).unite
                .map(str =>
                  JsonObject
                    .create()
                    .put(ctx.docTypeKey.v, col.docTypeValue.v)
                    .put("value", jsonTranscoder.stringToJsonObject(str))))
              ).into[Eff].liftB
      docs <- data.traverse(d => GenUUID.Ops[Eff].asks(uuid =>
                JsonDocument.create(uuid.toString, d)
              )).liftB
      _    <- lift(Task.delay(
                Observable
                  .from(docs)
                  .flatMap(ctx.bucket.async.insert(_).asScala)
                  .toBlocking
                  .last
              )).into[Eff].liftB
    } yield Vector()).run.value ∘ (_.valueOr(Vector(_)))

  def close(h: WriteHandle): Configured[Unit] =
    wh.delete(h).liftM[ConfiguredT]
}
