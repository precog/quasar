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
import quasar.{Data, DataCodec}
import quasar.effect.{KeyValueStore, MonotonicSeq, Read}
import quasar.effect.uuid.GenUUID
import quasar.fp.free._
import quasar.fs._
import quasar.physical.couchbase.common._

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.transcoder.JsonTranscoder
import rx.lang.scala._, JavaConverters._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object writefile {
  import WriteFile._

  implicit val codec: DataCodec = CBDataCodec

  final case class State(collection: DocTypeValue)

  def interpret[S[_]](
    implicit
    S0: KeyValueStore[WriteHandle, State, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: Read[ClientContext, ?] :<:  S,
    S3: GenUUID :<: S,
    S4: Task :<: S
  ): WriteFile ~> Free[S, ?] = λ[WriteFile ~> Free[S, ?]] {
    case Open(file)     => open(file)
    case Write(h, data) => write(h, data)
    case Close(h)       => close(h)
  }

  def writeHandles[S[_]](
    implicit
    S0: KeyValueStore[WriteHandle, State, ?] :<: S
  ) = KeyValueStore.Ops[WriteHandle, State, S]

  val jsonTranscoder = new JsonTranscoder

  def open[S[_]](
    file: AFile
  )(implicit
    S0: KeyValueStore[WriteHandle, State, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: Task :<: S,
    context: Read.Ops[ClientContext, S]
  ): Free[S, FileSystemError \/ WriteHandle] =
    (for {
      ctx      <- context.ask.liftM[FileSystemErrT]
      col      <- docTypeValueFromPath(file).η[FileSystemErrT[Free[S, ?], ?]]
      i        <- MonotonicSeq.Ops[S].next.liftM[FileSystemErrT]
      handle   =  WriteHandle(file, i)
      _        <- writeHandles.put(handle, State(col)).liftM[FileSystemErrT]
    } yield handle).run

  def write[S[_]](
    h: WriteHandle,
    chunk: Vector[Data]
  )(implicit
    S0: KeyValueStore[WriteHandle, State, ?] :<: S,
    S1: GenUUID :<: S,
    S2: Task :<: S,
    context: Read.Ops[ClientContext, S]
  ): Free[S, Vector[FileSystemError]] =
    (for {
      st   <- writeHandles.get(h).toRight(Vector(FileSystemError.unknownWriteHandle(h)))
      ctx  <- context.ask.liftM[EitherT[?[_], Vector[FileSystemError], ?]]
      data <- lift(Task.delay(chunk.map(DataCodec.render).unite
                .map(str =>
                  JsonObject
                    .create()
                    .put(ctx.docTypeKey.v, st.collection.v)
                    .put("value", jsonTranscoder.stringToJsonObject(str))))).into.liftM[EitherT[?[_], Vector[FileSystemError], ?]]
      docs <- data.traverse(d => GenUUID.Ops[S].asks(uuid =>
                JsonDocument.create(uuid.toString, d)
              )).liftM[EitherT[?[_], Vector[FileSystemError], ?]]
      _    <- lift(Task.delay(
                Observable
                  .from(docs)
                  .flatMap(ctx.bucket.async.insert(_).asScala)
                  .toBlocking
                  .last
              )).into.liftM[EitherT[?[_], Vector[FileSystemError], ?]]
    } yield Vector.empty).merge

  def close[S[_]](
    h: WriteHandle
  )(implicit
    S0: KeyValueStore[WriteHandle, State, ?] :<: S,
    S1: Task :<: S,
    context: Read.Ops[ClientContext, S]
  ): Free[S, Unit] =
    writeHandles.delete(h)

}
