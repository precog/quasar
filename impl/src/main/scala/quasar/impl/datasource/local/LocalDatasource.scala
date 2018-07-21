/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.impl.datasource.local

import slamdata.Predef.{Stream => _, Seq => _, _}
import quasar.api.datasource.DatasourceType
import quasar.common.resource._, ResourceError._
import quasar.common.data.Data
import quasar.connector.Datasource
import quasar.connector.datasource.LightweightDatasource
import quasar.contrib.fs2.convert
import quasar.contrib.scalaz.MonadError_
import quasar.frontend.data.DataCodec.Precise
import quasar.fp.ski.ι

import java.nio.file.{Files, Path => JPath}
import java.text.ParseException

import scala.concurrent.ExecutionContext

import argonaut.Json
import cats.effect.{Effect, Timer}
import fs2.{io, Chunk, Stream}
import jawn.support.argonaut.Parser.facade
import jawnfs2._
import pathy.Path
import scalaz.{\/, Scalaz}, Scalaz._
import shims._

/** A Datasource backed by the underlying filesystem local to Quasar.
  *
  * TODO: Currently only supports reading line-delimited JSON files (with
  *       support for the "precise" `Data` codec). Expand this via a pluggable
  *       set of decoders for CSV, XML, avro, etc.
  *
  * @param root the scope of this datasource, all paths will be considered relative to this one.
  * @param readChunkSizeBytes the number of bytes per chunk to use when reading files.
  */
final class LocalDatasource[F[_]: Timer] private (
    root: JPath,
    readChunkSizeBytes: Int,
    pool: ExecutionContext)(
    implicit F: Effect[F], RE: MonadResourceErr[F])
    extends LightweightDatasource[F, Stream[F, ?], Stream[F, Data]] {

  implicit val ec: ExecutionContext = pool

  val kind: DatasourceType = LocalType

  def evaluate(path: ResourcePath): F[Stream[F, Data]] =
    for {
      jp <- toNio(path)

      exists <- F.delay(Files.exists(jp))
      _ <- exists.unlessM(RE.raiseError(pathNotFound(path)))

      isFile <- F.delay(Files.isRegularFile(jp))
      _ <- isFile.unlessM(RE.raiseError(notAResource(path)))
    } yield {
      io.file.readAllAsync[F](jp, readChunkSizeBytes)
        .chunks
        .map(_.toByteBuffer)
        .parseJsonStream[Json]
        .chunks
        .flatMap(decodeChunk)
    }

  def pathIsResource(path: ResourcePath): F[Boolean] =
    toNio(path) >>= (jp => F.delay(Files.isRegularFile(jp)))

  def prefixedChildPaths(path: ResourcePath): F[Option[Stream[F, (ResourceName, ResourcePathType)]]] = {
    def withType(jp: JPath): F[(ResourceName, ResourcePathType)] =
      F.delay(Files.isRegularFile(jp))
        .map(_.fold(ResourcePathType.leafResource, ResourcePathType.prefix))
        .strengthL(toResourceName(jp))

    for {
      jp <- toNio(path)

      exists <- F.delay(Files.exists(jp))

      children = exists option {
        convert.fromJavaStream(F.delay(Files.list(jp)))
          .evalMap(withType)
      }
    } yield children
  }

  ////

  private val ME = MonadError_[F, Throwable]

  private def decodeChunk(c: Chunk[Json]): Stream[F, Data] =
    c.traverse(Precise.decode)
      .leftMap(e => new ParseException(e.message, -1))
      .fold(Stream.raiseError, Stream.chunk)
      .covary[F]

  private def toNio(rp: ResourcePath): F[JPath] =
    Path.flatten("", "", "", ι, ι, rp.toPath).foldLeftM(root) { (p, n) =>
      if (n.isEmpty) p.point[F]
      else ME.unattempt_(\/.fromTryCatchNonFatal(p.resolve(n)))
    }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  private def toResourceName(jp: JPath): ResourceName =
    ResourceName(jp.getFileName.toString)
}

object LocalDatasource {
  def apply[F[_]: Effect: MonadResourceErr: Timer](
      root: JPath,
      readChunkSizeBytes: Int,
      pool: ExecutionContext)
      : Datasource[F, Stream[F, ?], ResourcePath, Stream[F, Data]] =
    new LocalDatasource[F](root, readChunkSizeBytes, pool)
}
