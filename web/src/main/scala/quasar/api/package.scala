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

package quasar

import slamdata.Predef.{ -> => _, _ }
import quasar.contrib.pathy._

import argonaut.{DecodeResult => _, _}, Argonaut._
import org.http4s._
import org.http4s.dsl.{Path => HPath, _}
import org.http4s.headers.Warning
import org.http4s.server.staticcontent._
import pathy.Path, Path._
import pathy.argonaut.PosixCodecJson._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task

package object api {
  type FailedResponseT[F[_], A] = EitherT[F, FailedResponse, A]
  type FailedResponseOr[A]      = FailedResponseT[Task, A]

  type ApiErrT[F[_], A] = EitherT[F, ApiError, A]

  // https://tools.ietf.org/html/rfc7234#section-4.2.4
  val StaleHeader = Header(Warning.name.value, """110 - "Response is Stale"""")

  // TODO: See if possible to avoid re-encoding and decoding
  object AsDirPath {
    def unapply(p: HPath): Option[ADir] = {
      UriPathCodec.parseAbsDir(pathString(p)) map unsafeSandboxAbs
    }
  }

  object AsFilePath {
    def unapply(p: HPath): Option[AFile] = {
      UriPathCodec.parseAbsFile(pathString(p)) map unsafeSandboxAbs
    }
  }

  object AsPath {
    def unapply(p: HPath): Option[APath] = {
      AsDirPath.unapply(p) orElse AsFilePath.unapply(p)
    }
  }

  def decodedDir(encodedPath: String): ApiError \/ ADir =
    decodedPath(encodedPath) flatMap { path =>
      refineType(path).swap.leftAs(ApiError.fromMsg(
        BadRequest withReason "Directory path expected.",
        s"Expected '${posixCodec.printPath(path)}' to be a directory.",
        "path" := path))
    }

  def decodedFile(encodedPath: String): ApiError \/ AFile =
    decodedPath(encodedPath) flatMap { path =>
      refineType(path).leftAs(ApiError.fromMsg(
        BadRequest withReason "File path expected.",
        s"Expected '${posixCodec.printPath(path)}' to be a file.",
        "path" := path))
    }

  def decodedPath(encodedPath: String): ApiError \/ APath =
    AsPath.unapply(HPath(encodedPath)) \/> ApiError.fromMsg(
      BadRequest withReason "Malformed path.",
      s"Failed to parse '${UriPathCodec.unescape(encodedPath)}' as an absolute path.",
      "encodedPath" := encodedPath)

  def transcode(from: PathCodec, to: PathCodec): String => String =
    from.parsePath(to.unsafePrintPath, to.unsafePrintPath, to.unsafePrintPath, to.unsafePrintPath)

  def staticFileService(basePath: String): HttpService =
    fileService(FileService.Config(systemPath = basePath))

  def redirectService(basePath: String) = HttpService {
    // NB: this means we redirected to a path that wasn't handled, and need
    // to avoid getting into a loop.
    case GET -> path if path.startsWith(HPath(basePath)) => NotFound()

    case req @ GET -> AsPath(path) =>
      // TODO: probably need a URL-specific codec here
      TemporaryRedirect(req.uri.copy(path = basePath + posixCodec.printPath(path)))
  }

  // NB: HPath's own toString doesn't encode properly
  private def pathString(p: HPath) =
    "/" + p.toList.map(UriPathCodec.escape).mkString("/")
}
