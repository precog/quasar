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

package quasar.api.services

import quasar.Predef._
import quasar.api._, ToApiError.ops._
import quasar.fp._
import quasar.fs.{AbsPath, AFile, APath, sandboxAbs}
import quasar.fs.mount._

import argonaut._, Argonaut._
import org.http4s._, Method.MOVE
import org.http4s.dsl._
import pathy.Path, Path._
import pathy.argonaut.PosixCodecJson._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

// TODO: Remove the "low level" MountConfigsF effect from mount. It was
// introduced here as a workaround for a few regressions in the mount api when
// the appropriate fix at another level wasn't obvious. Relatedly ServiceSpec
// was introduced to capture a few scenarios that seemingly should have been
// captured by existing tests, but were not due to the differences between the
// wiring of interpreters in Server vs tests. Once the server's use of
// interpreters is closer to the way tests use them the appropriate fix should
// become apparent and the need for MountConfigsF here should disappear.

object mount {
  import posixCodec.printPath

  def service[S[_]](
    implicit
    M: Mounting.Ops[S],
    S0: Task :<: S,
    S1: MountConfigs :<: S
  ): QHttpService[S] =
    QHttpService {
      case GET -> AsPath(path) =>
        def err = ApiError.fromMsg(
          NotFound withReason "Mount point not found.",
          s"There is no mount point at ${printPath(path)}",
          "path" := path)
        respond(M.lookup(path).toRight(err).run)

      case req @ MOVE -> AsPath(src) =>
        respond(requiredHeader(Destination, req).map(_.value).fold(
            err => EitherT.leftU[String](err.point[Free[S, ?]]),
            dst => EitherT[Free[S, ?], ApiError, String] {
              OptionT[Free[S, ?], (AFile, AFile)](
                (refineType(src).toOption |@| UriPathCodec.parseAbsFile(dst).map(sandboxAbs)).tupled.point[Free[S, ?]])
                .flatMap { case sd @ (s, _) => OptionT(ViewMounter.exists[S](s).map(_ option sd)) }
                .map((ViewMounter.move[S] _).tupled(_) *>
                     s"moved ${printPath(src)} to $dst".right[ApiError].point[Free[S, ?]])
                .getOrElse(refineType(src).fold(
                  srcDir  => move[S, Dir](srcDir,  dst, UriPathCodec.parseAbsDir,  "directory"),
                  srcFile => move[S, File](srcFile, dst, UriPathCodec.parseAbsFile, "file")).run).join
            }).run)

      case req @ POST -> AsDirPath(parent) => respond((for {
        hdr <- EitherT.fromDisjunction[M.F](requiredHeader(XFileName, req))
        fn  =  hdr.value
        dst <- EitherT.fromDisjunction[M.F](
                (UriPathCodec.parseRelDir(fn) orElse UriPathCodec.parseRelFile(fn))
                  .flatMap(sandbox(currentDir, _))
                  .map(parent </> _)
                  .toRightDisjunction(ApiError.apiError(
                    BadRequest withReason "Filename must be a relative path.",
                    "fileName" := fn)))
        _   <- mount[S](dst, req, replaceIfExists = false)
      } yield s"added ${printPath(dst)}").run)

      case req @ PUT -> AsPath(path) =>
        respond(
          mount[S](path, req, replaceIfExists = true)
            .map(upd => s"${upd ? "updated" | "added"} ${printPath(path)}")
            .run)

      case DELETE -> AsPath(p) =>
        respond(M.unmount(p).as(s"deleted ${printPath(p)}").run)
    }

  ////

  private def move[S[_], T](
    src: AbsPath[T],
    dstStr: String,
    parse: String => Option[Path[Abs, T, Unsandboxed]],
    typeStr: String
  )(implicit
    M: Mounting.Ops[S]
  ): EitherT[Free[S, ?], ApiError, String] =
    parse(dstStr).map(sandboxAbs).cata(dst =>
      M.remount[T](src, dst)
        .as(s"moved ${printPath(src)} to ${printPath(dst)}")
        .leftMap(_.toApiError),
      ApiError.apiError(
        BadRequest withReason s"Expected an absolute $typeStr.",
        "path" := transcode(UriPathCodec, posixCodec)(dstStr)
      ).raiseError[EitherT[M.F, ApiError, ?], String])

  private def mount[S[_]](
    path: APath,
    req: Request,
    replaceIfExists: Boolean
  )(implicit
    M: Mounting.Ops[S],
    S0: Task :<: S,
    S1: MountConfigs :<: S
  ): EitherT[Free[S, ?], ApiError, Boolean] = {
    type FreeS[A] = Free[S, A]

    for {
      body  <- EitherT.right(free.injectFT[Task, S].apply(
                 EntityDecoder.decodeString(req)): FreeS[String])
      bConf <- EitherT.fromDisjunction[FreeS](Parse.decodeWith(
                  body,
                  (_: MountConfig).right[ApiError],
                  parseErrorMsg => ApiError.fromMsg_(
                    BadRequest withReason "Malformed input.",
                    parseErrorMsg).left,
                  (msg, _) => ApiError.fromMsg_(
                    BadRequest, msg).left))
      exists <- EitherT.right(M.lookup(path).isDefined)
      mnt    =
        if (replaceIfExists && exists) M.replace(path, bConf)
        else bConf match {
          case MountConfig.ViewConfig(expr, vars) =>
            refineType(path).fold(
              κ(M.mount(path, bConf)),
              f => EitherT[Free[S, ?], MountingError, Unit](ViewMounter.mount[S](f, expr, vars)).map(_.right))
          case mc: MountConfig.FileSystemConfig =>
            M.mount(path, bConf)
        }
      r      <- mnt.leftMap(_.toApiError)
      _      <- EitherT.fromDisjunction[FreeS](r.leftMap(_.toApiError))
    } yield exists
  }
}
