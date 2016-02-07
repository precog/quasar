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
import quasar.api._
import quasar.fp._
import quasar.fs.{APath, PathError2, sandboxAbs}
import quasar.fs.mount._

import argonaut._
import org.http4s._, Method.MOVE
import org.http4s.headers.Accept
import org.http4s.dsl.{Path => HPath, _}
import org.http4s.server._
import org.http4s.argonaut._
import pathy.Path, Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object mount {
  def mountingErrorResponse(err: MountingError): Task[Response] =
    err match {
      case MountingError.PathError(e @ PathError2.Case.InvalidPath(path, reason))
                                                    => errorResponse(Conflict, s"cannot mount at ${posixCodec.printPath(path)} because $reason")
      case MountingError.PathError(e)               => pathErrorResponse(e)
      case MountingError.EnvironmentError(e)        => errorResponse(InternalServerError, e.shows)
      case MountingError.InvalidConfig(cfg, reasons) => errorResponse(BadRequest, reasons.list.mkString("; "))
    }

  def pathTypeMismatchResponse(err: Mounting.PathTypeMismatch): Task[Response] = {
    val expectedType = refineType(err.path).fold(κ("file"), κ("directory"))
    errorResponse(BadRequest, s"wrong path type for mount: ${posixCodec.printPath(err.path)}; $expectedType path required")
  }

  def service[S[_]: Functor]
      (f: S ~> Task)
      (implicit
        M: Mounting.Ops[S]
      ): HttpService = {

    def addPath(path: APath, req: Request, replacing: Boolean): EitherT[Task, Task[Response], Boolean] =
      for {
        body  <- EitherT.right(EntityDecoder.decodeString(req))
        bConf <- EitherT(Task.now(Parse.decodeWith[Task[Response] \/ MountConfig2, MountConfig2](body,
                    \/.right,
                    parseErrorMsg => errorResponse(BadRequest, s"input error: $parseErrorMsg").left,
                    { case (msg, _) => errorResponse(BadRequest, msg).left })))
        exists <- EitherT.right(M.lookup(path).isDefined.foldMap(f))
        mnt    =  if (replacing && exists) M.replace(path, bConf) else M.mount(path, bConf)
        _      <- EitherT(mnt.run.foldMap(f).map(_.fold(
                    mountingErrorResponse(_).left,
                    _.leftMap(pathTypeMismatchResponse))))
      } yield exists

    HttpService {
      case GET -> AsPath(path) =>
        M.lookup(path).fold(
          cfg => Ok(EncodeJson.of[MountConfig2].encode(cfg)),
          errorResponse(NotFound, "There is no mount point at " + posixCodec.printPath(path)))
        .foldMap(f).join

      case req @ MOVE -> AsPath(src) =>
        def move[T](src: Path[Abs, T, Sandboxed], dstStr: String, parse: String => Option[Path[Abs, T, Unsandboxed]], typeStr: String)
            : EitherT[M.F, Task[Response], Unit] =
          parse(dstStr).map(sandboxAbs).cata(
            dst => M.remount[T](src, dst).leftMap(mountingErrorResponse),
            EitherT.left(errorResponse(BadRequest, s"Not an absolute $typeStr path: $dstStr").point[M.F]))
        (for {
          dst  <- EitherT(Task.now(requiredHeader(Destination, req)))
          _    <- EitherT(refineType(src).fold[EitherT[M.F, Task[Response], Unit]](
            srcDir => move(srcDir, dst, posixCodec.parseAbsDir, "directory"),
            srcFile => move(srcFile, dst, posixCodec.parseAbsFile, "file")).run.foldMap(f))
          resp <- EitherT.right(Ok(s"moved ${posixCodec.printPath(src)} to $dst"))
        } yield resp).run.map(_.map(Task.now).merge).join

      case req @ POST -> AsDirPath(parent) => (for {
        f   <- EitherT(Task.now(
                requiredHeader(XFileName, req)))
        dst <- EitherT(Task.now(
                (posixCodec.parseRelDir(f) orElse posixCodec.parseRelFile(f))
                  .flatMap(sandbox(currentDir, _)).map(parent </> _) \/>
                  errorResponse(BadRequest, "Not a relative path: " + f)))
        _   <- addPath(dst, req, false)
      } yield Ok("added " + posixCodec.printPath(dst))).merge.join

      case req @ PUT -> AsPath(path) => (for {
        upd <- addPath(path, req, true)
      } yield Ok((if (upd) "updated" else "added") + " " + posixCodec.printPath(path))).merge.join

      case DELETE -> AsPath(p) =>
        M.unmount(p).run.foldMap(f).map(_.fold(
          mountingErrorResponse,
          κ(Ok(s"deleted ${posixCodec.printPath(p)}")))).join
    }
  }
}
