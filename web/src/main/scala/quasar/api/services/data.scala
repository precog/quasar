/*
 * Copyright 2014 - 2015 SlamData Inc.
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

import quasar.{DataCodec, Data}
import quasar.api._, ToQuasarResponse.ops._
import quasar.fp._
import quasar.fs._
import quasar.Predef._
import quasar.repl.Prettify

import java.nio.charset.StandardCharsets

import argonaut.Argonaut._
import argonaut.Json
import org.http4s._
import org.http4s.dsl._
import org.http4s.headers.{`Content-Type`, Accept}
import pathy.Path._, posixCodec._
import scalaz.{Zip => _, _}, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process
import scodec.bits.ByteVector

object data {
  import ManageFile.{MoveSemantics, MoveScenario}

  def service[S[_]: Functor](implicit
    R: ReadFile.Ops[S],
    W: WriteFile.Ops[S],
    M: ManageFile.Ops[S],
    Q: QueryFile.Ops[S],
    S0: Task :<: S,
    S1: FileSystemFailureF :<: S
  ): QHttpService[S] = QHttpService {

    case req @ GET -> AsPath(path) :? Offset(offsetParam) +& Limit(limitParam) =>
      (offsetOrInvalid[S](offsetParam) |@| limitOrInvalid[S](limitParam)) { (offset, limit) =>
        val requestedFormat = MessageFormat.fromAccept(req.headers.get(Accept))
        download[S](requestedFormat, path, offset.getOrElse(Natural._0), limit)
      }.merge.point[R.F]

    case req @ POST -> AsFilePath(path) =>
      upload(req, W.append(path, _))

    case req @ PUT -> AsFilePath(path) =>
      upload(req, W.save(path, _))

    case req @ Method.MOVE -> AsPath(path) =>
      (for {
        dst <- EitherT.fromDisjunction[M.F](
                 requiredHeader[S](Destination, req) map (_.value))
        scn <- EitherT.fromDisjunction[M.F](moveScenario(path, dst))
                 .leftMap(QuasarResponse.error[S](BadRequest, _))
        _   <- M.move(scn, MoveSemantics.FailIfExists)
                 .leftMap(_.toResponse[S])
      } yield QuasarResponse.empty[S].withStatus(Created)).merge

    case DELETE -> AsPath(path) =>
      respond(M.delete(path).run)
  }

  ////

  private val dataDecoder: EntityDecoder[Process[Task, DecodeError \/ Data]] =
    MessageFormat.decoder orElse EntityDecoder.error(MessageFormat.UnsupportedContentType)

  private def download[S[_]: Functor](
    format: MessageFormat,
    path: APath,
    offset: Natural,
    limit: Option[Positive]
  )(implicit
    R: ReadFile.Ops[S],
    Q: QueryFile.Ops[S],
    S0: FileSystemFailureF :<: S,
    S1: Task :<: S
  ): QuasarResponse[S] =
    refineType(path).fold(
      dirPath => {
        val p = zippedContents[S](dirPath, format, offset, limit)
        val headers = `Content-Type`(MediaType.`application/zip`) :: format.disposition.toList
        QuasarResponse.headers.modify(_ ++ headers)(QuasarResponse.streaming(p))
      },
      filePath => formattedDataResponse(format, R.scan(filePath, offset, limit)))

  private def moveScenario(src: APath, dstStr: String): String \/ MoveScenario =
    refineType(src).fold(
      srcDir =>
        parseAbsDir(dstStr)
          .map(sandboxAbs)
          .map(MoveScenario.DirToDir(srcDir, _))
          .toRightDisjunction("Cannot move directory into a file"),
      srcFile =>
        parseAbsFile(dstStr)
          .map(sandboxAbs)
          // TODO: Why not move into directory if dst is a dir?
          .map(MoveScenario.FileToFile(srcFile, _))
          .toRightDisjunction("Cannot move a file into a directory, must specify destination precisely"))

  // TODO: Streaming
  private def upload[S[_]: Functor](
    req: Request,
    by: Process[Free[S,?], Data] => Process[FileSystemErrT[Free[S,?],?], FileSystemError]
  )(implicit S0: Task :<: S): Free[S, QuasarResponse[S]] = {
    import free._

    type FreeS[A] = Free[S, A]
    type FreeFS[A] = FileSystemErrT[FreeS, A]
    type QRespT[F[_], A] = EitherT[F, QuasarResponse[S], A]

    def dataError[A: Show](status: Status, errs: IndexedSeq[A]): QuasarResponse[S] =
      QuasarResponse.json(status, Json(
        "error"   := "some uploaded value(s) could not be processed",
        "details" := Json.array(errs.map(e => jString(e.shows)): _*)))

    def errorsResponse(
      decodeErrors: IndexedSeq[DecodeError],
      persistErrors: Process[FreeFS, FileSystemError]
    ): Free[S, QuasarResponse[S]] =
      if (decodeErrors.nonEmpty)
        dataError(BadRequest, decodeErrors).point[FreeS]
      else
        persistErrors.runLog.fold(_.toResponse[S], errs =>
          if (errs.isEmpty) QuasarResponse.ok[S]
          else dataError(InternalServerError, errs))

    def write(xs: IndexedSeq[(DecodeError \/ Data)]): Free[S, QuasarResponse[S]] =
      if (xs.isEmpty) {
        QuasarResponse.error(BadRequest, "Request has no body").point[FreeS]
      } else {
        val (errors, data) = xs.toVector.separate
        errorsResponse(errors, by(Process.emitAll(data)))
      }

    injectFT[Task, S].apply(
      dataDecoder.decode(req)
        .leftMap(_.toResponse[S])
        .flatMap(_.runLog.liftM[QRespT])
        .run handleWith {
          case MessageFormat.UnsupportedContentType =>
            QuasarResponse.error[S](
              UnsupportedMediaType,
              "No media-type is specified in Content-Type header"
            ).left.point[Task]
        })
      .flatMap(_.fold(_.point[FreeS], write(_)))
  }

  private def zippedContents[S[_]: Functor](
    dir: AbsDir[Sandboxed],
    format: MessageFormat,
    offset: Natural,
    limit: Option[Positive]
  )(implicit
    R: ReadFile.Ops[S],
    Q: QueryFile.Ops[S]
  ): Process[R.M, ByteVector] =
    Process.await(Q.descendantFiles(dir)) { files =>
      Zip.zipFiles(files.toList map { file =>
        val data = R.scan(dir </> file, offset, limit)
        val bytes = format.encode(data).map(str => ByteVector.view(str.getBytes(StandardCharsets.UTF_8)))
        (file, bytes)
      })
    }
}
