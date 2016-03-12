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

import quasar.Data
import quasar.api._, ToQResponse.ops._
import quasar.fp._, numeric._
import quasar.fs._
import quasar.Predef._

import java.nio.charset.StandardCharsets

import argonaut.Argonaut._
import argonaut.Json
import eu.timepit.refined.auto._
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
        download[S](requestedFormat, path, offset.getOrElse(0L), limit)
      }.merge.point[R.F]

    case req @ POST -> AsFilePath(path) =>
      upload(req, W.appendThese(path, _))

    case req @ PUT -> AsFilePath(path) =>
      upload(req, W.saveThese(path, _))

    case req @ Method.MOVE -> AsPath(path) =>
      (for {
        dst <- EitherT.fromDisjunction[M.F](
                 requiredHeader[S](Destination, req) map (_.value))
        scn <- EitherT.fromDisjunction[M.F](moveScenario(path, dst))
                 .leftMap(QResponse.error[S](BadRequest, _))
        _   <- M.move(scn, MoveSemantics.FailIfExists)
                 .leftMap(_.toResponse[S])
      } yield QResponse.empty[S].withStatus(Created)).merge

    case DELETE -> AsPath(path) =>
      respond(M.delete(path).run)
  }

  ////

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
  ): QResponse[S] =
    refineType(path).fold(
      dirPath => {
        val p = zippedContents[S](dirPath, format, offset, limit)
        val headers = `Content-Type`(MediaType.`application/zip`) :: format.disposition.toList
        QResponse.headers.modify(_ ++ headers)(QResponse.streaming(p))
      },
      filePath => formattedDataResponse(format, R.scan(filePath, offset, limit)))

  private def moveScenario(src: APath, dstStr: String): String \/ MoveScenario =
    refineType(src).fold(
      srcDir =>
        parseAbsDir(dstStr)
          .map(sandboxAbs)
          .map(MoveScenario.dirToDir(srcDir, _))
          .toRightDisjunction("Cannot move directory into a file"),
      srcFile =>
        parseAbsFile(dstStr)
          .map(sandboxAbs)
          // TODO: Why not move into directory if dst is a dir?
          .map(MoveScenario.fileToFile(srcFile, _))
          .toRightDisjunction("Cannot move a file into a directory, must specify destination precisely"))

  // TODO: Streaming
  private def upload[S[_]: Functor](
    req: Request,
    by: Vector[Data] => FileSystemErrT[Free[S,?], Vector[FileSystemError]]
  )(implicit S0: Task :<: S): Free[S, QResponse[S]] = {
    type FreeS[A] = Free[S, A]
    type FreeFS[A] = FileSystemErrT[FreeS, A]
    type QRespT[F[_], A] = EitherT[F, QResponse[S], A]

    def dataError[A: Show](status: Status, errs: IndexedSeq[A]): QResponse[S] =
      QResponse.json(status, Json(
        "error"   := "some uploaded value(s) could not be processed",
        "details" := Json.array(errs.map(e => jString(e.shows)): _*)))

    def errorsResponse(
      decodeErrors: IndexedSeq[DecodeError],
      persistErrors: FreeFS[Vector[FileSystemError]]
    ): Free[S, QResponse[S]] =
      if (decodeErrors.nonEmpty)
        dataError(BadRequest, decodeErrors).point[FreeS]
      else
        persistErrors.fold(_.toResponse[S], errs =>
          if (errs.isEmpty) QResponse.ok[S]
          else dataError(InternalServerError, errs))

    def write(xs: IndexedSeq[(DecodeError \/ Data)]): Free[S, QResponse[S]] =
      if (xs.isEmpty) {
        QResponse.error(BadRequest, "Request has no body").point[FreeS]
      } else {
        val (errors, data) = xs.toVector.separate
        errorsResponse(errors, by(data))
      }

      injectFT[Task,S].apply(
        MessageFormat.decoder.decode(req,true)
          .leftMap(_.toResponse[S].point[FreeS])
          .flatMap(dataStream => EitherT.right(dataStream.runLog.map(write)))
          .merge
      ).flatMap(ι)
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
