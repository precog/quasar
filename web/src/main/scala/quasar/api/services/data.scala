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
import quasar.api._, ToQResponse.ops._, ToApiError.ops._
import quasar.contrib.pathy._
import quasar.fp._
import quasar.fp.numeric._
import quasar.fs._
import quasar.Predef._

import java.nio.charset.StandardCharsets

import argonaut.Argonaut._
import argonaut.ArgonautScalaz._
import eu.timepit.refined.auto._
import org.http4s._
import org.http4s.dsl._
import org.http4s.headers.{`Content-Type`, Accept}
import pathy.Path._
import pathy.argonaut.PosixCodecJson._
import scalaz.{Zip => _, _}, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process
import scodec.bits.ByteVector

object data {
  import ManageFile.{MoveSemantics, MoveScenario}

  def service[S[_]](
    implicit
    R: ReadFile.Ops[S],
    W: WriteFile.Ops[S],
    M: ManageFile.Ops[S],
    Q: QueryFile.Ops[S],
    S0: Task :<: S,
    S1: FileSystemFailure :<: S
  ): QHttpService[S] = QHttpService {

    case req @ GET -> AsPath(path) :? Offset(offsetParam) +& Limit(limitParam) =>
      respond_((offsetOrInvalid(offsetParam) |@| limitOrInvalid(limitParam)) { (offset, limit) =>
        val requestedFormat = MessageFormat.fromAccept(req.headers.get(Accept))
        download[S](requestedFormat, path, offset, limit)
      })

    case req @ POST -> AsFilePath(path) =>
      upload(req, W.appendThese(path, _))

    case req @ PUT -> AsFilePath(path) =>
      upload(req, W.saveThese(path, _))

    case req @ Method.MOVE -> AsPath(path) =>
      respond((for {
        dstStr <- EitherT.fromDisjunction[M.F](
                    requiredHeader(Destination, req) map (_.value))
        dst    <- EitherT.fromDisjunction[M.F](parseDestination(dstStr))
        scn    <- EitherT.fromDisjunction[M.F](moveScenario(path, dst))
        _      <- M.move(scn, MoveSemantics.FailIfExists)
                    .leftMap(_.toApiError)
      } yield Created).run)

    case DELETE -> AsPath(path) =>
      respond(M.delete(path).run)
  }

  ////

  private def download[S[_]](
    format: MessageFormat,
    path: APath,
    offset: Natural,
    limit: Option[Positive]
  )(implicit
    R: ReadFile.Ops[S],
    Q: QueryFile.Ops[S],
    S0: FileSystemFailure :<: S,
    S1: Task :<: S
  ): QResponse[S] =
    refineType(path).fold(
      dirPath => {
        val p = zippedContents[S](dirPath, format, offset, limit)
        val headers = `Content-Type`(MediaType.`application/zip`) :: format.disposition.toList
        QResponse.headers.modify(_ ++ headers)(QResponse.streaming(p))
      },
      filePath => formattedDataResponse(format, R.scan(filePath, offset, limit)))

  private def parseDestination(dstString: String): ApiError \/ APath = {
    def absPathRequired(rf: pathy.Path[Rel, _, _]) = ApiError.fromMsg(
      BadRequest withReason "Illegal move.",
      "Absolute path required for Destination.",
      "dstPath" := posixCodec.unsafePrintPath(rf)).left
    UriPathCodec.parsePath(
      absPathRequired,
      sandboxAbs(_).right,
      absPathRequired,
      sandboxAbs(_).right
    )(dstString)
  }

  private def moveScenario(src: APath, dst: APath): ApiError \/ MoveScenario =
    refineType(src).fold(
      srcDir =>
        refineType(dst).swap.bimap(
          df => ApiError.fromMsg(
            BadRequest withReason "Illegal move.",
            "Cannot move directory into a file",
            "srcPath" := srcDir,
            "dstPath" := df),
          MoveScenario.dirToDir(srcDir, _)),
      srcFile =>
        refineType(dst).bimap(
          dd => ApiError.fromMsg(
            BadRequest withReason "Illegal move.",
            "Cannot move a file into a directory, must specify destination precisely",
            "srcPath" := srcFile,
            "dstPath" := dd),
          // TODO: Why not move into directory if dst is a dir?
          MoveScenario.fileToFile(srcFile, _)))

  // TODO: Streaming
  private def upload[S[_]](
    req: Request,
    by: Vector[Data] => FileSystemErrT[Free[S,?], Vector[FileSystemError]]
  )(implicit S0: Task :<: S): Free[S, QResponse[S]] = {
    type FreeS[A] = Free[S, A]
    type FreeFS[A] = FileSystemErrT[FreeS, A]
    type QRespT[F[_], A] = EitherT[F, QResponse[S], A]

    def errorsResponse(
      decodeErrors: IndexedSeq[DecodeError],
      persistErrors: FreeFS[Vector[FileSystemError]]
    ): Free[S, QResponse[S]] =
      decodeErrors.toList.toNel
        .map(errs => respond_[S, ApiError](ApiError.apiError(
          BadRequest withReason "Malformed upload data.",
          "errors" := errs.map(_.shows))))
        .getOrElse(persistErrors.fold(_.toResponse[S], errs =>
          errs.toList.toNel.fold(QResponse.ok[S])(errs1 =>
            errs1.toApiError.copy(status = InternalServerError.withReason(
              "Error persisting uploaded data."
            )).toResponse[S])))

    def write(xs: IndexedSeq[(DecodeError \/ Data)]): Free[S, QResponse[S]] =
      if (xs.isEmpty) {
        respond_(ApiError.fromStatus(BadRequest withReason "Request has no body."))
      } else {
        val (errors, data) = xs.toVector.separate
        errorsResponse(errors, by(data))
      }

      free.injectFT[Task,S].apply(
        MessageFormat.decoder.decode(req,true)
          .leftMap(_.toResponse[S].point[FreeS])
          .flatMap(dataStream => EitherT.right(dataStream.runLog.map(write)))
          .merge
      ).flatMap(ι)
  }

  private def zippedContents[S[_]](
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
