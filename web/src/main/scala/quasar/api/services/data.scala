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

import quasar.Predef.{ -> => _, _ }
import quasar.Data
import quasar.api._, ToQResponse.ops._, ToApiError.ops._
import quasar.contrib.pathy._
import quasar.fp._, numeric._
import quasar.fs._

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
      upload(req, path, W.appendThese(_, _))

    case req @ PUT -> AsPath(path) =>
      upload(req, path, W.saveThese(_, _))

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
        val headers =
          `Content-Type`(MediaType.`application/zip`) ::
            (format.disposition.toList: List[Header])
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
    path: APath,
    by: (AFile, Vector[Data]) => FileSystemErrT[Free[S,?], Vector[FileSystemError]]
  )(implicit S0: Task :<: S): Free[S, QResponse[S]] = {
    type FreeS[A] = Free[S, A]

    val inj = free.injectFT[Task, S]
    def hoist = Hoist[EitherT[?[_], QResponse[S], ?]].hoist(inj)
    def decodeUtf8(bytes: ByteVector): EitherT[FreeS, QResponse[S], String] =
      EitherT(bytes.decodeUtf8.disjunction.point[FreeS])
        .leftMap(err => InvalidMessageBodyFailure(err.toString).toResponse[S])

    def errorsResponse(
      decodeErrors: IndexedSeq[DecodeError],
      persistErrors: FileSystemErrT[FreeS, Vector[FileSystemError]]
    ): OptionT[FreeS, QResponse[S]] =
      OptionT(decodeErrors.toList.toNel.map(errs =>
          respond_[S, ApiError](ApiError.apiError(
            BadRequest withReason "Malformed upload data.",
            "errors" := errs.map(_.shows)))).sequence)
        .orElse(OptionT(persistErrors.fold[Option[QResponse[S]]](
          _.toResponse[S].some,
          errs => errs.toList.toNel.map(errs1 =>
            errs1.toApiError.copy(status = InternalServerError.withReason(
              "Error persisting uploaded data."
            )).toResponse[S]))))

    def write(fPath: AFile, xs: IndexedSeq[(DecodeError \/ Data)]): FreeS[QResponse[S] \/ Unit] =
      if (xs.isEmpty) {
         respond_[S, ApiError](ApiError.fromStatus(BadRequest withReason "Request has no body.")).map(_.left)
      } else {
        val (errors, data) = xs.toVector.separate
        errorsResponse(errors, by(fPath, data)).toLeft(()).run
      }

    def decodeContent(format: MessageFormat, strs: Process[Task, String])
        : EitherT[Task, DecodeFailure, Process[Task, DecodeError \/ Data]] =
      EitherT(format.decode(strs).map(_.leftMap(err => InvalidMessageBodyFailure(err.msg): DecodeFailure)))

    def handleOne(fPath: AFile, fmt: MessageFormat, strs: Process[Task, String])
        : EitherT[FreeS, QResponse[S], Unit] = {
      hoist(decodeContent(fmt, strs).leftMap(_.toResponse[S]))
        .flatMap(dataStream => EitherT(inj(dataStream.runLog).flatMap(write(fPath, _))))
    }

    refineType(path).fold[EitherT[FreeS, QResponse[S], Unit]](
      aDir =>
        for {
          files <- hoist(Zip.unzipFiles(req.body)
                   .leftMap(err => InvalidMessageBodyFailure(err).toResponse[S]))
          t <- files.partition(_._1 ≟ ArchiveMetadata.HiddenFile) match {
            case ((_, meta) :: Nil, other) =>
              decodeUtf8(meta).strengthR(other)
            case _ =>
              ???
              // EitherT.left(InvalidMessageBodyFailure("metadata not found: " + posixCodec.printPath(ArchiveMetadata.HiddenFile)).toResponse[S])
          }
          (meta, other) = t
          _     <- other.traverse { case (aFile, bytes) =>
                   // EitherT(bytes.decodeUtf8.disjunction.point[FreeS])
                   //   .leftMap(err => InvalidMessageBodyFailure(err.toString).toResponse[S])
                   decodeUtf8(bytes)
                     .flatMap(str => handleOne(rebaseA(aDir)(aFile), fmt, Process.emit(str)))
                  }
        } yield (),
      aFile => for {
        fmt <- EitherT(MessageFormat.forMessage(req).point[FreeS]).leftMap(_.toResponse[S])
        _   <- handleOne(aFile, fmt, req.bodyAsText)
      } yield ()).as(QResponse.ok[S]).run.map(_.merge)
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
        (rootDir </> file, bytes)
      })
    }
}
