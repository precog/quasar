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
import quasar.api._
import quasar.fp._
import quasar.fs._
import quasar.fs.ManageFile.{MoveSemantics, MoveScenario}
import quasar.Predef._
import quasar.repl.Prettify

import java.nio.charset.StandardCharsets

import argonaut.Argonaut._
import argonaut.Json
import org.http4s._
import org.http4s.argonaut._
import org.http4s.dsl._
import org.http4s.headers.{`Content-Type`, Accept}
import org.http4s.server._
import pathy.Path._, posixCodec._
import scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process
import scalaz.syntax.monad._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.TraverseOps
import scodec.bits.ByteVector

object data {

  // TODO: remove f
  def service[S[_]: Functor](f: S ~> Task)(implicit R: ReadFile.Ops[S],
                                                    W: WriteFile.Ops[S],
                                                    M: ManageFile.Ops[S],
                                                    Q: QueryFile.Ops[S],
                                                    S0: Task :<: S,
                                                    S1: FileSystemFailure :<: S): HttpService = {

    def download(format: MessageFormat, path: APath, offset: Natural, limit: Option[Positive])
      : QuasarResponse[S] =
      refineType(path).fold(
        dirPath => {
          val p = zippedBytes[S](dirPath, format, offset, limit)
          val headers = `Content-Type`(MediaType.`application/zip`) :: format.disposition.toList
          QuasarResponse.headers.modify(_.put(headers: _*))(QuasarResponse.streaming(p))
        },
        filePath => {
          val p = format.encode[FileSystemErrT[Free[S,?], ?]](R.scan(filePath, offset, limit))
          val headers = `Content-Type`(format.mediaType, Some(Charset.`UTF-8`)) :: format.disposition.toList
          QuasarResponse.headers.modify(_.put(headers: _*))(QuasarResponse.streaming(p))
        })

    def upload(req: Request,
               by: Process[Free[S,?], Data] => Process[FileSystemErrT[Free[S,?],?], FileSystemError]): Task[Response] = {
      handleMissingContentType(
        req.decode[Process[Task,DecodeError \/ Data]] { data =>
          for {
            all <- data.runLog
            result <- if (all.isEmpty) BadRequest("Request has no body")
            else {
              val (errors, cleanData) = unzipDisj(all.toList)
              responseForUpload(errors, convert[S, FileSystemError](f)(by(Process.emitAll(cleanData))).runLog.map(_.toList))
            }
          } yield result
        }
      )
    }

    HttpService {
      case req @ GET -> AsPath(path) :? Offset(offsetParam) +& Limit(limitParam) => ???
      // {
      //   handleOffsetLimitParams(offsetParam,limitParam){ (offset, limit) =>
      //     val requestedFormat = MessageFormat.fromAccept(req.headers.get(Accept))
      //     download(requestedFormat, path, offset.getOrElse(Natural._0), limit)
      //   }
      // }
      case req @ POST -> AsFilePath(path) => upload(req, W.append(path,_))
      case req @ PUT -> AsFilePath(path) => upload(req, W.save(path,_))
      case req @ Method.MOVE -> AsPath(path) =>
        requiredHeader(Destination, req).map { destPathString =>
            val scenarioOrProblem = refineType(path).fold(
              src => parseAbsDir(destPathString.value).map(sandboxAbs).map(
                dest => MoveScenario.DirToDir(src, dest)) \/> "Cannot move directory into a file",
              src => parseAbsFile(destPathString.value).map(sandboxAbs).map(
                dest => MoveScenario.FileToFile(src, dest)) \/> "Cannot move a file into a directory, must specify destination precisely"
            )
            scenarioOrProblem.map{ scenario =>
              val response = M.move(scenario, MoveSemantics.FailIfExists).fold(fileSystemErrorResponse,_ => Created(""))
              hoistFree(f).apply(response).join
            }.leftMap(BadRequest(_)).merge
          }.merge

      case DELETE -> AsPath(path) =>
        val response = M.delete(path).fold(fileSystemErrorResponse, _ => Ok(""))
        hoistFree(f).apply(response).join
    }
  }

  def zippedBytes[S[_]: Functor](dir: AbsDir[Sandboxed], format: MessageFormat, offset: Natural, limit: Option[Positive])
                 (implicit R: ReadFile.Ops[S], Q: QueryFile.Ops[S]): Process[R.M, ByteVector] = {
    Process.await(Q.descendantFiles(dir)) { files =>
      val filesAndBytes = files.toList.map { file =>
        val data = R.scan(dir </> file, offset, limit)
        val bytes = format.encode(data).map(str => ByteVector.view(str.getBytes(StandardCharsets.UTF_8)))
        (file, bytes)
      }
      quasar.api.Zip.zipFiles(filesAndBytes)
    }
  }

  def handleMissingContentType(response: Task[Response]) =
    response.handleWith{ case MessageFormat.UnsupportedContentType =>
      UnsupportedMediaType("No media-type is specified in Content-Type header")
        .withContentType(Some(`Content-Type`(MediaType.`text/plain`)))
    }

  private def responseForUpload[A](decodeErrors: List[DecodeError], persistErrors: FilesystemTask[List[FileSystemError]]): Task[Response] = {
    def dataErrorBody[A: Show](status: org.http4s.dsl.impl.EntityResponseGenerator, errs: List[A]) =
      status(Json(
        "error" := "some uploaded value(s) could not be processed",
        "details" := Json.array(errs.map(e => jString(e.shows)): _*)))

    if (decodeErrors.nonEmpty)
      dataErrorBody(BadRequest, decodeErrors)
    else
      persistErrors.run.flatMap(_.fold(
        fileSystemErrorResponse,
        errors => if(errors.isEmpty) Ok("") else dataErrorBody(InternalServerError, errors)))
  }

  implicit val dataDecoder: EntityDecoder[Process[Task,DecodeError \/ Data]] =
    MessageFormat.decoder orElse EntityDecoder.error(MessageFormat.UnsupportedContentType)
}
