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

package quasar.api.services.query

import slamdata.Predef.{ -> => _, _ }
import quasar._
import quasar.effect.{ExecutionId, ScopeExecution}
import quasar.api._, ToApiError.ops._
import quasar.api.services._
import quasar.contrib.pathy._
import quasar.fp._
import quasar.fp.ski._
import quasar.fp.numeric._
import quasar.fs._
import quasar.fs.mount.Mounting
import quasar.main.FilesystemQueries

import argonaut._, Argonaut._
import org.http4s._
import org.http4s.dsl._
import org.http4s.headers.Accept
import pathy.Path, Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object execute {

  // QScriptCore :<: F ===> ThetaJoin :+: F => EquiJoin :+: F

  def service[S[_], T](executionIdRef: TaskRef[Long])(
    implicit
    W: WriteFile :<: S,
    Q: QueryFile.Ops[S],
    M: ManageFile :<: S,
    S1: Task :<: S,
    S2: FileSystemFailure :<: S,
    S3: Mounting :<: S,
    SE: ScopeExecution[Free[S, ?], T]
  ): QHttpService[S] = {
    val fsQ = new FilesystemQueries[S]
    val xform = QueryFile.Transforms[Free[S, ?]]

    QHttpService {
      case req @ GET -> _ :? Offset(offset) +& Limit(limit) =>
        respond(parsedQueryRequest(req, offset, limit) traverse { case (xpr, basePath, off, lim) =>
          // FIXME: use fsQ.evaluateQuery here
          Free.roll(S1(ExecutionId.ofRef(executionIdRef).map(SE.newExecution(_, ST =>
            for {
              result <- ST.newScope("total query pipeline (GET)",
                for {
                  block <- ST.newScope("resolve imports", resolveImports[S](xpr, basePath).run)
                  lpOrSemanticErr <-
                    ST.newScope("plan query",
                        block.leftMap(_.wrapNel).flatMap(block =>
                        queryPlan(block, requestVars(req), basePath, off, lim)
                        .run.value).point[Free[S, ?]]
                    )
                  evaluated <- ST.newScope("evaluate query",
                    lpOrSemanticErr traverse (lp => formattedDataResponse(
                      MessageFormat.fromAccept(req.headers.get(Accept)),
                      Q.evaluate(lp).translate(xform.dropPhases))))
                } yield evaluated)
            } yield result
          ))))
      })

      case req @ POST -> AsDirPath(path) =>
        def destinationFile(fileStr: String): ApiError \/ AFile = {
          val err = -\/(ApiError.apiError(
            BadRequest withReason "Destination must be a file.",
            "destination" := transcode(UriPathCodec, posixCodec)(fileStr)))
          for {
            relOrAbs <- UriPathCodec.parsePath(relFile => \/-(\/-(relFile)), absFile => \/-(-\/(absFile)), κ(err), κ(err))(fileStr)
          } yield unsafeSandboxAbs(relOrAbs.map(unsandbox(path) </> _).merge)
        }
        free.lift(EntityDecoder.decodeString(req)).into[S] flatMap { query =>
          if (query.isEmpty) {
            respond_(bodyMustContainQuery)
          } else {
             respond(Free.roll(S1(ExecutionId.ofRef(executionIdRef).map(SE.newExecution(_, ST =>
              (for {
                destination <- EitherT(requiredHeader(Destination, req).pure[Free[S, ?]])
                parsed <- EitherT(ST.newScope("parse SQL", sql.fixParser.parse(query).leftMap(_.toApiError).pure[Free[S, ?]]))
                out <- EitherT(destinationFile(destination.value).pure[Free[S, ?]])
                basePath <- EitherT(decodedDir(req.uri.path).pure[Free[S, ?]])
                resolved <- EitherT(ST.newScope("resolve imports", resolveImports(parsed, basePath).leftMap(_.toApiError).run))
                executed <- EitherT(ST.newScope("execute query", fsQ.executeQuery(resolved, requestVars(req), basePath, out).run.run.run map {
                  case (phases, result) =>
                    result
                    .leftMap(_.toApiError)
                    .flatMap(_.leftMap(_.toApiError))
                    .bimap(
                      _ :+ ("phases" := phases),
                      κ(Json(
                      "out"   := posixCodec.printPath(out),
                      "phases" := phases)))
                }))
              } yield executed).run
            )))))
          }
        }
    }
  }

}
