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

package quasar.api.services.query

import quasar.Predef._
import quasar._
import quasar.api._
import quasar.api.ToApiError.ops._
import quasar.api.services._
import quasar.fp._
import quasar.fs._
import quasar.sql.{Expr, Query}

import argonaut._, Argonaut._
import org.http4s.headers.Accept
import org.http4s._
import org.http4s.dsl._
import pathy.Path, Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object execute {

  def service[S[_]: Functor](implicit S0: QueryFileF :<: S,
                                      S1: Task :<: S,
                                      S2: FileSystemFailureF :<: S): QHttpService[S] = {
    val Q = QueryFile.Ops[S]

    val removePhaseResults = new (FileSystemErrT[PhaseResultT[Free[S,?], ?], ?] ~> FileSystemErrT[Free[S,?], ?]) {
      def apply[A](t: FileSystemErrT[PhaseResultT[Free[S,?], ?], A]): FileSystemErrT[Free[S,?], A] =
        EitherT[Free[S,?],FileSystemError,A](t.run.value)
    }

    def destinationFile(fileStr: String): ApiError \/ (Path[Abs,File,Unsandboxed] \/ Path[Rel,File,Unsandboxed]) = {
      val err = -\/(ApiError.apiError(
        BadRequest withReason "Destination must be a file.",
        "destination" := fileStr))

      posixCodec.parsePath(relFile => \/-(\/-(relFile)), absFile => \/-(-\/(absFile)), κ(err), κ(err))(fileStr)
    }

    QHttpService {
      case req @ GET -> _ :? Offset(offset) +& Limit(limit) =>
        respond_(parsedQueryRequest(req, offset, limit) map { case (xpr, off, lim) =>
          queryPlan(addOffsetLimit(xpr, off, lim), requestVars(req))
            .run.value map (lp => formattedDataResponse(
              MessageFormat.fromAccept(req.headers.get(Accept)),
              Q.evaluate(lp).translate[FileSystemErrT[Free[S, ?], ?]](removePhaseResults)))
        })

      case req @ POST -> AsDirPath(path) =>
        free.lift(EntityDecoder.decodeString(req)).into[S] flatMap { query =>
          if (query.isEmpty) {
            respond_(bodyMustContainQuery)
          } else {
            respond(requiredHeader(Destination, req) flatMap { destination =>
              val parseRes: ApiError \/ Expr =
                sql.parseInContext(Query(query), path)
                  .leftMap(_.toApiError)

              val absDestination: ApiError \/ AFile =
                destinationFile(destination.value) map (res =>
                  sandboxAbs(res.map(unsandbox(path) </> _).merge))

              parseRes tuple absDestination
            } traverseU { case (expr, out) =>
              Q.executeQuery(expr, requestVars(req), out).run.run.run map {
                case (phases, result) =>
                  result.leftMap(_.toApiError).flatMap(_.leftMap(_.toApiError))
                    .bimap(_ :+ ("phases" := phases), f => Json(
                      "out"    := posixCodec.printPath(f),
                      "phases" := phases
                    ))
              }
            })
          }
        }
    }
  }

}
