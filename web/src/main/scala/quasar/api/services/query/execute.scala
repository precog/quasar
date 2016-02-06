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

package quasar.api.services.query

import quasar.Predef._
import quasar._
import quasar.api._
import quasar.api.services._
import quasar.api.ToQuasarResponse.ops._
import quasar.{Variables, fs}
import quasar.fp._
import quasar.fs.{Path => QPath, _}
import quasar.recursionschemes.Fix
import quasar.sql.{ParsingPathError, ParsingError, SQLParser, Query}

import argonaut._, Argonaut._
import org.http4s.headers.Accept
import org.http4s._
import org.http4s.dsl._
import org.http4s.headers.`Content-Type`
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object execute {

  def service[S[_]: Functor](implicit S0: QueryFileF :<: S,
                                      S1: Task :<: S,
                                      S2: FileSystemFailureF :<: S): QHttpService[S] = {

    val removePhaseResults = new (FileSystemErrT[PhaseResultT[Free[S,?], ?], ?] ~> FileSystemErrT[Free[S,?], ?]) {
      def apply[A](t: FileSystemErrT[PhaseResultT[Free[S,?], ?], A]): FileSystemErrT[Free[S,?], A] =
        EitherT[Free[S,?],FileSystemError,A](t.run.value)
    }

    val Q = QueryFile.Ops[S]

    QHttpService {
      case req @ GET -> AsDirPath(path) :? QueryParam(query) +& Offset(offset) +& Limit(limit) => respond(
        (offsetOrInvalid[S](offset) |@| limitOrInvalid[S](limit)) { (offset, limit) =>
          SQLParser.parseInContext(query, QPath.fromAPath(path)).map(
            expr => queryPlan(addOffsetLimit(expr, offset, limit), vars(req)).run.value.map(
              logicalPlan => {
                val requestedFormat = MessageFormat.fromAccept(req.headers.get(Accept))
                formattedDataResponse(
                  requestedFormat,
                  Q.evaluate(logicalPlan).translate[FileSystemErrT[Free[S, ?], ?]](removePhaseResults))
              }
            )
          )
        }.point[Free[S, ?]]
      )
      case GET -> _ => queryParameterMustContainQuery[S]
      case req @ POST -> AsDirPath(path) =>
        Free.liftF(S1.inj(EntityDecoder.decodeString(req))).flatMap { query =>
          if (query == "") postContentMustContainQuery[S]
          else {
            respond(requiredHeader[S](Destination, req)
              .traverse[Free[S, ?], QuasarResponse[S], QuasarResponse[S]] { destination =>
                val parseRes = SQLParser.parseInContext(Query(query), QPath.fromAPath(path))
                  .leftMap(_.toResponse[S])
                val destinationFile = posixCodec.parsePath(
                  relFile => \/-(\/-(relFile)),
                  absFile => \/-(-\/(absFile)),
                  relDir => -\/(QuasarResponse.error[S](BadRequest, "Destination must be a file")),
                  absDir => -\/(QuasarResponse.error[S](BadRequest, "Destination must be a file")))(destination.value)
                // Add path of query if destination is a relative file or else just jump through Sandbox hoop
                val absDestination: QuasarResponse[S] \/ AFile = destinationFile.flatMap(f =>
                  sandbox(rootDir, f.fold(ι, relFile => unsandbox(path) </> relFile)).map(rootDir </> _) \/>
                    QuasarResponse.error[S](BadRequest, "Destination file is invalid"))
                respond(parseRes.tuple(absDestination)
                  .traverse[Free[S, ?], QuasarResponse[S], SemanticErrors \/ (FileSystemError \/ Json)] {
                    case (expr, out) =>
                      Q.executeQuery(expr, vars(req), out).run.run.run map { case (phases, result) =>
                        result.map(_.map(rfile => Json(
                          "out"    := posixCodec.printPath(rfile),
                          "phases" := phases)))
                      }
                  })
              })
          }
        }
    }
  }

}
