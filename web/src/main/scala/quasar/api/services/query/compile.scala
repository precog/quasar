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
import quasar._, api._, fs._
import quasar.api.services._
import quasar.api.ToQuasarResponse.ops._
import quasar.fs.{Path => QPath}
import quasar.recursionschemes.Fix
import quasar.sql.{ParsingError, SQLParser}

import argonaut._, Argonaut._
import org.http4s.dsl._
import scalaz._, Scalaz._

object compile {

  def service[S[_]: Functor](implicit Q: QueryFile.Ops[S], M: ManageFile.Ops[S]): QHttpService[S] = {
    def phaseResultsResponse(prs: PhaseResults): Option[QuasarResponse[S]] =
      prs.lastOption map {
        case PhaseResult.Tree(name, value)   => Json(name := value).toResponse
        case PhaseResult.Detail(name, value) => QuasarResponse.string(Ok, name + "\n" + value)
      }

    def explainQuery(
      expr: sql.Expr, offset: Option[Natural], limit: Option[Positive], vars: Variables): Free[S, QuasarResponse[S]] = respond(
        queryPlan(addOffsetLimit(expr, offset, limit), vars).run.value
          .traverse[Free[S, ?], SemanticErrors, QuasarResponse[S]](lp =>
            Q.explain(lp).run.run.map {
              case (phases, \/-(_)) =>
                phaseResultsResponse(phases)
                  .getOrElse(QuasarResponse.error(InternalServerError,
                    s"No explain output for plan: \n\n" + RenderTree[Fix[LogicalPlan]].render(lp).shows))
              case (_, -\/(fsErr)) => fsErr.toResponse[S]
            }))

    QHttpService {
      case req @ GET -> AsDirPath(path) :? QueryParam(query) +& Offset(offset) +& Limit(limit) => respond(
        offsetOrInvalid[S](offset).tuple(limitOrInvalid[S](limit))
          .traverse[Free[S, ?], QuasarResponse[S], ParsingError \/ QuasarResponse[S]] { case (offset, limit) =>
            SQLParser.parseInContext(query, QPath.fromAPath(path))
              .traverse[Free[S, ?], ParsingError, QuasarResponse[S]](expr =>
                explainQuery(expr, offset, limit, vars(req)))
        })
      case GET -> _ => queryParameterMustContainQuery
    }
  }

}
