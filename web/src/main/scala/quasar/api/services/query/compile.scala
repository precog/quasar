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
import quasar._, api._, fs._
import quasar.api.services._
import quasar.api.ToQResponse.ops._
import quasar.fp._
import quasar.fp.numeric._
import quasar.sql.{ParsingError}

import argonaut._, Argonaut._
import matryoshka.Fix
import org.http4s.dsl._
import scalaz._, Scalaz._

object compile {

  def service[S[_]: Functor](implicit Q: QueryFile.Ops[S], M: ManageFile.Ops[S]): QHttpService[S] = {
    def phaseResultsResponse(prs: PhaseResults): Option[QResponse[S]] =
      prs.lastOption map {
        case PhaseResult.Tree(name, value)   => Json(name := value).toResponse
        case PhaseResult.Detail(name, value) => QResponse.string(Ok, name + "\n" + value)
      }

    def explainQuery(
      expr: sql.Expr, offset: Option[Natural], limit: Option[Positive], vars: Variables): Free[S, QResponse[S]] = respond(
        queryPlan(addOffsetLimit(expr, offset, limit), vars).run.value
          .traverse[Free[S, ?], SemanticErrors, QResponse[S]](lp =>
            Q.explain(lp).run.run.map {
              case (phases, \/-(_)) =>
                phaseResultsResponse(phases)
                  .getOrElse(QResponse.error(InternalServerError,
                    s"No explain output for plan: \n\n" + RenderTree[Fix[LogicalPlan]].render(lp).shows))
              case (_, -\/(fsErr)) => fsErr.toResponse[S]
            }))

    QHttpService {
      case req @ GET -> _ :? Offset(offset) +& Limit(limit) => respond(
        parseQueryRequest[S](req, offset, limit).traverse[Free[S,?], QResponse[S], ParsingError \/ QResponse[S]] {
          case (path, query, offset, limit) =>
            sql.parseInContext(query, path).traverse[Free[S, ?], ParsingError, QResponse[S]](
              expr => explainQuery(expr, offset, limit, vars(req)))
        })
    }
  }

}
