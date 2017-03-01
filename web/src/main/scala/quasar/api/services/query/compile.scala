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

import quasar.Predef.{ -> => _, _ }
import quasar._, RenderTree.ops._
import quasar.api._, ToQResponse.ops._
import quasar.api.services._
import quasar.common._
import quasar.contrib.pathy._
import quasar.fp.ski._
import quasar.fp.numeric._
import quasar.frontend._
import quasar.fs._
import quasar.frontend.logicalplan.{LogicalPlan, LogicalPlanR}

import argonaut._, Argonaut._
import matryoshka._
import matryoshka.data.Fix
import org.http4s.dsl._
import pathy.Path.posixCodec
import scalaz._, Scalaz._

object compile {
  private val lpr = new LogicalPlanR[Fix[LogicalPlan]]

  def service[S[_]](
    implicit
    Q: QueryFile.Ops[S],
    M: ManageFile.Ops[S]
  ): QHttpService[S] = {
    def phaseResultsResponse(prs: PhaseResults): Option[Json] =
      prs.lastOption.map(PhaseResult.asJson)

    def noOutputError(lp: Fix[LogicalPlan]): ApiError =
      ApiError.apiError(
        InternalServerError withReason "No explain output for plan.",
        "logicalplan" := lp.render)

    def explainQuery(
      expr: Fix[sql.Sql],
      vars: Variables,
      basePath: ADir,
      offset: Natural,
      limit: Option[Positive]
    ): Free[S, QResponse[S]] =
      respond(queryPlan(expr, vars, basePath, Nil, offset, limit)
        .run.value.traverse[Free[S, ?], SemanticErrors, QResponse[S]](_.fold(
          κ(Json(
            "physicalPlan" -> jNull,
            "inputs"       := List.empty[String]).toResponse[S].point[Free[S, ?]]),
          lp => Q.explain(lp).run.run.map {
            case (phases, \/-(_)) =>
              phaseResultsResponse(phases)
                .map(physicalPlanJson =>
                  Json(
                    "physicalPlan" := physicalPlanJson,
                    "inputs"       := lpr.paths(lp).map(posixCodec.printPath)
                  ).toResponse[S])
                .toRightDisjunction(noOutputError(lp))
                .toResponse[S]
            case (_, -\/(fsErr)) => fsErr.toResponse[S]
          })))

    QHttpService {
      case req @ GET -> _ :? Offset(offset) +& Limit(limit) =>
        respond(parsedQueryRequest(req, offset, limit) traverse {
          case (expr, basePath, offset, limit) =>
            explainQuery(expr, requestVars(req), basePath, offset, limit)
        })
    }
  }

}
