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
import quasar._
import quasar.api._, ToApiError.ops._
import quasar.api.services._
import quasar.contrib.pathy._
import quasar.fp.numeric._
import quasar.fs._
import quasar.frontend.logicalplan.{LogicalPlan, LogicalPlanR}

import argonaut._, Argonaut._
import matryoshka._
import matryoshka.data.Fix
import org.http4s.dsl._
import scalaz._, Scalaz._

object compile {
  private val lpr = new LogicalPlanR[Fix[LogicalPlan]]

  def service[S[_]](
    implicit
    Q: QueryFile.Ops[S],
    M: ManageFile.Ops[S]
  ): QHttpService[S] = {
    def constantResponse(data: List[Data]): Json =
      Json(
        "type"  := "constant",
        "value" := data.map(DataCodec.Precise.encode).unite)

    def explainQuery(
      blob: sql.Blob[Fix[sql.Sql]],
      vars: Variables,
      basePath: ADir,
      offset: Natural,
      limit: Option[Positive]
    ): Free[S, ApiError \/ Json] =
      queryPlan(blob, vars, basePath, offset, limit)
        .run.value
        .traverse(_.fold(
          data => constantResponse(data).right[ApiError].point[Free[S, ?]],
          lp   => Q.explain(lp).run.value.map(_.bimap(_.toApiError, _.asJson))))
        .map(_.valueOr(_.toApiError.left[Json]))

    QHttpService {
      case req @ GET -> _ :? Offset(offset) +& Limit(limit) =>
        respond(parsedQueryRequest(req, offset, limit) traverse {
          case (expr, basePath, offset, limit) =>
            explainQuery(expr, requestVars(req), basePath, offset, limit)
        })
    }
  }

}
