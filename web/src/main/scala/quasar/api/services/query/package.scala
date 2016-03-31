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

import quasar.Predef._
import quasar._, api._
import quasar.fp.numeric._
import quasar.fs._
import quasar.sql.Query

import org.http4s._, dsl._
import scalaz._, Scalaz._

package object query {

  implicit val QueryDecoder = new QueryParamDecoder[Query] {
    def decode(value: QueryParameterValue): ValidationNel[ParseFailure, Query] =
      Query(value.value).successNel[ParseFailure]
  }

  // https://github.com/puffnfresh/wartremover/issues/149
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  object QueryParam extends QueryParamDecoderMatcher[Query]("q")

  def queryOrBadRequest[S[_]](params: Map[String, scala.collection.Seq[String]]): QResponse[S] \/ Query =
    params.get("q").cata(
      values =>
        if (values.size > 1) QResponse.error(BadRequest, "The request must contain only one query parameter").left
        else values.headOption.map(Query(_)).toRightDisjunction(QResponse.error(BadRequest, "The request must contain the query parameter")),
      QResponse.error(BadRequest, "Request must contain a query").left
    )

  def parseQueryRequest[S[_]](req: Request,
                        offset: Option[ValidationNel[ParseFailure, Natural]],
                        limit: Option[ValidationNel[ParseFailure, Positive]])
    : QResponse[S] \/ (ADir, Query, Option[Natural], Option[Positive]) =
    (dirPathOrBadRequest[S](req.uri.path)  |@|
     queryOrBadRequest[S](req.multiParams) |@|
     offsetOrInvalid[S](offset)         |@|
     limitOrInvalid[S](limit)) { (dir, query, offset, limit) => (dir, query, offset, limit)}

  def queryParameterMustContainQuery[S[_]] =
    Free.pure[S, QResponse[S]](QResponse.error(BadRequest, "The request must contain a query"))

  def postContentMustContainQuery[S[_]] =
    Free.pure[S, QResponse[S]](QResponse.error(BadRequest, "The body of the POST must contain a query"))

  private val VarPrefix = "var."
  def vars(req: Request) = Variables(req.params.collect {
    case (k, v) if k.startsWith(VarPrefix) => (VarName(k.substring(VarPrefix.length)), VarValue(v)) })

  def addOffsetLimit(query: sql.Expr, offset: Option[Natural], limit: Option[Positive]): sql.Expr = {
    val skipped = offset.fold(query)(o => sql.Binop(query, sql.IntLiteral(o.get), sql.Offset))
    limit.fold(skipped)(l => sql.Binop(skipped, sql.IntLiteral(l.get), sql.Limit))
  }

}
