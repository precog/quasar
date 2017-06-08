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

package quasar.api.services

import slamdata.Predef.{ -> => _, _ }
import quasar.api._
import quasar.api.ApiError._
import quasar.api.ToQResponse._
import quasar.api.ToQResponse.ops._
import quasar.contrib.pathy._
import quasar.effect.Failure
import quasar.fp._, numeric._
import quasar.fs.mount.module.Module

import org.http4s.dsl._
import org.http4s.headers.Accept
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object invoke {

  def service[S[_]](
    implicit
    I: Module.Ops[S],
    S0: Task :<: S,
    S1: Failure[Module.Error, ?] :<: S
  ): QHttpService[S] = QHttpService {

    case req @ GET -> AsPath(path) :? Offset(offsetParam) +& Limit(limitParam) =>
      respond_ {
        (offsetOrInvalid(offsetParam) |@| limitOrInvalid(limitParam)) { (offset, limit) =>
          refineType(path).fold(
            dir => apiError(BadRequest withReason "Path must be a file").toResponse[S],
            file => {
              val requestedFormat = MessageFormat.fromAccept(req.headers.get(Accept))
              invoke[S](requestedFormat, file, req.params, offset, limit)
            })
        }
      }
  }

  ////

  private def invoke[S[_]](
    format: MessageFormat,
    filePath: AFile,
    args: Map[String, String],
    offset: Natural,
    limit: Option[Positive]
  )(implicit
    I: Module.Ops[S],
    S0: Failure[Module.Error, ?] :<: S,
    S1: Task :<: S
  ): QResponse[S] =
      formattedDataResponse(format, I.invokeFunction(filePath, args, offset, limit))
}
