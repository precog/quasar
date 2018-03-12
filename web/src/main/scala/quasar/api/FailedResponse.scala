/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.api

import slamdata.Predef.{RuntimeException, Throwable}
import quasar.effect.Failure
import quasar.api.ToQResponse.ops._

import org.http4s.Response
import scalaz.{~>, EitherT, NaturalTransformation, Monad, Need, Show}

final class FailedResponse private (throwable0: Need[Throwable], response0: Need[Response]) {
  def toResponse: Response = response0.value
  def toThrowable: Throwable = throwable0.value
}

object FailedResponse {
  def apply(t: => Throwable, r: => Response): FailedResponse =
    new FailedResponse(Need(t), Need(r))

  /** Interpret a `Failure` effect into `FailedResponseOr` given evidence the
    * failure type can be converted to a `QResponse`.
    */
  def fromFailure[F[_]: Monad, E](f: E => Throwable)(implicit E: ToQResponse[E, FailedResponseOr])
      : Failure[E, ?] ~> FailedResponseT[F, ?] = {

    def errToResp(e: E): FailedResponse =
      FailedResponse(
        f(e),
        e.toResponse[FailedResponseOr].toHttpResponse(NaturalTransformation.refl))

    Failure.toError[EitherT[F, FailedResponse, ?], FailedResponse]
      .compose(Failure.mapError(errToResp))
  }

  def fromFailureMessage[F[_]: Monad, E: Show](implicit E: ToQResponse[E, FailedResponseOr])
      : Failure[E, ?] ~> FailedResponseT[F, ?] =
    fromFailure[F, E](e => new RuntimeException(Show[E].shows(e)))
}
