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

package quasar.physical.marklogic.xcc

import slamdata.Predef._
import quasar.effect.Capture

import com.marklogic.xcc._
import scalaz._, Scalaz._

object contentsource {
  def defaultSession[F[_]: Bind: Capture: CSourceReader]: F[Session] =
    newSession[F](None)

  def newSession[F[_]: Bind: Capture: CSourceReader](
    defaultRequestOptions: Option[RequestOptions]
  ): F[Session] =
    CSourceReader[F].ask >>= (cs => Capture[F].capture {
      val session = cs.newSession
      defaultRequestOptions foreach session.setDefaultRequestOptions
      session
    })
}
