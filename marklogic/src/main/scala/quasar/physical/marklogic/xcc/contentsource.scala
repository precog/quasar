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

package quasar.physical.marklogic.xcc

import quasar.Predef._
import quasar.effect.Capture
import quasar.fp.numeric.Positive

import com.marklogic.xcc._
import scalaz._, Scalaz._

object contentsource {
  def defaultSession[F[_]: Bind: Capture: CSourceReader]: F[Session] =
    newSession[F](None)

  def newSession[F[_]: Bind](
    defaultRequestOptions: Option[RequestOptions]
  )(implicit
    C: Capture[F],
    R: CSourceReader[F]
  ): F[Session] =
    R.ask >>= (cs => C.delay {
      val session = cs.newSession
      defaultRequestOptions foreach session.setDefaultRequestOptions
      session
    })

  def resultCursor[F[_]: Bind: Capture: CSourceReader](chunkSize: Positive)(f: Session => F[QueryResults]): F[ResultCursor] =
    newSession[F](some(streamingOptions)) >>= (s =>
      Capture[F].delay(f(s)).join map (qr => new ResultCursor(chunkSize, s, qr.resultSequence)))

  ////

  private def streamingOptions: RequestOptions = {
    val opts = new RequestOptions
    opts.setCacheResult(false)
    opts
  }
}
