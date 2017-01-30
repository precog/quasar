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

import com.marklogic.xcc.{ResultItem, ResultSequence, Session}
import com.marklogic.xcc.types.XdmItem
import scalaz._, Scalaz._

final class ResultCursor private[xcc] (val chunkSize: Positive, s: Session, rs: ResultSequence) {
  def close[F[_]: Capture: Functor]: F[Executed] =
    Capture[F].delay(s.close()).as(Executed.executed)

  def nextChunk[F[_]: Capture: Monad]: F[Vector[XdmItem]] = {
    def nextChunk0(size: Long, xs: Vector[XdmItem]): F[Vector[XdmItem]] =
      if (size === 0) xs.point[F]
      else next >>= (_.cata(x => nextChunk0(size - 1, xs :+ x), xs.point[F]))

    nextChunk0(chunkSize.get, Vector())
  }

  ////

  private def next[F[_]: Capture: Monad]: F[Option[XdmItem]] =
    nextItem >>= (_ traverse resultitem.loadItem[F])

  private def nextItem[F[_]: Capture]: F[Option[ResultItem]] =
    Capture[F].delay(if (rs.hasNext) Some(rs.next) else None)
}
