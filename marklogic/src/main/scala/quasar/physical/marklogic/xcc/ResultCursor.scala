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
import quasar.fp.numeric.Positive

import com.marklogic.xcc.{ResultItem, ResultSequence, Session}
import com.marklogic.xcc.types.XdmItem
import scalaz._, Scalaz._
import scalaz.stream.Process
import scalaz.concurrent.Task

final class ResultCursor private[xcc] (val chunkSize: Positive, s: Session, rs: ResultSequence) {

  def close: Task[Executed] =
    Task.delay(s.close()).as(Executed.executed)

  def nextChunk: Task[Vector[XdmItem]] =
    Process.unfoldEval(())(_ => next.map(_ strengthR (())))
      .take(chunkSize.value.toInt)
      .runLog

  ////

  private def next: Task[Option[XdmItem]] =
    nextItem >>= (_ traverse resultitem.loadItem)

  private def nextItem: Task[Option[ResultItem]] =
    Task.delay(if (rs.hasNext) Some(rs.next) else None)
}
