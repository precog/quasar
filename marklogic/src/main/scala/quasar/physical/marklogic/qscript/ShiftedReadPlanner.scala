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

package quasar.physical.marklogic.qscript

import quasar.Predef._
import quasar.fp.ski.κ
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._

import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._

private[qscript] final class ShiftedReadPlanner[F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr, FMT](
  implicit SP: StructuralPlanner[F, FMT], O: SearchOptions[FMT]
) extends Planner[F, FMT, Const[ShiftedRead, ?]] {

  import expr._, axes.child

  val plan: AlgebraM[F, Const[ShiftedRead, ?], XQuery] = {
    case Const(ShiftedRead(absFile, idStatus)) =>
      val uri    = posixCodec.printPath(fileParent(absFile) </> dir(fileName(absFile).value))
      val dirQry = cts.directoryQuery(uri.xs, "1".xs)

      idStatus match {
        case IncludeId => search(dirQry, doc => mkId(fn.documentUri(doc)).some)
        case ExcludeId => search(dirQry, κ(None))
        case IdOnly    => freshName[F] map (u =>
                            for_(u in cts.uris(start = emptySeq, query = dirQry)) return_ mkId(~u))
      }
  }

  ////

  private def mkId(uriStr: XQuery): XQuery =
    fn.concat("_".xs, xdmp.hmacSha1("quasar".xs, uriStr))

  private def search(ctsQuery: XQuery, extractId: XQuery => Option[XQuery]): F[XQuery] =
    for {
      d     <- freshName[F]
      c     <- freshName[F]
      incId <- extractId(~d) traverse (id => SP.seqToArray(mkSeq_(id, ~c)))
    } yield {
      for_(
        d in cts.search(expr = fn.doc(), query = ctsQuery, options = O.searchOptions))
      .let_(
        c := ~d `/` child.node())
      .return_(incId getOrElse ~c)
    }
}
