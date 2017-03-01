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

package quasar.physical.marklogic.qscript

import quasar.contrib.pathy.{ADir, UriPathCodec}
import quasar.physical.marklogic.xml.NCName
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._

import eu.timepit.refined.auto._
import matryoshka._
import scalaz._, Scalaz._

private[qscript] final class ShiftedReadDirPlanner[F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr, FMT: SearchOptions](
  implicit
  SP: StructuralPlanner[F, FMT]
) extends Planner[F, FMT, Const[ShiftedRead[ADir], ?]] {

  import expr._, axes.child

  val plan: AlgebraM[F, Const[ShiftedRead[ADir], ?], XQuery] = {
    case Const(ShiftedRead(dir, idStatus)) =>
      val uri = UriPathCodec.printPath(dir).xs

      idStatus match {
        case ExcludeId => docsOnly(uri).point[F]
        case IncludeId => urisAndDocs(uri)
        case IdOnly    => urisOnly(uri).point[F]
      }
  }

  def docsOnly(uri: XQuery): XQuery =
    directoryDocuments(uri, false) `/` child.node()

  def urisAndDocs(uri: XQuery): F[XQuery] =
    freshName[F] >>= { d =>
      SP.seqToArray(mkSeq_(fn.baseUri(~d), ~d))
        .map(pair => fn.map(func(d.render) { pair }, docsOnly(uri)))
    }

  // NB: Might be able to get better performance out of cts:uris but will
  //     need a way to identify the format of a URI, which there doesn't
  //     seem to be an obvious way to do.
  def urisOnly(uri: XQuery): XQuery =
    fn.map(fn.ns(NCName("base-uri")) :# 1, docsOnly(uri))
}
