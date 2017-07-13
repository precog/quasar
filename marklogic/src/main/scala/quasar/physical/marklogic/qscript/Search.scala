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

import quasar.physical.marklogic.cts._
import quasar.physical.marklogic.xquery._, syntax._
import quasar.qscript._

import eu.timepit.refined.auto._
import matryoshka._
import monocle.macros.Lenses
import scalaz._
import scalaz.syntax.monad._
import xml.name._

@Lenses
/** Represents a cts:search expression. */
final case class Search[Q](query: Q, idStatus: IdStatus)

object Search {
  def plan[F[_]: Monad: PrologW, Q, V, FMT](s: Search[Q], f: V => F[XQuery])(
    implicit
    Q:  Recursive.Aux[Q, Query[V, ?]],
    SP: StructuralPlanner[F, FMT],
    O:  SearchOptions[FMT]
  ): F[XQuery] = {
    import axes.child
    val x = $("x")

    def docsOnly: F[XQuery] = {
      val queryM = Q.cataM(s.query)(Query.toXQuery[V, F](f))

      queryM.map(q =>
        cts.search(
          expr    = fn.doc(),
          query   = q,
          options = SearchOptions[FMT].searchOptions
        ) `/` child.node())
    }

    def urisAndDocs: F[XQuery] =
      docsOnly >>= (docOnly => SP.seqToArray(mkSeq_(fn.baseUri(~x), ~x)) map { pair =>
        fn.map(expr.func(x.render) { pair }, docOnly)
      })

    def urisOnly: F[XQuery] =
      docsOnly map (fn.map(fn.ns(NCName("base-uri")) :# 1, _))

    s.idStatus match {
      case ExcludeId => docsOnly
      case IncludeId => urisAndDocs
      case IdOnly    => urisOnly
    }
  }
}
