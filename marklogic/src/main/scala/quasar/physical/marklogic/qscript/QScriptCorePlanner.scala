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

import quasar.Predef.{Map => _, _}
import quasar.NameGenerator
import quasar.fp.ShowT
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._

import matryoshka._
import scalaz._, Scalaz._

private[qscript] final class QScriptCorePlanner[F[_]: NameGenerator: PrologW: MonadPlanErr, T[_[_]]: Recursive: ShowT]
  extends MarkLogicPlanner[F, QScriptCore[T, ?]] {

  import expr.{func, for_, let_}

  val plan: AlgebraM[F, QScriptCore[T, ?], XQuery] = {
    case Map(src, f) =>
      for {
        x <- freshVar[F]
        g <- mapFuncXQuery(f, x.xqy)
      } yield fn.map(func(x) { g }, src)

    case LeftShift(src, struct, repair) =>
      for {
        s       <- freshVar[F]
        l       <- freshVar[F]
        r       <- freshVar[F]
        extract <- mapFuncXQuery(struct, s.xqy)
        lshift  <- qscript.leftShift[F] apply (l.xqy)
        merge   <- mergeXQuery(repair, l.xqy, r.xqy)
      } yield for_ (s -> src) let_ (l -> extract, r -> lshift) return_ merge

    case Reduce(src, bucket, reducers, repair) =>
      XQuery(s"((: REDUCE :)$src)").point[F]

    case Sort(src, bucket, order) =>
      for {
        x           <- freshVar[F]
        xQueryOrder <- order.traverse { case (func, sortDir) =>
                         mapFuncXQuery(func, x.xqy).strengthR(SortDirection.fromQScript(sortDir))
                       }
        bucketOrder <- mapFuncXQuery(bucket, x.xqy).strengthR(SortDirection.Ascending)
      } yield for_(x -> src) orderBy (bucketOrder, xQueryOrder: _*) return_ x.xqy

    case Union(src, lBranch, rBranch) =>
      for {
        s <- freshVar[F]
        l <- rebaseXQuery(lBranch, s.xqy)
        r <- rebaseXQuery(rBranch, s.xqy)
      } yield let_(s -> src) return_ (l union r)

    case Filter(src, f) =>
      for {
        x <- freshVar[F]
        p <- mapFuncXQuery(f, x.xqy)
      } yield fn.filter(func(x) { p }, src)

    // NB: XQuery sequences use 1-based indexing.
    case Take(src, from, count) =>
      for {
        s   <- freshVar[F]
        f   <- freshVar[F]
        c   <- freshVar[F]
        fm  <- rebaseXQuery(from, s.xqy)
        ct  <- rebaseXQuery(count, s.xqy)
      } yield let_(s -> src, f -> fm, c -> ct) return_ fn.subsequence(f.xqy, 1.xqy, some(c.xqy))

    case Drop(src, from, count) =>
      for {
        s  <- freshVar[F]
        f  <- freshVar[F]
        c  <- freshVar[F]
        fm <- rebaseXQuery(from, s.xqy)
        ct <- rebaseXQuery(count, s.xqy)
      } yield let_(s -> src, f -> fm, c -> ct) return_ fn.subsequence(f.xqy, c.xqy + 1.xqy)

    case Unreferenced() =>
      expr.emptySeq.point[F]
  }
}
