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
import quasar.contrib.matryoshka.ShowT
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._

import matryoshka._
import scalaz._, Scalaz._

private[qscript] final class QScriptCorePlanner[F[_]: NameGenerator: PrologW: MonadPlanErr, T[_[_]]: Recursive: ShowT]
  extends MarkLogicPlanner[F, QScriptCore[T, ?]] {

  import expr.{func, for_, let_}
  import ReduceFuncs._

  val plan: AlgebraM[F, QScriptCore[T, ?], XQuery] = {
    case Map(src, f) =>
      for {
        x <- freshVar[F]
        g <- mapFuncXQuery(f, x.xqy)
      } yield fn.map(func(x) { g }, src)

    case LeftShift(src, struct, repair) =>
      for {
        l       <- freshVar[F]
        v       <- freshVar[F]
        rs      <- freshVar[F]
        r       <- freshVar[F]
        extract <- mapFuncXQuery(struct, l.xqy)
        lshift  <- qscript.elementLeftShift[F] apply (v.xqy)
        merge   <- mergeXQuery(repair, l.xqy, r.xqy)
      } yield for_ (l -> src) let_ (v -> extract, rs -> lshift) return_ fn.map(func(r)(merge), rs.xqy)

    // TODO: Start leveraging the cts:* aggregation functions when possible
    case Reduce(src, bucket, reducers, repair) =>
      for {
        inits <- reducers traverse (reduceFuncInit)
        init  <- qscript.combineApply[F] apply (mkSeq(inits))
        cmbs  <- reducers traverse (reduceFuncCombine)
        cmb   <- qscript.combineN[F] apply (mkSeq(cmbs))
        fnls  <- reducers traverse (reduceFuncFinalize)
        fnl   <- qscript.zipApply[F] apply (mkSeq(fnls))
        y     <- freshVar[F]
        rpr   <- planMapFunc(repair)(r => y.xqy((r.idx + 1).xqy))
        rfnl  <- fx(x => let_(y -> fnl.fnapply(x)).return_(rpr).point[F])
        bckt  <- fx(mapFuncXQuery[T, F](bucket, _))
        red   <- qscript.reduceWith[F] apply (init, cmb, rfnl, bckt, src)
      } yield red

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
    case Subset(src, from, sel, count) =>
      for {
        s   <- freshVar[F]
        f   <- freshVar[F]
        c   <- freshVar[F]
        fm  <- rebaseXQuery(from, s.xqy)
        ct  <- rebaseXQuery(count, s.xqy)
      } yield let_(s -> src, f -> fm, c -> ct) return_ (sel match {
        case Drop => fn.subsequence(f.xqy, c.xqy + 1.xqy)
        case Take => fn.subsequence(f.xqy, 1.xqy, some(c.xqy))
        // TODO: Better sampling
        case Sample => fn.subsequence(f.xqy, 1.xqy, some(c.xqy))
      })

    case Unreferenced() =>
      "Unreferenced".xs.point[F]
  }

  ////

  def fx(f: XQuery => F[XQuery]): F[XQuery] =
    f("$x".xqy) map (func("$x")(_))

  def combiner(fm: FreeMap[T])(f: (XQuery, XQuery) => F[XQuery]): F[XQuery] =
    for {
      x1  <- mapFuncXQuery[T, F](fm, "$x".xqy)
      nxt <- f("$acc".xqy, x1)
    } yield func("$acc", "$x")(nxt)

  def reduceFuncInit(rf: ReduceFunc[FreeMap[T]]): F[XQuery] = rf match {
    case Avg(fm)              => fx(x => mapFuncXQuery[T, F](fm, x) flatMap { v =>
                                   qscript.incAvgState[F].apply(1.xqy, v)
                                 })
    case Count(_)             => fx(_ => 1.xqy.point[F])
    case Max(fm)              => fx(mapFuncXQuery[T, F](fm, _))
    case Min(fm)              => fx(mapFuncXQuery[T, F](fm, _))
    case Sum(fm)              => fx(mapFuncXQuery[T, F](fm, _))
    case Arbitrary(fm)        => fx(mapFuncXQuery[T, F](fm, _))
    case UnshiftArray(fm)     => fx(x => mapFuncXQuery[T, F](fm, x) flatMap (ejson.singletonArray[F].apply(_)))
    case UnshiftMap(kfm, vfm) => fx(x => mapFuncXQuery[T, F](kfm, x).tuple(mapFuncXQuery[T, F](vfm, x)).flatMap {
                                   case (k, v) => ejson.singletonObject[F].apply(k, v)
                                 })
  }

  def reduceFuncFinalize(rf: ReduceFunc[_]): F[XQuery] = rf match {
    case Avg(_) => func("$m")(map.get("$m".xqy, "avg".xs)).point[F]
    case other  => qscript.identity[F] flatMap (_.ref)
  }

  def reduceFuncCombine(rf: ReduceFunc[FreeMap[T]]): F[XQuery] = rf match {
    case Avg(fm)              => combiner(fm)(qscript.incAvg[F].apply(_, _))
    case Count(fm)            => combiner(fm)((c, _) => (c + 1.xqy).point[F])
    case Max(fm)              => combiner(fm)((x, y) => fn.max(mkSeq_(x, y)).point[F])
    case Min(fm)              => combiner(fm)((x, y) => fn.min(mkSeq_(x, y)).point[F])
    case Sum(fm)              => combiner(fm)((x, y) => fn.sum(mkSeq_(x, y)).point[F])
    case Arbitrary(fm)        => combiner(fm)((x, _) => x.point[F])
    case UnshiftArray(fm)     => combiner(fm)(ejson.arrayAppend[F].apply(_, _))

    case UnshiftMap(kfm, vfm) =>
      val (m, x) = ("$m", "$x")
      mapFuncXQuery[T, F](kfm, x.xqy).tuple(mapFuncXQuery[T, F](vfm, x.xqy)).flatMap {
        case (k, v) => ejson.objectInsert[F].apply(m.xqy, k, v) map (func(m, x)(_))
      }
  }
}
