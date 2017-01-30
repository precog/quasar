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
import quasar.common.SortDir
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._

import eu.timepit.refined.auto._
import matryoshka._
import scalaz._, Scalaz._

// TODO: We assume a FLWOR expression emits single values if at least one tuple
//       stream is defined. This isn't required by XQuery as emitted sequences
//       will automatically be flattened. We don't emit any of these in the
//       QScript planner, but we also have no way of ensuring this.
//
//       Without this contract we cannot safely fuse expressions by binding the
//       return expression to a variable as, if it is a sequence, the results will,
//       in the best case, also be sequences and exceptions in the worst.
private[qscript] final class QScriptCorePlanner[F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr, FMT, T[_[_]]: BirecursiveT](
  implicit
  SP: StructuralPlanner[F, FMT],
  QTP: Planner[F, FMT, QScriptTotal[T, ?]],
  MFP: Planner[F, FMT, MapFunc[T, ?]]
) extends Planner[F, FMT, QScriptCore[T, ?]] {

  import expr.{func, for_, if_, let_}
  import ReduceFuncs._

  val plan: AlgebraM[F, QScriptCore[T, ?], XQuery] = {
    case Map(src, f) =>
      for {
        x <- freshName[F]
        g <- mapFuncXQuery[T, F, FMT](f, ~x)
      } yield src match {
        case IterativeFlwor(bindings, filter, order, isStable, result) =>
          XQuery.Flwor(bindings :::> IList(BindingClause.let_(x := result)), filter, order, isStable, g)

        case _ =>
          for_(x in src) return_ g
      }

    // TODO: Use type information from `Guard` when available to determine
    //       if `ext` is being treated as an array or an object.
    case LeftShift(src, struct, id, repair) =>
      for {
        l       <- freshName[F]
        ext     <- freshName[F]
        isArr   <- freshName[F]
        r0      <- freshName[F]
        r       <- freshName[F]
        i       <- freshName[F]
        extract <- mapFuncXQuery[T, F, FMT](struct, ~l)
        lshift  <- SP.leftShift(~ext)
        chkArr  <- SP.isArray(~ext)
        getId   =  if_ (~isArr) then_ ~i else_ fn.nodeName(~r0)
        idExpr  <- id match {
                     case IdOnly    => getId.point[F]
                     case IncludeId => SP.seqToArray(mkSeq_(getId, ~r0))
                     case ExcludeId => (~r0).point[F]
                   }
        merge   <- mergeXQuery[T, F, FMT](repair, ~l, ~r)
      } yield  src match {
        case IterativeFlwor(bindings, filter, order, isStable, result) =>
          val addlBindings = IList(
            BindingClause.let_(l := result, ext := extract, isArr := chkArr),
            BindingClause.for_(r0 at i in lshift),
            BindingClause.let_(r := idExpr))

          XQuery.Flwor(bindings :::> addlBindings, filter, order, isStable, merge)

        case _ =>
          for_ (l in src)
          .let_(ext   := extract,
                isArr := chkArr)
          .for_(r0 at i in lshift)
          .let_(r     := idExpr)
          .return_(merge)
      }

    // TODO: Start leveraging the cts:* aggregation functions when possible
    case Reduce(src, bucket, reducers, repair) =>
      for {
        inits <- reducers traverse (reduceFuncInit)
        init  <- lib.combineApply[F] apply (mkSeq(inits))
        cmbs  <- reducers traverse (reduceFuncCombine)
        cmb   <- lib.combineN[F] apply (mkSeq(cmbs))
        fnls  <- reducers traverse (reduceFuncFinalize)
        fnl   <- lib.zipApply[F] apply (mkSeq(fnls))
        y     <- freshName[F]
        rpr   <- planMapFunc[T, F, FMT, ReduceIndex](repair)(r => (~y)((r.idx + 1).xqy))
        rfnl  <- fx(x => let_(y := fnl.fnapply(x)).return_(rpr).point[F])
        bckt  <- fx(mapFuncXQuery[T, F, FMT](bucket, _))
        red   <- lib.reduceWith[F] apply (init, cmb, rfnl, bckt, src)
      } yield red

    case Sort(src, bucket, order) =>
      for {
        x        <- freshName[F]
        xqyOrder <- ((bucket, SortDir.asc) <:: order).traverse { case (func, sortDir) =>
                      mapFuncXQuery[T, F, FMT](func, ~x) flatMap { by =>
                        SP.asSortKey(by) strengthR SortDirection.fromQScript(sortDir)
                      }
                    }
      } yield src match {
        case IterativeFlwor(bindings, filter, _, _, result) =>
          XQuery.Flwor(bindings :::> IList(BindingClause.let_(x := result)), filter, xqyOrder.list, false, ~x)

        case _ =>
          for_(x in src) orderBy (xqyOrder.head, xqyOrder.tail.toList: _*) return_ ~x
      }

    case Union(src, lBranch, rBranch) =>
      for {
        s <- freshName[F]
        l <- rebaseXQuery[T, F, FMT](lBranch, ~s)
        r <- rebaseXQuery[T, F, FMT](rBranch, ~s)
      } yield let_(s := src) return_ (mkSeq_(l) union mkSeq_(r))

    case Filter(src, f) =>
      for {
        x <- freshName[F]
        p <- mapFuncXQuery[T, F, FMT](f, ~x) map (xs.boolean)
      } yield src match {
        case IterativeFlwor(bindings, filter, order, isStable, result) =>
          XQuery.Flwor(
            bindings :::> IList(BindingClause.let_(x := result)),
            Some(filter.fold(p)(_ and p)),
            order,
            isStable,
            ~x)

        case _ =>
          for_(x in src) where_ p return_ ~x
      }

    // NB: XQuery sequences use 1-based indexing.
    case Subset(src, from, sel, count) =>
      for {
        s   <- freshName[F]
        f   <- freshName[F]
        c   <- freshName[F]
        fm  <- rebaseXQuery[T, F, FMT](from, ~s)
        ct  <- rebaseXQuery[T, F, FMT](count, ~s)
      } yield let_(s := src, f := fm, c := ct) return_ (sel match {
        case Drop   => fn.subsequence(~f, ~c + 1.xqy)
        case Take   => fn.subsequence(~f, 1.xqy, some(~c))
        // TODO: Better sampling
        case Sample => fn.subsequence(~f, 1.xqy, some(~c))
      })

    case Unreferenced() =>
      "Unreferenced".xs.point[F]
  }

  ////

  def fx(f: XQuery => F[XQuery]): F[XQuery] = {
    val x = $("x")
    f(~x) map (func(x.render)(_))
  }

  def combiner(fm: FreeMap[T])(f: (XQuery, XQuery) => F[XQuery]): F[XQuery] = {
    val (acc, x) = ($("acc"), $("x"))

    for {
      x1  <- mapFuncXQuery[T, F, FMT](fm, ~x)
      nxt <- f(~acc, x1)
    } yield func(acc.render, x.render)(nxt)
  }

  def castingCombiner(fm: FreeMap[T])(f: (XQuery, XQuery) => F[XQuery]): F[XQuery] =
    combiner(fm)((acc, x) => SP.castIfNode(x) >>= (f(acc, _)))

  def reduceFuncInit(rf: ReduceFunc[FreeMap[T]]): F[XQuery] = rf match {
    case Avg(fm)              => fx(x => for {
                                   v0 <- mapFuncXQuery[T, F, FMT](fm, x)
                                   v  <- SP.castIfNode(v0)
                                   st <- lib.incAvgState[F].apply(1.xqy, v)
                                 } yield st)
    case Count(_)             => fx(_ => 1.xqy.point[F])
    case Max(fm)              => fx(x => mapFuncXQuery[T, F, FMT](fm, x) >>= (SP.castIfNode(_)))
    case Min(fm)              => fx(x => mapFuncXQuery[T, F, FMT](fm, x) >>= (SP.castIfNode(_)))
    case Sum(fm)              => fx(x => mapFuncXQuery[T, F, FMT](fm, x) >>= (SP.castIfNode(_)))
    case Arbitrary(fm)        => fx(mapFuncXQuery[T, F, FMT](fm, _))
    case First(fm)            => fx(mapFuncXQuery[T, F, FMT](fm, _))
    case Last(fm)             => fx(mapFuncXQuery[T, F, FMT](fm, _))
    case UnshiftArray(fm)     => fx(x => mapFuncXQuery[T, F, FMT](fm, x) >>= (SP.singletonArray(_)))
    case UnshiftMap(kfm, vfm) => fx(x => mapFuncXQuery[T, F, FMT](kfm, x).tuple(mapFuncXQuery[T, F, FMT](vfm, x)).flatMap {
                                   case (k, v) => SP.singletonObject(k, v)
                                 })
  }

  def reduceFuncFinalize(rf: ReduceFunc[_]): F[XQuery] = {
    val m = $("m")
    rf match {
      case Avg(_) => func(m.render)(map.get(~m, "avg".xs)).point[F]
      case other  => lib.identity[F] flatMap (_.ref)
    }
  }

  def reduceFuncCombine(rf: ReduceFunc[FreeMap[T]]): F[XQuery] = rf match {
    case Avg(fm)              => castingCombiner(fm)(lib.incAvg[F].apply(_, _))
    case Count(fm)            => combiner(fm)((c, _) => (c + 1.xqy).point[F])
    case Max(fm)              => castingCombiner(fm)((x, y) => (if_ (y gt x) then_ y else_ x).point[F])
    case Min(fm)              => castingCombiner(fm)((x, y) => (if_ (y lt x) then_ y else_ x).point[F])
    case Sum(fm)              => castingCombiner(fm)((x, y) => fn.sum(mkSeq_(x, y)).point[F])
    case Arbitrary(fm)        => combiner(fm)((x, _) => x.point[F])
    case First(fm)            => combiner(fm)((x, _) => x.point[F])
    case Last(fm)             => combiner(fm)((_, y) => y.point[F])
    case UnshiftArray(fm)     => combiner(fm)(SP.arrayAppend(_, _))
    case UnshiftMap(kfm, vfm) =>
      val (m, x) = ($("m"), $("x"))
      mapFuncXQuery[T, F, FMT](kfm, ~x).tuple(mapFuncXQuery[T, F, FMT](vfm, ~x)).flatMap {
        case (k, v) => SP.objectInsert(~m, k, v) map (func(m.render, x.render)(_))
      }
  }
}
