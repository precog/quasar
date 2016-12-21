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
private[qscript] final class QScriptCorePlanner[F[_]: QNameGenerator: PrologW: MonadPlanErr, T[_[_]]: BirecursiveT]
  extends MarkLogicPlanner[F, QScriptCore[T, ?]] {

  import expr.{func, for_, if_, let_}
  import ReduceFuncs._

  val plan: AlgebraM[F, QScriptCore[T, ?], XQuery] = {
    case Map(src, f) =>
      for {
        x <- freshName[F]
        g <- mapFuncXQuery(f, ~x)
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
        extract <- mapFuncXQuery(struct, ~l)
        lshift  <- qscript.elementLeftShift[F] apply (~ext)
        chkArr  <- ejson.isArray[F] apply ~ext
        getId   =  if_ (~isArr) then_ ~i else_ fn.nodeName(~r0)
        idExpr  <- id match {
                     case IdOnly    => getId.point[F]
                     case IncludeId => ejson.seqToArray_[F](mkSeq_(getId, ~r0))
                     case ExcludeId => (~r0).point[F]
                   }
        merge   <- mergeXQuery(repair, ~l, ~r)
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
        init  <- qscript.combineApply[F] apply (mkSeq(inits))
        cmbs  <- reducers traverse (reduceFuncCombine)
        cmb   <- qscript.combineN[F] apply (mkSeq(cmbs))
        fnls  <- reducers traverse (reduceFuncFinalize)
        fnl   <- qscript.zipApply[F] apply (mkSeq(fnls))
        y     <- freshName[F]
        rpr   <- planMapFunc(repair)(r => (~y)((r.idx + 1).xqy))
        rfnl  <- fx(x => let_(y := fnl.fnapply(x)).return_(rpr).point[F])
        bckt  <- fx(mapFuncXQuery[T, F](bucket, _))
        red   <- qscript.reduceWith[F] apply (init, cmb, rfnl, bckt, src)
      } yield red

    case Sort(src, bucket, order) =>
      for {
        x        <- freshName[F]
        xqyOrder <- ((bucket, SortDir.asc) <:: order).traverse { case (func, sortDir) =>
                      mapFuncXQuery(func, ~x) flatMap { by =>
                        ejson.castAsAscribed[F].apply(by) strengthR SortDirection.fromQScript(sortDir)
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
        l <- rebaseXQuery(lBranch, ~s)
        r <- rebaseXQuery(rBranch, ~s)
      } yield let_(s := src) return_ (l union r)

    case Filter(src, f) =>
      for {
        x <- freshName[F]
        p <- mapFuncXQuery(f, ~x) map (xs.boolean)
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
        fm  <- rebaseXQuery(from, ~s)
        ct  <- rebaseXQuery(count, ~s)
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
      x1  <- mapFuncXQuery[T, F](fm, ~x)
      nxt <- f(~acc, x1)
    } yield func(acc.render, x.render)(nxt)
  }

  def castingCombiner(fm: FreeMap[T])(f: (XQuery, XQuery) => F[XQuery]): F[XQuery] =
    combiner(fm)((acc, x) => ejson.castAsAscribed[F].apply(x) flatMap (f(acc, _)))

  def reduceFuncInit(rf: ReduceFunc[FreeMap[T]]): F[XQuery] = rf match {
    case Avg(fm)              => fx(x => mapFuncXQuery[T, F](fm, x) flatMap { v =>
                                   qscript.incAvgState[F].apply(1.xqy, v)
                                 })
    case Count(_)             => fx(_ => 1.xqy.point[F])
    case Max(fm)              => fx(x => (ejson.castAsAscribed[F] |@| mapFuncXQuery[T, F](fm, x))(_ apply _).join)
    case Min(fm)              => fx(x => (ejson.castAsAscribed[F] |@| mapFuncXQuery[T, F](fm, x))(_ apply _).join)
    case Sum(fm)              => fx(mapFuncXQuery[T, F](fm, _))
    case Arbitrary(fm)        => fx(mapFuncXQuery[T, F](fm, _))
    case UnshiftArray(fm)     => fx(x => mapFuncXQuery[T, F](fm, x) flatMap (ejson.singletonArray[F].apply(_)))
    case UnshiftMap(kfm, vfm) => fx(x => mapFuncXQuery[T, F](kfm, x).tuple(mapFuncXQuery[T, F](vfm, x)).flatMap {
                                   case (k, v) => ejson.singletonObject[F].apply(k, v)
                                 })
  }

  def reduceFuncFinalize(rf: ReduceFunc[_]): F[XQuery] = {
    val m = $("m")
    rf match {
      case Avg(_) => func(m.render)(map.get(~m, "avg".xs)).point[F]
      case other  => qscript.identity[F] flatMap (_.ref)
    }
  }

  def reduceFuncCombine(rf: ReduceFunc[FreeMap[T]]): F[XQuery] = rf match {
    case Avg(fm)              => combiner(fm)(qscript.incAvg[F].apply(_, _))
    case Count(fm)            => combiner(fm)((c, _) => (c + 1.xqy).point[F])
    case Max(fm)              => castingCombiner(fm)((x, y) => (if_ (y gt x) then_ y else_ x).point[F])
    case Min(fm)              => castingCombiner(fm)((x, y) => (if_ (y lt x) then_ y else_ x).point[F])
    case Sum(fm)              => combiner(fm)((x, y) => fn.sum(mkSeq_(x, y)).point[F])
    case Arbitrary(fm)        => combiner(fm)((x, _) => x.point[F])
    case UnshiftArray(fm)     => combiner(fm)(ejson.arrayAppend[F].apply(_, _))

    case UnshiftMap(kfm, vfm) =>
      val (m, x) = ($("m"), $("x"))
      mapFuncXQuery[T, F](kfm, ~x).tuple(mapFuncXQuery[T, F](vfm, ~x)).flatMap {
        case (k, v) => ejson.objectInsert[F].apply(~m, k, v) map (func(m.render, x.render)(_))
      }
  }
}
