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

package quasar.qscript

import quasar.Predef._
import quasar.RenderTree
import quasar.fp._

import matryoshka._
import matryoshka.patterns._
import monocle.macros.Lenses
import scalaz._, Scalaz._

sealed abstract class QScriptCore[T[_[_]], A]

/** A data-level transformation.
  */
@Lenses final case class Map[T[_[_]], A](src: A, f: FreeMap[T])
    extends QScriptCore[T, A]

@Lenses final case class ReduceIndex(idx: Int) {
  def incr(j: Int): ReduceIndex = ReduceIndex(idx + j)
}

object ReduceIndex {
  implicit def equal: Equal[ReduceIndex] =
    Equal.equalBy(_.idx)

  implicit def show: Show[ReduceIndex] =
    Show.show {
      case ReduceIndex(idx) =>
        Cord("ReduceIndex(") ++ idx.show ++ Cord(")")
    }
}

/** Flattens nested structure, converting each value into a data set, which are
  * then unioned.
  *
  * `struct` is an expression that evaluates to an array or object, which is
  * then “exploded” into multiple values. `repair` is applied across the new
  * set, integrating the exploded values into the original set.
  *
  * E.g., in:
  *     LeftShift(x,
  *               ProjectField(SrcHole, "bar"),
  *               ConcatMaps(LeftSide, MakeMap("bar", RightSide)))```
  * If `x` consists of things that look like `{ foo: 7, bar: [1, 2, 3] }`, then
  * that’s what [[LeftSide]] is. And [[RightSide]] is values like `1`, `2`, and
  * `3`, because that’s what you get from flattening the struct.So then our
  * right-biased [[quasar.qscript.MapFuncs.ConcatMaps]] says to concat
  * `{ foo: 7, bar: [1, 2, 3] }` with `{ bar: 1 }`, resulting in
  * `{ foo: 7, bar: 1 }` (then again with `{ foo: 7, bar: 2 }` and
  * `{ foo: 7, bar: 3 }`, finishing up the handling of that one element in the
  * original (`x`) dataset.
  */
@Lenses final case class LeftShift[T[_[_]], A](
  src: A,
  struct: FreeMap[T],
  repair: JoinFunc[T])
    extends QScriptCore[T, A]

/** Performs a reduction over a dataset, with the dataset partitioned by the
  * result of the MapFunc. So, rather than many-to-one, this is many-to-fewer.
  *
  * `bucket` partitions the values into buckets based on the result of the
  * expression, `reducers` applies the provided reduction to each expression,
  * and repair finally turns those reduced expressions into a final value.
  *
  * @group MRA
  */
// TODO: type level guarantees about indexing with `repair` into `reducers`
@Lenses final case class Reduce[T[_[_]], A](
  src: A,
  bucket: FreeMap[T],
  reducers: List[ReduceFunc[FreeMap[T]]],
  repair: Free[MapFunc[T, ?], ReduceIndex])
    extends QScriptCore[T, A]

/** Sorts values within a bucket. This could be represented with
  *     LeftShift(Map(Reduce(src, bucket, UnshiftArray(_)), _.sort(order)),
  *               RightSide)
  * but backends tend to provide sort directly, so this avoids backends having
  * to recognize the pattern. We could provide an algebra
  *     (Sort :+: QScript)#λ => QScript
  * so that a backend without a native sort could eliminate this node.
  */
@Lenses final case class Sort[T[_[_]], A](
  src: A,
  bucket: FreeMap[T],
  order: List[(FreeMap[T], SortDir)])
    extends QScriptCore[T, A]

/** Creates a new dataset that contains the elements from the datasets created
  * by each branch. Duplicate values should be eliminated.
  */
@Lenses final case class Union[T[_[_]], A](
  src: A,
  lBranch: FreeQS[T],
  rBranch: FreeQS[T])
    extends QScriptCore[T, A]

/** Eliminates some values from a dataset, based on the result of `f` (which
  * must evaluate to a boolean value for each element in the set).
  */
@Lenses final case class Filter[T[_[_]], A](src: A, f: FreeMap[T])
    extends QScriptCore[T, A]

@Lenses final case class Take[T[_[_]], A](src: A, from: FreeQS[T], count: FreeQS[T])
    extends QScriptCore[T, A]

@Lenses final case class Drop[T[_[_]], A](src: A, from: FreeQS[T], count: FreeQS[T])
    extends QScriptCore[T, A]

/** A placeholder value that can appear in plans, but will never be referenced
  * in the result. We consider this a wart. It should be implemented as an
  * arbitrary value with minimal cost to generate (since it will simply be
  * discarded).
  */
@Lenses final case class Unreferenced[T[_[_]], A]()
    extends QScriptCore[T, A]

object QScriptCore {
  implicit def equal[T[_[_]]: EqualT]: Delay[Equal, QScriptCore[T, ?]] =
    new Delay[Equal, QScriptCore[T, ?]] {
      def apply[A](eq: Equal[A]) =
        Equal.equal {
          case (Map(a1, f1), Map(a2, f2)) => f1 ≟ f2 && eq.equal(a1, a2)
          case (LeftShift(a1, s1, r1), LeftShift(a2, s2, r2)) =>
            eq.equal(a1, a2) && s1 ≟ s2 && r1 ≟ r2
          case (Reduce(a1, b1, f1, r1), Reduce(a2, b2, f2, r2)) =>
            b1 ≟ b2 && f1 ≟ f2 && r1 ≟ r2 && eq.equal(a1, a2)
          case (Sort(a1, b1, o1), Sort(a2, b2, o2)) =>
            b1 ≟ b2 && o1 ≟ o2 && eq.equal(a1, a2)
          case (Union(a1, l1, r1), Union(a2, l2, r2)) =>
            eq.equal(a1, a2) && l1 ≟ l2 && r1 ≟ r2
          case (Filter(a1, f1), Filter(a2, f2)) => f1 ≟ f2 && eq.equal(a1, a2)
          case (Take(a1, f1, c1), Take(a2, f2, c2)) => eq.equal(a1, a2) && f1 ≟ f2 && c1 ≟ c2
          case (Drop(a1, f1, c1), Drop(a2, f2, c2)) => eq.equal(a1, a2) && f1 ≟ f2 && c1 ≟ c2
          case (Unreferenced(), Unreferenced()) => true
          case (_, _) => false
        }
    }

  implicit def traverse[T[_[_]]]: Traverse[QScriptCore[T, ?]] =
    new Traverse[QScriptCore[T, ?]] {
      def traverseImpl[G[_]: Applicative, A, B](
        fa: QScriptCore[T, A])(
        f: A => G[B]) =
        fa match {
          case Map(a, func)               => f(a) ∘ (Map[T, B](_, func))
          case LeftShift(a, s, r)         => f(a) ∘ (LeftShift(_, s, r))
          case Reduce(a, b, func, repair) => f(a) ∘ (Reduce(_, b, func, repair))
          case Sort(a, b, o)              => f(a) ∘ (Sort(_, b, o))
          case Union(a, l, r)             => f(a) ∘ (Union(_, l, r))
          case Filter(a, func)            => f(a) ∘ (Filter(_, func))
          case Take(a, from, c)           => f(a) ∘ (Take(_, from, c))
          case Drop(a, from, c)           => f(a) ∘ (Drop(_, from, c))
          case Unreferenced()             => (Unreferenced[T, B](): QScriptCore[T, B]).point[G]
        }
    }

  implicit def show[T[_[_]]: ShowT]: Delay[Show, QScriptCore[T, ?]] =
    new Delay[Show, QScriptCore[T, ?]] {
      def apply[A](s: Show[A]): Show[QScriptCore[T, A]] =
        Show.show {
          case Map(src, mf) => Cord("Map(") ++
            s.show(src) ++ Cord(",") ++
            mf.show ++ Cord(")")
          case LeftShift(src, struct, repair) => Cord("LeftShift(") ++
            s.show(src) ++ Cord(",") ++
            struct.show ++ Cord(",") ++
            repair.show ++ Cord(")")
          case Reduce(a, b, red, rep) => Cord("Reduce(") ++
            s.show(a) ++ Cord(",") ++
            b.show ++ Cord(",") ++
            red.show ++ Cord(",") ++
            rep.show ++ Cord(")")
          case Sort(a, b, o) => Cord("Sort(") ++
            s.show(a) ++ Cord(",") ++
            b.show ++ Cord(",") ++
            o.show ++ Cord(")")
          case Union(src, l, r) => Cord("Union(") ++
            s.show(src) ++ Cord(",") ++
            l.show ++ Cord(",") ++
            r.show ++ Cord(")")
          case Filter(a, func) => Cord("Filter(") ++
            s.show(a) ++ Cord(",") ++
            func.show ++ Cord(")")
          case Take(a, f, c) => Cord("Take(") ++
            s.show(a) ++ Cord(",") ++
            f.show ++ Cord(",") ++
            c.show ++ Cord(")")
          case Drop(a, f, c) => Cord("Drop(") ++
            s.show(a) ++ Cord(",") ++
            f.show ++ Cord(",") ++
            c.show ++ Cord(")")
          case Unreferenced() => Cord("Unreferenced")
        }
    }

  implicit def renderTree[T[_[_]]: ShowT]:
      Delay[RenderTree, QScriptCore[T, ?]] =
    RenderTree.delayFromShow

  implicit def mergeable[T[_[_]]: Recursive: Corecursive: EqualT]:
      Mergeable.Aux[T, QScriptCore[T, ?]] =
    new Mergeable[QScriptCore[T, ?]] {
      type IT[F[_]] = T[F]

      def mergeSrcs(
        left: FreeMap[T],
        right: FreeMap[T],
        p1: EnvT[Ann[T], QScriptCore[IT, ?], ExternallyManaged],
        p2: EnvT[Ann[T], QScriptCore[IT, ?], ExternallyManaged]) =
        (p1, p2) match {
          case (EnvT((Ann(b1, v1), Map(_, m1))),
                EnvT((Ann(_,  v2), Map(_, m2)))) =>
            // TODO: optimize cases where one side is a subset of the other
            val (mf, lv, rv) = concat(v1 >> m1 >> left, v2 >> m2 >> right)
            concatBuckets(b1.map(_ >> m1)) match {
              case Some((buck, newBuckets)) => {
                val (full, buckAccess, valAccess) = concat(buck, mf)
                SrcMerge[EnvT[Ann[T], QScriptCore[IT, ?], ExternallyManaged], FreeMap[IT]](
                  EnvT((
                    Ann(newBuckets.list.toList.map(_ >> buckAccess), valAccess),
                    Map(Extern, full))),
                  lv,
                  rv).some
              }
              case None =>
                SrcMerge[EnvT[Ann[T], QScriptCore[IT, ?], ExternallyManaged], FreeMap[IT]](
                  EnvT((EmptyAnn[T], Map(Extern, mf))),
                  lv,
                  rv).some
            }

          case (EnvT((Ann(b1, _), Reduce(_, bucket1, func1, rep1))),
                EnvT((Ann(_,  _), Reduce(_, bucket2, func2, rep2)))) =>
            val mapL = bucket1 >> left
            val mapR = bucket2 >> right

            (mapL ≟ mapR).option {
              val funcL = func1.map(_.map(_ >> left))
              val funcR = func1.map(_.map(_ >> right))
              val (newRep, lrep, rrep) = concat(rep1, rep2.map(_.incr(func1.length)))

              SrcMerge[EnvT[Ann[T], QScriptCore[IT, ?], ExternallyManaged], FreeMap[IT]](
                EnvT((
                  Ann(b1, HoleF),
                  Reduce(Extern, mapL, funcL ++ funcR, newRep))),
                lrep,
                rrep)
            }

          case (_, _) => None
        }
    }

  // show is needed for debugging
  implicit def normalizable[T[_[_]]: Recursive: Corecursive: EqualT: ShowT]:
      Normalizable[QScriptCore[T, ?]] =
    new Normalizable[QScriptCore[T, ?]] {
      val opt = new Optimize[T]

      def normalize = new (QScriptCore[T, ?] ~> QScriptCore[T, ?]) {
        def apply[A](qc: QScriptCore[T, A]) = qc match {
          case Map(src, f) => Map(src, normalizeMapFunc(f))
          case LeftShift(src, s, r) =>
            LeftShift(src, normalizeMapFunc(s), normalizeMapFunc(r))
          case Reduce(src, bucket, reducers, repair) =>
            val normBuck = normalizeMapFunc(bucket)
            Reduce(
              src,
              // NB: all single-bucket reductions should reduce on `null`
              normBuck.resume.fold({
                case MapFuncs.Constant(_) => MapFuncs.NullLit[T, Hole]()
                case _                    => normBuck
              }, κ(normBuck)),
              reducers.map(_.map(normalizeMapFunc(_))),
              normalizeMapFunc(repair))
          case Sort(src, bucket, order) =>
            Sort(src, normalizeMapFunc(bucket), order.map(_.leftMap(normalizeMapFunc(_))))
          case Union(src, l, r) =>
            Union(
              src,
              freeTransCata(l)(liftCo(opt.applyToFreeQS[QScriptTotal[T, ?]])),
              freeTransCata(r)(liftCo(opt.applyToFreeQS[QScriptTotal[T, ?]])))
          case Filter(src, f) => Filter(src, normalizeMapFunc(f))
          case Take(src, from, count) =>
            Take(
              src,
              freeTransCata(from)(liftCo(opt.applyToFreeQS[QScriptTotal[T, ?]])),
              freeTransCata(count)(liftCo(opt.applyToFreeQS[QScriptTotal[T, ?]])))
          case Drop(src, from, count) =>
            Drop(
              src,
              freeTransCata(from)(liftCo(opt.applyToFreeQS[QScriptTotal[T, ?]])),
              freeTransCata(count)(liftCo(opt.applyToFreeQS[QScriptTotal[T, ?]])))
          case Unreferenced() => Unreferenced()
        }
      }
    }
}
