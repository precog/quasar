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

package quasar.qscript

import slamdata.Predef.{Map => ScalaMap, _}
import quasar.{NonTerminal, Terminal, RenderTree, RenderTreeT}, RenderTree.ops._
import quasar.common.SortDir
import quasar.contrib.matryoshka._
import quasar.fp._
import quasar.qscript.analysis.{DeepShape, RefEq}

import matryoshka._
import matryoshka.data._
import monocle.macros.Lenses
import scalaz._, Scalaz._

sealed abstract class QScriptCore[T[_[_]], A] extends Product with Serializable

/** A data-level transformation.
  */
@Lenses final case class Map[T[_[_]], A](src: A, f: FreeMap[T])
    extends QScriptCore[T, A]

/** Left indexes into the bucket. Right indexes into the reducers.
  */
@Lenses final case class ReduceIndex(idx: Int \/ Int) {
  def shift(mapping: ScalaMap[Int, Int]): ReduceIndex =
    ReduceIndex(idx ∘ (x => x + mapping.get(x).getOrElse(0)))
}

object ReduceIndex {

  implicit val equal: Equal[ReduceIndex] =
    Equal.equalBy(_.idx)

  implicit val renderTree: RenderTree[ReduceIndex] =
    RenderTree.simple(List("ReduceIndex"), _.idx.shows.some)
  implicit val show: Show[ReduceIndex] = RenderTree.toShow
}

/** Flattens nested structure, converting each value into a data set, which are
  * then unioned.
  *
  * `struct` is an expression that evaluates to an array or object, which is
  * then “exploded” into multiple values. `idStatus` indicates what each of
  * those exploded values should look like (either just the value, just the “id”
  * (i.e., the key or index), or a 2-element array of key and value). `repair`
  * is applied across the new set, integrating the exploded values into the
  * original set.
  *
  * E.g., in:
  *     LeftShift(x,
  *               ProjectField(SrcHole, "bar"),
  *               ExcludeId,
  *               ConcatMaps(LeftSide, MakeMap("bar", RightSide)))```
  * If `x` consists of things that look like `{ foo: 7, bar: [1, 2, 3] }`, then
  * that’s what [[LeftSide]] is. And [[RightSide]] is values like `1`, `2`, and
  * `3`, because that’s what you get from flattening the struct.So then our
  * right-biased [[quasar.qscript.MapFuncsCore.ConcatMaps]] says to concat
  * `{ foo: 7, bar: [1, 2, 3] }` with `{ bar: 1 }`, resulting in
  * `{ foo: 7, bar: 1 }` (then again with `{ foo: 7, bar: 2 }` and
  * `{ foo: 7, bar: 3 }`, finishing up the handling of that one element in the
  * original (`x`) dataset.
  */
@Lenses final case class LeftShift[T[_[_]], A](
  src: A,
  struct: FreeMap[T],
  idStatus: IdStatus,
  repair: JoinFunc[T])
    extends QScriptCore[T, A]

/** Performs a reduction over a dataset, with the dataset partitioned by the
  * result of the bucket MapFuncCore. So, rather than many-to-one, this is many-to-fewer.
  *
  * `bucket` partitions the values into buckets based on the result of the
  * expression, `reducers` applies the provided reduction to each expression,
  * and repair finally turns those reduced expressions into a final value.
  *
  * ReduceIndex is guaranteed to be a valid index into `reducers`.
  * @group MRA
  */
// TODO: type level guarantees about indexing with `repair` into `reducers`
@Lenses final case class Reduce[T[_[_]], A](
  src: A,
  bucket: List[FreeMap[T]],
  reducers: List[ReduceFunc[FreeMap[T]]], // FIXME: Use Vector instead
  repair: FreeMapA[T, ReduceIndex])
    extends QScriptCore[T, A]

/** Sorts values within a bucket. This can be an _unstable_ sort, but the
  * elements of `order` must be stably sorted.
  */
// NB: This could be represented with
//     LeftShift(Map(Reduce(src, bucket, UnshiftArray(_)), _.sort(order)),
//               RightSide)
// but backends tend to provide sort directly, so this avoids backends having
// to recognize the pattern. We could provide an algebra
//     (Sort :+: QScript)#λ => QScript
// so that a backend without a native sort could eliminate this node.
@Lenses final case class Sort[T[_[_]], A](
  src: A,
  bucket: List[FreeMap[T]],
  order: NonEmptyList[(FreeMap[T], SortDir)])
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
@Lenses final case class Filter[T[_[_]], A](
  src: A,
  f: FreeMap[T])
    extends QScriptCore[T, A]

/** Chooses a subset of values from a dataset, given a count. */
@Lenses final case class Subset[T[_[_]], A](
  src: A,
  from: FreeQS[T],
  op: SelectionOp,
  count: FreeQS[T])
    extends QScriptCore[T, A]

/** A placeholder value that can appear in plans, but will never be referenced
  * in the result. We consider this a wart. It should be implemented as an
  * arbitrary value (of cardinality 1) with minimal cost to generate (since it
  * will simply be discarded).
  */
@Lenses final case class Unreferenced[T[_[_]], A]()
    extends QScriptCore[T, A]

object QScriptCore {
  implicit def equal[T[_[_]]: BirecursiveT: EqualT]: Delay[Equal, QScriptCore[T, ?]] =
    new Delay[Equal, QScriptCore[T, ?]] {
      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def apply[A](eq: Equal[A]) =
        Equal.equal {
          case (Map(a1, f1), Map(a2, f2)) => f1 ≟ f2 && eq.equal(a1, a2)
          case (LeftShift(a1, s1, i1, r1), LeftShift(a2, s2, i2, r2)) =>
            eq.equal(a1, a2) && s1 ≟ s2 && i1 ≟ i2 && r1 ≟ r2
          case (Reduce(a1, b1, f1, r1), Reduce(a2, b2, f2, r2)) =>
            b1 ≟ b2 && f1 ≟ f2 && r1 ≟ r2 && eq.equal(a1, a2)
          case (Sort(a1, b1, o1), Sort(a2, b2, o2)) =>
            b1 ≟ b2 && o1 ≟ o2 && eq.equal(a1, a2)
          case (Union(a1, l1, r1), Union(a2, l2, r2)) =>
            eq.equal(a1, a2) && l1 ≟ l2 && r1 ≟ r2
          case (Filter(a1, f1), Filter(a2, f2)) => f1 ≟ f2 && eq.equal(a1, a2)
          case (Subset(a1, f1, s1, c1), Subset(a2, f2, s2, c2)) => eq.equal(a1, a2) && f1 ≟ f2 && s1 ≟ s2 && c1 ≟ c2
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
          case LeftShift(a, s, i, r)      => f(a) ∘ (LeftShift(_, s, i, r))
          case Reduce(a, b, func, repair) => f(a) ∘ (Reduce(_, b, func, repair))
          case Sort(a, b, o)              => f(a) ∘ (Sort(_, b, o))
          case Union(a, l, r)             => f(a) ∘ (Union(_, l, r))
          case Filter(a, func)            => f(a) ∘ (Filter(_, func))
          case Subset(a, from, sel, c)    => f(a) ∘ (Subset(_, from, sel, c))
          case Unreferenced()             => (Unreferenced[T, B](): QScriptCore[T, B]).point[G]
        }
    }

  implicit def show[T[_[_]]: ShowT]: Delay[Show, QScriptCore[T, ?]] =
    new Delay[Show, QScriptCore[T, ?]] {
      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def apply[A](s: Show[A]): Show[QScriptCore[T, A]] =
        Show.show {
          case Map(src, mf) => Cord("Map(") ++
            s.show(src) ++ Cord(", ") ++
            mf.show ++ Cord(")")
          case LeftShift(src, struct, id, repair) => Cord("LeftShift(") ++
            s.show(src) ++ Cord(", ") ++
            struct.show ++ Cord(", ") ++
            id.show ++ Cord(", ") ++
            repair.show ++ Cord(")")
          case Reduce(a, b, red, rep) => Cord("Reduce(") ++
            s.show(a) ++ Cord(", ") ++
            b.show ++ Cord(", ") ++
            red.show ++ Cord(", ") ++
            rep.show ++ Cord(")")
          case Sort(a, b, o) => Cord("Sort(") ++
            s.show(a) ++ Cord(", ") ++
            b.show ++ Cord(", ") ++
            o.show ++ Cord(")")
          case Union(src, l, r) => Cord("Union(") ++
            s.show(src) ++ Cord(", ") ++
            l.show ++ Cord(", ") ++
            r.show ++ Cord(")")
          case Filter(a, func) => Cord("Filter(") ++
            s.show(a) ++ Cord(", ") ++
            func.show ++ Cord(")")
          case Subset(a, f, sel, c) => Cord("Subset(") ++
            s.show(a) ++ Cord(", ") ++
            f.show ++ Cord(", ") ++
            sel.show ++ Cord(", ") ++
            c.show ++ Cord(")")
          case Unreferenced() => Cord("Unreferenced")
        }
    }

  implicit def renderTree[T[_[_]]: RenderTreeT: ShowT]
      : Delay[RenderTree, QScriptCore[T, ?]] =
    new Delay[RenderTree, QScriptCore[T, ?]] {
      def apply[A](RA: RenderTree[A]): RenderTree[QScriptCore[T, A]] =
        new RenderTree[QScriptCore[T, A]] {
          val nt = List("QScriptCore")

          @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
          def nested[A: RenderTree](label: String, a: A) =
            NonTerminal(label :: nt, None, a.render :: Nil)
          @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
          def render(v: QScriptCore[T, A]) =
            v match {
              case Map(src, f) =>
                NonTerminal("Map" :: nt, None,
                  RA.render(src) :: f.render :: Nil)
              case LeftShift(src, struct, id, repair) =>
                NonTerminal("LeftShift" :: nt, None,
                  RA.render(src) ::
                    nested("Struct", struct) ::
                    nested("IdStatus", id) ::
                    nested("Repair", repair) ::
                    Nil)
              case Reduce(src, bucket, reducers, repair) =>
                NonTerminal("Reduce" :: nt, None,
                  RA.render(src) ::
                    nested("Bucket", bucket) ::
                    NonTerminal("Reducers" :: nt, None, reducers.map(_.render)) ::
                    nested("Repair", repair) ::
                    Nil)
              case Sort(src, bucket, order) =>
                NonTerminal("Sort" :: nt, None,
                  RA.render(src) :: nested("Bucket", bucket) ::
                    NonTerminal("Order" :: "Sort" :: nt, None, order.map { case (x, dir) =>
                      NonTerminal(dir.shows :: "Sort" :: nt, None, nested("By", x) :: Nil) }.toList) ::
                    Nil)
              case Union(src, lBranch, rBranch) =>
                NonTerminal("Union" :: nt, None, List(
                  RA.render(src),
                  nested("LeftBranch", lBranch),
                  nested("RightBranch", rBranch)))
              case Filter(src, func) =>
                NonTerminal("Filter" :: nt, None, List(
                  RA.render(src),
                  func.render))
              case Subset(src, from, sel, count) =>
                NonTerminal("Subset" :: nt, sel.shows.some, List(
                  RA.render(src),
                  nested("From", from),
                  nested("Count", count)))
              case Unreferenced()             =>
                Terminal("Unreferenced" :: nt, None)
            }
        }
    }

  // When checking equality of `FreeMap`s we check their literal equality and
  // their referential equality. It is imperative that we do not _only_ check
  // their referential equality as this may be false (because we failed to
  // see through the shape) while their literal equality holds.
  // This is code smell in that referential equality should ideally be an
  // extension of literal equality.
  //
  // TODO Address the above problem so that in the least it's not possible to
  // only check for referentially equality without checking for literal equality.
  // Ideally we can unify these definitions more though.
  implicit def mergeable[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]:
      Mergeable.Aux[T, QScriptCore[T, ?]] =
    new Mergeable[QScriptCore[T, ?]] {
      type IT[F[_]] = T[F]

      def mergeSrcs(
        left: Mergeable.MergeSide[IT, QScriptCore[T, ?]],
        right: Mergeable.MergeSide[IT, QScriptCore[T, ?]]) = {

        val norm = Normalizable.normalizable[T]

        val lacc = left.access
        val racc = right.access

        (left.source, right.source) match {
          case (Unreferenced(), Unreferenced()) =>
            SrcMerge[QScriptCore[IT, ExternallyManaged], FreeMap[IT]](
              Unreferenced(),
              HoleF,
              HoleF).some

          case (Map(_, m1), Map(_, m2)) =>
            // TODO: optimize cases where one side is a subset of the other
            val (mf, lv, rv) = concat(m1 >> lacc, m2 >> racc)
            SrcMerge[QScriptCore[IT, ExternallyManaged], FreeMap[IT]](
              Map(Extern, mf),
              lv,
              rv).some

          case (
            Reduce(_, bucket1, reducers1, repair1),
            Reduce(_, bucket2, reducers2, repair2)) =>

            // we arbitrarily choose the left or the right bucket in the result
            val lBucket: List[FreeMap[IT]] = bucket1 ∘ (b => norm.freeMF(b >> lacc))
            val rBucket: List[FreeMap[IT]] = bucket2 ∘ (b => norm.freeMF(b >> racc))

            lazy val lBucketEq: List[DeepShape.FreeShape[IT]] =
	      bucket1 ∘ (b => DeepShape.normalize(b >> left.shape))

            lazy val rBucketEq: List[DeepShape.FreeShape[IT]] =
	      bucket2 ∘ (b => DeepShape.normalize(b >> right.shape))

            (lBucket ≟ rBucket || RefEq.refEq(lBucketEq, rBucketEq)).option {
              val lReducers = reducers1 ∘ (_ ∘ (_ >> lacc))
              val rReducers = reducers2 ∘ (_ ∘ (_ >> racc))

              if (lReducers ≟ rReducers) {
                val (newRepair, lRepair, rRepair) = concat(repair1, repair2)

                SrcMerge[QScriptCore[IT, ExternallyManaged], FreeMap[IT]](
                  Reduce(Extern, lBucket, lReducers, newRepair),
                  lRepair,
                  rRepair)
              } else {
                // this isn't performant but the lists are typically small
                val (newReducers, _mappingR) =
                  rReducers.foldLeft((lReducers, List.empty[Int])) {
                    case ((acc, indices), value) =>
                      lReducers.indexWhere(_ ≟ value) match {
                        case -1 => // the right value does not exist on the left
                          (acc :+ value, indices :+ acc.length)
                        case i => // the right value exists on the left
                          (acc, indices :+ i)
                      }
                  }

                val mappingR: ScalaMap[Int, Int] =
                  _mappingR.zipWithIndex.map(_.swap).toMap

                val (newRepair, lRepair, rRepair) =
                  concat(repair1, repair2 ∘ (_.shift(mappingR)))

                SrcMerge[QScriptCore[IT, ExternallyManaged], FreeMap[IT]](
                  Reduce(Extern, lBucket, newReducers, newRepair),
                  lRepair,
                  rRepair)
              }
            }

          case (
            LeftShift(_, struct1, id1, repair1),
            LeftShift(_, struct2, id2, repair2)) =>

            // we arbitrarily choose the left or the right struct in the result
            val lStruct: FreeMap[IT] = norm.freeMF(struct1 >> lacc)
            val rStruct: FreeMap[IT] = norm.freeMF(struct2 >> racc)

            lazy val lStructEq: DeepShape.FreeShape[IT] = DeepShape.normalize(struct1 >> left.shape)
            lazy val rStructEq: DeepShape.FreeShape[IT] = DeepShape.normalize(struct2 >> right.shape)

            val idAccess: IdStatus => JoinFunc[IT] = {
              case ExcludeId =>
                Free.roll(MFC(MapFuncsCore.ProjectIndex[IT, JoinFunc[IT]](
                  RightSideF[IT],
                  MapFuncsCore.IntLit[IT, JoinSide](1))))
              case IdOnly =>
                Free.roll(MFC(MapFuncsCore.ProjectIndex[IT, JoinFunc[IT]](
                  RightSideF[IT],
                  MapFuncsCore.IntLit[IT, JoinSide](0))))
              case IncludeId => RightSideF
            }

            (lStruct ≟ rStruct || RefEq.refEq(lStructEq, rStructEq)).option {
              def constructMerge(access1: JoinFunc[IT], access2: JoinFunc[IT]) = {
                val (repair, repL, repR) =
                  concat(
                    norm.freeMF(repair1 >>= {
                      case LeftSide  => lacc >> LeftSideF
                      case RightSide => access1
                    }),
                    norm.freeMF(repair2 >>= {
                      case LeftSide  => racc >> LeftSideF
                      case RightSide => access2
                    }))

                SrcMerge[QScriptCore[IT, ExternallyManaged], FreeMap[IT]](
                  LeftShift(Extern, lStruct, id1 |+| id2, repair),
                  repL,
                  repR)
              }

              (id1 ≟ id2).fold(
                constructMerge(RightSideF,    RightSideF),
                constructMerge(idAccess(id1), idAccess(id2)))
            }

          case (Filter(s1, c1), Filter(_, c2)) =>
            // we arbitrarily choose the left or the right condition in the result
	    val lCond: FreeMap[IT] = norm.freeMF(c1 >> lacc)
	    val rCond: FreeMap[IT] = norm.freeMF(c2 >> racc)

	    lazy val lCondEq: DeepShape.FreeShape[IT] = DeepShape.normalize(c1 >> left.shape)
	    lazy val rCondEq: DeepShape.FreeShape[IT] = DeepShape.normalize(c2 >> right.shape)

            (lCond ≟ rCond || RefEq.refEq(lCondEq, rCondEq))
              .option(SrcMerge(Filter(s1, lCond), lacc, racc))

          case (Sort(s1, b1, o1), Sort(_, b2, o2)) =>
            // we arbitrarily choose the left or the right bucket in the result
            val lBucket = b1.map(b => norm.freeMF(b >> lacc))
            val rBucket = b2.map(b => norm.freeMF(b >> racc))

            // we arbitrarily choose the left or the right order in the result
            val lOrder = o1.map(_.leftMap(o => norm.freeMF(o >> lacc)))
            val rOrder = o2.map(_.leftMap(o => norm.freeMF(o >> racc)))

            lazy val lBucketEq = b1.map(b => DeepShape.normalize(b >> left.shape))
            lazy val rBucketEq = b2.map(b => DeepShape.normalize(b >> right.shape))

            lazy val lOrderEq = o1.map(_.leftMap(o => DeepShape.normalize(o >> left.shape)))
            lazy val rOrderEq = o2.map(_.leftMap(o => DeepShape.normalize(o >> right.shape)))

            ((lBucket ≟ rBucket || RefEq.refEq(lBucketEq, rBucketEq)) &&
	      (lOrder ≟ rOrder || RefEq.refEq(lOrderEq, rOrderEq))).option {
                SrcMerge(Sort(s1, lBucket, lOrder), lacc, racc)
	      }

          case (Subset(s1, f1, o1, c1), Subset(_, f2, o2, c2)) =>
            val from1 = rebaseBranch(f1, lacc)
            val from2 = rebaseBranch(f2, racc)
            val count1 = rebaseBranch(c1, lacc)
            val count2 = rebaseBranch(c2, racc)
            (from1 ≟ from2 && o1 ≟ o2 && count1 ≟ count2).option(
              SrcMerge(Subset(s1, from1, o1, count1), HoleF, HoleF))

          case (Union(s1, l1, r1), Union(_, l2, r2)) =>
            val left1 = rebaseBranch(l1, lacc)
            val left2 = rebaseBranch(l2, racc)
            val right1 = rebaseBranch(r1, lacc)
            val right2 = rebaseBranch(r2, racc)
            (left1 ≟ left2 && right1 ≟ right2).option(
              SrcMerge(Union(s1, left1, right1), HoleF, HoleF))

          case (_, _) => None
        }
      }
    }
}
