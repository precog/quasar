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
import quasar.{RenderTree, RenderTreeT}
import quasar.contrib.matryoshka._
import quasar.fp._
import quasar.fp.ski._
import quasar.qscript.MapFuncCore._
import quasar.qscript.MapFuncsCore._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
import simulacrum.typeclass

object PATypes {
  type SeenIndices = Set[BigInt]
  type KnownIndices = Option[SeenIndices]
  type Indices = ScalaMap[Hole \/ JoinSide, KnownIndices]

  sealed abstract class RewriteState
  final case object Ignore extends RewriteState
  final case class Rewrite(indices: Indices) extends RewriteState

  def liftHole(in: ScalaMap[Hole, KnownIndices]): RewriteState =
    Rewrite(in.mapKeys(_.left))

  def liftJoinSide(in: ScalaMap[JoinSide, KnownIndices]): RewriteState =
    Rewrite(in.mapKeys(_.right))

  implicit final class KnownIndicesOps(val self: KnownIndices) extends AnyVal {

    /** The standard Semigroup on Option appends a Some and a None to result in a Some.
     * This appends a Some and a None to result in a None.
     */
    def |++|(other: KnownIndices): KnownIndices =
      Semigroup.liftSemigroup[Option, SeenIndices].append(self, other)
  }

  implicit final class IndicesOps[A](val self: ScalaMap[A, KnownIndices]) extends AnyVal {
    def |++|(other: ScalaMap[A, KnownIndices]): ScalaMap[A, KnownIndices] =
      self.unionWith(other)(_ |++| _)
  }

  implicit def RewriteStateMonoid: Monoid[RewriteState] = new Monoid[RewriteState] {
    def zero: RewriteState = Rewrite(ScalaMap.empty)
    def append(f1: RewriteState, f2: => RewriteState): RewriteState =
      (f1, f2) match {
        case (Ignore, _) => Ignore
        case (_, Ignore) => Ignore
        case (Rewrite(a), Rewrite(b)) => Rewrite(a |++| b)
      }
  }
}

@typeclass trait PruneArrays[F[_]] {
  import PATypes._

  def find[M[_], A](in: F[A])(implicit M: MonadState[M, RewriteState]): M[RewriteState]
  def remap[M[_], A](env: RewriteState, in: F[A])(
      implicit M: MonadState[M, RewriteState]): M[F[A]]
}

class PAHelpers[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] extends TTypes[T] {
  import PATypes._

  type IndexMapping = ScalaMap[BigInt, BigInt]
  type EitherMap[A] = FreeMapA[A \/ A]

  // NB: Neded due to how the type parameters of CoEnv are ordered.
  private val cbf = Bifunctor[CoEnv[?, MapFunc, ?]]

  /** Returns `None` if a non-static non-integer index was found.
   * Else returns all indices of the form `ProjectIndex(SrcHole, IntLit(_))`.
   */
  def findIndicesInFunc[A](func: FreeMapA[A]): ScalaMap[A, KnownIndices] =
    func.project.run.fold(k => ScalaMap(k -> none), κ(findIndicesInStruct(func)))

  def findIndicesInStruct[A](func: FreeMapA[A]): ScalaMap[A, KnownIndices] = {
    def accumulateIndices: GAlgebra[(FreeMapA[A], ?), MapFunc, ScalaMap[A, KnownIndices]] = {
      case MFC(ProjectIndex((src, acc1), (value, acc2))) =>
        val newMap = acc1 |++| acc2
        (src.project.run, value.project.run) match {
          case (-\/(k), \/-(IntLitMapFunc(idx))) =>
            newMap + (k -> newMap
              .get(k)
              .fold(Set(idx).some)(_.map(_ + idx))) // static integer index
          case (-\/(k), _) => newMap + (k -> none) // non-static index
          case (_, _) => newMap
        }
      // check if entire array is referenced
      case f =>
        f.foldRight(ScalaMap.empty[A, KnownIndices])((elem, acc) =>
          elem match {
            case (Embed(CoEnv(-\/(k))), value) => (value |++| acc) + (k -> none)
            case (_, value) => value |++| acc
        })
    }

    def findIndices: GAlgebra[(FreeMapA[A], ?), CoEnvMapA[A, ?], ScalaMap[A, KnownIndices]] =
      _.run.fold(k => ScalaMap.empty, accumulateIndices)

    func.para(findIndices)
  }

  /** NB: The lhs of the disjunction allows us to ensure we only remove a single
   *     layer of a nested projection of the same index, like
   *
   *     ProjectIndex(
   *       ProjectIndex(
   *         Hole,
   *         Int(1))
   *       Int(1))
   *
   *     without this extra bit of state, rewriting the inner project to just `Hole` would
   *     also end up rewriting the outer project during the next step of the fold as
   *     it would then match the ProjectIndex(Hole, Int(i)) pattern we look for.
   */
  private def remapResult[A](
      hole: A,
      mapping: IndexMapping,
      idx: BigInt): CoEnvMapA[A \/ A, EitherMap[A]] = {

    def mkPrj(i: BigInt) =
      CoEnv(
        MFC(
          ProjectIndex[T, EitherMap[A]](
            CoEnv(hole.right[A].left[MapFunc[EitherMap[A]]]).embed,
            IntLit(i))).right[A \/ A])

    mapping.get(idx).fold(mkPrj(idx)) { i =>
      if (i === 0 && mapping.size === 1)
        CoEnv(hole.left[A].left)
      else
        mkPrj(i)
    }
  }

  /** Remap all indices in `func` in structures like
   * `ProjectIndex(SrcHole, IntLit(_))` according to the provided `mapping`.
   */
  def remapIndicesInFunc(func: FreeMap, mapping: IndexMapping): FreeMap =
    func.transCata[EitherMap[Hole]] {
      case CoEnv(\/-(MFC(ProjectIndex(Embed(CoEnv(-\/(\/-(SrcHole)))), IntLit(idx))))) =>
        remapResult[Hole](SrcHole, mapping, idx)
      case co => cbf.leftMap(co)(_.right[Hole])
    } map (_.merge)

  def remapIndicesInJoinFunc(
      func: JoinFunc,
      lMapping: IndexMapping,
      rMapping: IndexMapping): JoinFunc =
    func.transCata[EitherMap[JoinSide]] {
      case CoEnv(\/-(MFC(ProjectIndex(Embed(CoEnv(-\/(\/-(LeftSide)))), IntLit(idx))))) =>
        remapResult[JoinSide](LeftSide, lMapping, idx)
      case CoEnv(\/-(MFC(ProjectIndex(Embed(CoEnv(-\/(\/-(RightSide)))), IntLit(idx))))) =>
        remapResult[JoinSide](RightSide, rMapping, idx)
      case co => cbf.leftMap(co)(_.right[JoinSide])
    } map (_.merge)

  // NB: We only remap `LeftSide` references as a) we only reported indices
  //     from `LeftSide` references in `find` and b) except under very
  //     contrived circumstances, we don't know anything about the shape of
  //     the values resulting from a left shift.
  def remapIndicesInLeftShift[A](repair: JoinFunc, mapping: IndexMapping): JoinFunc =
    repair.transCata[EitherMap[JoinSide]] {
      case CoEnv(\/-(MFC(ProjectIndex(Embed(CoEnv(-\/(\/-(LeftSide)))), IntLit(idx))))) =>
        remapResult[JoinSide](LeftSide, mapping, idx)
      case co => cbf.leftMap(co)(_.right[JoinSide])
    } map (_.merge)

  /** Prune the provided `array` keeping only the indices in `indicesToKeep`,
   * eliding it altogether if only a single element is retained.
   */
  def rewriteRepair[A: Show: RenderTree](
      repair: FreeMapA[A],
      seen: SeenIndices): Option[FreeMapA[A]] =
    repair.project.some collect {
      case StaticArray(array) =>
        val rewrite = new quasar.qscript.Rewrite[T]
        rewrite.rebuildArray[A](seen.map(_.toInt).toList.sorted ∘ array) match {
          case Embed(CoEnv(\/-(MFC(MakeArray(a))))) => a
          case other => other
        }
    }

  // TODO currently we only rewrite the branch if it is precisely a LeftShift
  // we need to generalize this so we can rewrite all rewritable branches
  // e.g. sometimes Filter(LeftShift(_, _, _, ConcatArrays)) is rewritable
  def rewriteBranch(branch: FreeQS, seen: SeenIndices): Option[FreeQS] =
    branch.resume match {
      case -\/(qs) =>
        Inject[QScriptCore, QScriptTotal].prj(qs) >>= {
          case LeftShift(src, struct, id, repair) =>
            rewriteRepair(repair, seen) ∘ (rep =>
              Free.roll(Inject[QScriptCore, QScriptTotal].inj(LeftShift(src, struct, id, rep))))

          case Reduce(src, bucket, reducers, repair) =>
            rewriteRepair(repair, seen) ∘ (rep =>
              Free.roll(
                Inject[QScriptCore, QScriptTotal].inj(Reduce(src, bucket, reducers, rep))))
          case _ => none
        }
      case _ => none
    }

  // TODO: Can we be more efficient? - can get rid of `.sorted`, but might be
  //       non-deterministic, then.
  val indexMapping: SeenIndices => IndexMapping =
    _.toList.sorted.zipWithIndex.map(_.rightMap(BigInt(_))).toMap
}

// TODO `find` and `remap` impls should be returning a free algebra
// which is interpreted separately
object PruneArrays {
  import PATypes._

  private def haltRemap[M[_], A](out: A)(implicit M: MonadState[M, RewriteState]): M[A] =
    M.put(Ignore).as(out)

  private def default[IN[_]]: PruneArrays[IN] =
    new PruneArrays[IN] {
      def find[M[_], A](in: IN[A])(implicit M: MonadState[M, RewriteState]) =
        haltRemap(Ignore)

      def remap[M[_], A](env: RewriteState, in: IN[A])(
          implicit M: MonadState[M, RewriteState]) =
        haltRemap(in)
    }

  private def getIndices(key: Hole \/ JoinSide, indices: Indices): KnownIndices =
    indices.get(key).getOrElse(Set.empty[BigInt].some)

  private def remapState[F[_], A](
      state: RewriteState,
      default: F[A],
      mapping: SeenIndices => F[A]): F[A] =
    state match {
      case Ignore => default
      case Rewrite(indices) => getIndices(SrcHole.left, indices).fold(default)(mapping)
    }

  implicit def coenv[T[_[_]]](
      implicit PAQST: PruneArrays[QScriptTotal[T, ?]]): PruneArrays[CoEnvQS[T, ?]] =
    new PruneArrays[CoEnvQS[T, ?]] {

      def find[M[_], A](in: CoEnvQS[T, A])(implicit M: MonadState[M, RewriteState]) =
        in.run.fold(κ(default.find(in)), PAQST.find(_))

      def remap[M[_], A](env: RewriteState, in: CoEnvQS[T, A])(
          implicit M: MonadState[M, RewriteState]) =
        in.run
          .fold(κ(default.remap(env, in)), PAQST.remap(env, _).map(qs => CoEnv(qs.right[Hole])))
    }

  implicit def coproduct[I[_], J[_]](
      implicit I: PruneArrays[I],
      J: PruneArrays[J]): PruneArrays[Coproduct[I, J, ?]] =
    new PruneArrays[Coproduct[I, J, ?]] {

      def find[M[_], A](in: Coproduct[I, J, A])(implicit M: MonadState[M, RewriteState]) =
        in.run.fold(I.find[M, A], J.find[M, A])

      def remap[M[_], A](env: RewriteState, in: Coproduct[I, J, A])(
          implicit M: MonadState[M, RewriteState]) =
        in.run.fold(I.remap(env, _) ∘ Coproduct.leftc, J.remap(env, _) ∘ Coproduct.rightc)
    }

  implicit def read[A]: PruneArrays[Const[Read[A], ?]] = default
  implicit def shiftedRead[A]: PruneArrays[Const[ShiftedRead[A], ?]] = default
  implicit def deadEnd: PruneArrays[Const[DeadEnd, ?]] = default

  implicit def thetaJoin[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]
    : PruneArrays[ThetaJoin[T, ?]] =
    new PruneArrays[ThetaJoin[T, ?]] {
      val helpers = new PAHelpers[T]
      import helpers._

      def find[M[_], A](in: ThetaJoin[A])(implicit M: MonadState[M, RewriteState]) = {
        val state: RewriteState =
          liftJoinSide(findIndicesInFunc[JoinSide](in.on)) |+|
            liftJoinSide(findIndicesInFunc[JoinSide](in.combine))
        M.put(Ignore).as(state) // annotate computed state as environment
      }

      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def remap[M[_], A](env: RewriteState, in: ThetaJoin[A])(
          implicit M: MonadState[M, RewriteState]) =
        haltRemap(env match {
          case Ignore => in
          case Rewrite(indices) => {
            val leftIndices: KnownIndices = getIndices(LeftSide.right, indices)
            val rightIndices: KnownIndices = getIndices(RightSide.right, indices)

            val (lrepl, lBranch): (IndexMapping, FreeQS) =
              leftIndices.flatMap { seen =>
                rewriteBranch(in.lBranch, seen).map((indexMapping(seen), _))
              }.getOrElse((ScalaMap.empty, in.lBranch))

            val (rrepl, rBranch): (IndexMapping, FreeQS) =
              rightIndices.flatMap { seen =>
                rewriteBranch(in.rBranch, seen).map((indexMapping(seen), _))
              }.getOrElse((ScalaMap.empty, in.rBranch))

            ThetaJoin(
              in.src,
              lBranch.pruneArraysBranch(Ignore),
              rBranch.pruneArraysBranch(Ignore),
              remapIndicesInJoinFunc(in.on, lrepl, rrepl),
              in.f,
              remapIndicesInJoinFunc(in.combine, lrepl, rrepl)
            )
          }
        })
    }

  implicit def equiJoin[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]
    : PruneArrays[EquiJoin[T, ?]] =
    new PruneArrays[EquiJoin[T, ?]] {
      val helpers = new PAHelpers[T]
      import helpers._

      def find[M[_], A](in: EquiJoin[A])(implicit M: MonadState[M, RewriteState]) = {
        val state: RewriteState =
          liftJoinSide(
            (in.key ∘ (_._1))
              .foldRight(ScalaMap.empty[Hole, KnownIndices])(findIndicesInFunc[Hole](_) |++| _)
              .collect { case (SrcHole, v) => (LeftSide, v) }) |+|
            liftJoinSide((in.key ∘ (_._2))
              .foldRight(ScalaMap.empty[Hole, KnownIndices])(findIndicesInFunc[Hole](_) |++| _)
              .collect { case (SrcHole, v) => (RightSide, v) }) |+|
            liftJoinSide(findIndicesInFunc[JoinSide](in.combine))
        M.put(Ignore).as(state) // annotate computed state as environment
      }

      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def remap[M[_], A](env: RewriteState, in: EquiJoin[A])(
          implicit M: MonadState[M, RewriteState]) =
        haltRemap(env match {
          case Ignore => in
          case Rewrite(indices) => {
            val leftIndices: KnownIndices = getIndices(LeftSide.right, indices)
            val rightIndices: KnownIndices = getIndices(RightSide.right, indices)

            val (lrepl, lBranch): (IndexMapping, FreeQS) =
              leftIndices.flatMap { seen =>
                rewriteBranch(in.lBranch, seen).map((indexMapping(seen), _))
              }.getOrElse((ScalaMap.empty, in.lBranch))

            val (rrepl, rBranch): (IndexMapping, FreeQS) =
              rightIndices.flatMap { seen =>
                rewriteBranch(in.rBranch, seen).map((indexMapping(seen), _))
              }.getOrElse((ScalaMap.empty, in.rBranch))

            EquiJoin(
              in.src,
              lBranch.pruneArraysBranch(Ignore),
              rBranch.pruneArraysBranch(Ignore),
              in.key ∘ (_.bimap(remapIndicesInFunc(_, lrepl), remapIndicesInFunc(_, rrepl))),
              in.f,
              remapIndicesInJoinFunc(in.combine, lrepl, rrepl)
            )
          }
        })
    }

  def extractFromMap[A](map: ScalaMap[A, KnownIndices], key: A): KnownIndices =
    map.get(key).getOrElse(Set.empty.some)

  implicit def projectBucket[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]
    : PruneArrays[ProjectBucket[T, ?]] =
    new PruneArrays[ProjectBucket[T, ?]] {

      val helpers = new PAHelpers[T]
      import helpers._

      private def findInBucket[M[_]](fm1: FreeMap, fm2: FreeMap)(
          implicit M: MonadState[M, RewriteState]): M[RewriteState] =
        M.put(
            liftHole(findIndicesInFunc[Hole](fm1)) |+| liftHole(findIndicesInFunc[Hole](fm2)))
          .as(Ignore)

      def find[M[_], A](in: ProjectBucket[A])(implicit M: MonadState[M, RewriteState]) =
        in match {
          case BucketField(_, value, name) => findInBucket(value, name)
          case BucketIndex(_, value, index) => findInBucket(value, index)
        }

      def remap[M[_], A](env: RewriteState, in: ProjectBucket[A])(
          implicit M: MonadState[M, RewriteState]) = {
        val mapping: SeenIndices => ProjectBucket[A] =
          indexMapping >>> (repl =>
            in match {
              case BucketField(src, value, name) =>
                BucketField(
                  src,
                  remapIndicesInFunc(value, repl),
                  remapIndicesInFunc(name, repl))
              case BucketIndex(src, value, index) =>
                BucketIndex(
                  src,
                  remapIndicesInFunc(value, repl),
                  remapIndicesInFunc(index, repl))
            })
        M.get >>= (st => haltRemap(remapState(st, in, mapping)))
      }
    }

  implicit def qscriptCore[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]
    : PruneArrays[QScriptCore[T, ?]] =
    new PruneArrays[QScriptCore[T, ?]] {

      val helpers = new PAHelpers[T]
      import helpers._

      def find[M[_], A](in: QScriptCore[A])(implicit M: MonadState[M, RewriteState]) =
        in match {
          case LeftShift(_, struct, _, repair) =>
            val state: RewriteState =
              liftHole(findIndicesInFunc[JoinSide](repair).collect {
                case (LeftSide, i) => (SrcHole, i)
              }) |+| liftHole(findIndicesInFunc[Hole](struct))

            M.get >>= (st =>
              M.put(state)
                .as(repair.resume match {
                  case -\/(MFC(ConcatArrays(_, _))) =>
                    st // annotate previous state as environment
                  case _ => Ignore
                }))

          case Reduce(src, bucket, reducers, _) =>
            val bucketIndices: RewriteState =
              liftHole(
                ScalaMap(
                  SrcHole ->
                    bucket.traverse(findIndicesInFunc(_).get(SrcHole).join).map(_.unite.toSet)))
            val reducersIndices: RewriteState =
              reducers.foldMap(
                _.foldMap[RewriteState](f => liftHole(findIndicesInFunc[Hole](f))))

            M.put(bucketIndices |+| reducersIndices).as(Ignore)

          case Union(_, _, _) => default.find(in)
          case Subset(_, _, _, _) => M.modify(ι).as(Ignore)

          case Map(_, func) => M.put(liftHole(findIndicesInFunc[Hole](func))).as(Ignore)
          case Filter(_, func) =>
            M.modify(liftHole(findIndicesInFunc[Hole](func)) |+| _).as(Ignore)

          case Sort(_, bucket, order) =>
            val bucketState: RewriteState =
              liftHole(
                ScalaMap(
                  SrcHole ->
                    bucket.traverse(findIndicesInFunc(_).get(SrcHole).join).map(_.unite.toSet)))
            val orderState: RewriteState = order.foldMap {
              case (f, _) => liftHole(findIndicesInFunc(f))
            }
            M.modify(bucketState |+| orderState |+| _).as(Ignore)

          case Unreferenced() => (Ignore: RewriteState).point[M]
        }

      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def remap[M[_], A](env: RewriteState, in: QScriptCore[A])(
          implicit M: MonadState[M, RewriteState]) =
        // ignore `env` everywhere except for `LeftShift`
        in match {
          case LeftShift(src, struct, id, repair) =>
            def replacement(repair: JoinFunc, repl: IndexMapping): QScriptCore[A] =
              LeftShift(
                src,
                remapIndicesInFunc(struct, repl),
                id,
                remapIndicesInLeftShift(repair, repl))

            def notPruned: M[QScriptCore[A]] =
              M.get >>= (st =>
                haltRemap(remapState(st, in, indexMapping >>> (replacement(repair, _)))))

            def pruned(prunedRepair: JoinFunc): M[QScriptCore[A]] = {
              val unchanged = LeftShift(src, struct, id, prunedRepair)
              (M.get <* M.put(env)) ∘ (remapState(
                _,
                unchanged,
                indexMapping >>> (replacement(prunedRepair, _))))
            }

            env match {
              case Ignore => notPruned
              case Rewrite(indices) =>
                ((getIndices(SrcHole.left, indices) >>= (rewriteRepair(repair, _))) ∘ pruned) | notPruned
            }

          case Reduce(src, bucket0, reducers0, repair) =>
            def replacement(repl: IndexMapping): QScriptCore[A] =
              Reduce(
                src,
                bucket0 ∘ (remapIndicesInFunc(_, repl)),
                reducers0.map(_.map(remapIndicesInFunc(_, repl))),
                repair)
            M.get >>= (st => haltRemap(remapState(st, in, indexMapping >>> replacement)))

          case Union(src, lBranch, rBranch) =>
            M.put(Ignore)
              .as(
                Union(
                  src,
                  lBranch.pruneArraysBranch(Ignore),
                  rBranch.pruneArraysBranch(Ignore)))

          case Subset(src, from, op, count) =>
            def replacement(state: RewriteState) =
              Subset(src, from.pruneArraysBranch(state), op, count.pruneArraysBranch(Ignore))
            M.get ∘ (state => remapState(state, replacement(Ignore), _ => replacement(state)))

          case Map(src, func) =>
            def replacement(repl: IndexMapping): QScriptCore[A] =
              Map(src, remapIndicesInFunc(func, repl))
            M.get >>= (st => haltRemap(remapState(st, in, indexMapping >>> replacement)))

          case Filter(src, func) =>
            def replacement(repl: IndexMapping): QScriptCore[A] =
              Filter(src, remapIndicesInFunc(func, repl))
            M.get ∘ (remapState(_, in, indexMapping >>> replacement))

          case Sort(src, bucket0, order0) =>
            def replacement(repl: IndexMapping): QScriptCore[A] =
              Sort(
                src,
                bucket0 ∘ (remapIndicesInFunc(_, repl)),
                order0.map(_.leftMap(remapIndicesInFunc(_, repl))))
            M.get ∘ (remapState(_, in, indexMapping >>> replacement))

          case Unreferenced() => in.point[M]
        }
    }
}

class PAFindRemap[T, F[_]: Functor](implicit P: PruneArrays[F]) {
  import PATypes._

  type ArrayEnv[A] = EnvT[RewriteState, F, A]

  /** Given an input, we accumulate state and annotate the focus.
   *
   * The state collects the used indices and indicates if the dereferenced
   * array can be pruned. For example, if we deref an array non-statically, we
   * cannot prune it.
   *
   * If the focus is an array that can be pruned, the annotatation is set to
   * the state. Else the annotation is set to `None`.
   */
  def findIndices[M[_]](
      implicit
      R: Recursive.Aux[T, F],
      M: MonadState[M, RewriteState]): CoalgebraM[M, ArrayEnv, T] = t => {
    val ft: F[T] = t.project
    P.find(ft) ∘ (newEnv => EnvT((newEnv, ft)))
  }

  /** Given an annotated input, we produce an output with state.
   *
   * If the previous state provides indices, we remap array dereferences accordingly.
   *
   * If an array has an associated environment, we update the state
   * to be the environment and prune the array.
   */
  def remapIndices[M[_]](
      implicit
      C: Corecursive.Aux[T, F],
      M: MonadState[M, RewriteState]): AlgebraM[M, ArrayEnv, T] = arrenv => {
    val (env, ft): (RewriteState, F[T]) = arrenv.run
    P.remap(env, ft) ∘ (_.embed)
  }
}
