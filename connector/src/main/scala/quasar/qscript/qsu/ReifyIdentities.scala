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

package quasar.qscript.qsu

import slamdata.Predef._

import quasar.NameGenerator
import quasar.contrib.scalaz.MonadState_
import quasar.ejson.EJson
import quasar.fp._
import quasar.fp.ski.κ
import quasar.qscript.{
  construction,
  Hole,
  IdOnly,
  IncludeId,
  ExcludeId,
  LeftSide,
  RightSide,
  MFC,
  ReduceFunc,
  ReduceIndex,
  ReduceIndexF}
import quasar.qscript.MapFuncCore.{EmptyMap, StaticMap}
import quasar.qscript.qsu.{QScriptUniform => QSU}

import matryoshka.BirecursiveT
import monocle.Lens
import scalaz.{Foldable, Free, Functor, IMap, ISet, Monad, NonEmptyList, StateT, Traverse}
import scalaz.Scalaz._

final class ReifyIdentities[T[_[_]]: BirecursiveT] extends QSUTTypes[T] {
  import ReifyIdentities.ResearchedQSU

  def apply[F[_]: Monad: NameGenerator](graph: QSUGraph): F[ResearchedQSU[T]] =
    reifyIdentities[F](gatherReferences(graph), graph)

  ////

  // Whether the result of a vertex includes reified identities.
  private type ReifiedStatus = IMap[Symbol, Boolean]

  private val LRF = Traverse[List].compose[ReduceFunc]

  private val O = QSU.Optics[T]
  private val func = construction.Func[T]

  private val Identities: T[EJson] = EJson.str("identities")
  private val Value: T[EJson] = EJson.str("value")

  private def bucketSymbol(src: Symbol, idx: Int): Symbol =
    Symbol(s"${src.name}_$idx")

  private val lookupValue: FreeMap =
    func.ProjectKey(func.Hole, func.Constant(Value))

  private val lookupIdentities: FreeMap =
    func.ProjectKey(func.Hole, func.Constant(Identities))

  private def lookupIdentity(src: Symbol): FreeMap =
    func.ProjectKey(lookupIdentities, func.Constant(EJson.str(src.name)))

  private val defaultAccess: Access[Symbol] => FreeMap = {
    case Access.Bucket(src, idx, _) => lookupIdentity(bucketSymbol(src, idx))
    case Access.Identity(src, _)    => lookupIdentity(src)
    case Access.Value(_)            => func.Hole
  }

  private def bucketIdAccess(src: Symbol, buckets: List[FreeAccess[Hole]]): ISet[Access[Symbol]] =
    Foldable[List].compose[FreeMapA].foldMap(buckets) { access =>
      ISet.singleton(access.symbolic(κ(src)))
    }

  private def recordAccesses[F[_]: Foldable](by: Symbol, fa: F[Access[Symbol]]): References =
    fa.foldLeft(References.noRefs[T])((r, a) => r.recordAccess(by, a, defaultAccess(a)))

  private final case class ReifyState(status: ReifiedStatus, refs: References) {
    lazy val seen: ISet[Symbol] = status.keySet
  }

  private val reifyStatus: Lens[ReifyState, ReifiedStatus] =
    Lens((_: ReifyState).status)(stats => _.copy(status = stats))

  private val reifyRefs: Lens[ReifyState, References] =
    Lens((_: ReifyState).refs)(rs => _.copy(refs = rs))

  private def gatherReferences(g: QSUGraph): References = {
    g.foldMapUp(g => g.unfold.map(_.root) match {
      case QSU.QSReduce(source, buckets, reducers, _) =>
        recordAccesses(g.root, bucketIdAccess(source, buckets))

      case QSU.QSSort(source, buckets, order) =>
        recordAccesses(g.root, bucketIdAccess(source, buckets))

      case QSU.ThetaJoin(left, right, condition, _, combiner) =>
        val condAccess = condition map { access =>
          access.symbolic(_.fold(left, right))
        }

        recordAccesses(g.root, condAccess)

      case other => References.noRefs
    })
  }

  private def reifyIdentities[F[_]: Monad: NameGenerator](
      refs: References,
      graph: QSUGraph)
      : F[ResearchedQSU[T]] = {

    import QSUGraph.{Extractors => E}

    type ReifyT[X[_], A] = StateT[X, ReifyState, A]
    type G[A] = ReifyT[F, A]
    val G = MonadState_[G, ReifyState]

    def bucketsI(buckets: NonEmptyList[Access.Bucket[Symbol]]): FreeMapA[ReduceIndex] =
      makeI(buckets map (ba => (bucketSymbol(ba.of, ba.idx), ReduceIndexF[T](ba.idx.left))))

    def emitsIVMap(g: QSUGraph): G[Boolean] =
      G.gets(_.status.lookup(g.root) getOrElse false)

    def freshName: F[Symbol] =
      NameGenerator[F].prefixedName("reify") map (Symbol(_))

    def isReferenced(access: Access[Symbol]): G[Boolean] =
      G.gets(_.refs.accessed.member(access))

    def includeIdRepair(oldRepair: JoinFunc, leftSide: JoinFunc): JoinFunc =
      oldRepair flatMap {
        case LeftSide => leftSide
        case RightSide => func.ProjectIndex(func.RightSide, func.Constant(EJson.int(1)))
      }

    def makeI[F[_]: Foldable, A](assocs: F[(Symbol, FreeMapA[A])]): FreeMapA[A] =
      StaticMap(assocs.toList.map(_.leftMap(s => EJson.str(s.name))))

    def makeI1[A](sym: Symbol, id: FreeMapA[A]): FreeMapA[A] =
      makeI[Id, A](sym -> id)

    def makeIV[A](initialI: FreeMapA[A], initialV: FreeMapA[A]): FreeMapA[A] =
      func.ConcatMaps(
        func.MakeMap(func.Constant(Identities), initialI),
        func.MakeMap(func.Constant(Value), initialV))

    def modifyAccess(of: Access[Symbol])(f: FreeMap => FreeMap): G[Unit] =
      G.modify(reifyRefs.modify(_.modifyAccess(of)(f)))

    /** Nests a graph in a Map vertex that wraps the original value in the value
      * side of an IV Map, taking care of all required bookkeeping.
      */
    def nestBranchValue(branch: QSUGraph): G[QSUGraph] =
      for {
        nestedRoot <- freshName.liftM[ReifyT]

        QSUGraph(origRoot, origVerts) = branch

        nestedVerts = origVerts.updated(nestedRoot, origVerts(origRoot))

        newVert = O.map(nestedRoot, makeIV(Free.roll(MFC(EmptyMap[T, FreeMap])), func.Hole))

        newBranch = QSUGraph(origRoot, nestedVerts.updated(origRoot, newVert))

        _ <- setStatus(nestedRoot, false)
        _ <- setStatus(origRoot, true)

        replaceOldAccess = reifyRefs.modify(_.replaceAccess(origRoot, nestedRoot))
        nestedAccess = Access.value(nestedRoot)
        recordNewAccess = reifyRefs.modify(_.recordAccess(origRoot, nestedAccess, defaultAccess(nestedAccess)))
        modifyValueAccess = reifyRefs.modify(_.modifyAccess(Access.value(origRoot))(rebaseV))

        _ <- G.modify(replaceOldAccess >>> recordNewAccess >>> modifyValueAccess)
      } yield newBranch

    def onNeedsIV(g: QSUGraph): G[Unit] =
      setStatus(g.root, true) >> modifyAccess(Access.value(g.root))(rebaseV)

    /** Preserves any IV emitted by `src` in the output of `through`, returning
      * whether IV was emitted.
      */
    def preserveIV(src: QSUGraph, through: QSUGraph): G[Boolean] =
      for {
        srcStatus <- emitsIVMap(src)
        _         <- setStatus(through.root, srcStatus)
        _         <- srcStatus.whenM(modifyAccess(Access.value(through.root))(rebaseV))
      } yield srcStatus

    /** Rebase the given `FreeMap` to access the value side of an IV map. */
    def rebaseV[A](fm: FreeMapA[A]): FreeMapA[A] =
      fm >>= (lookupValue as _)

    def setStatus(root: Symbol, status: Boolean): G[Unit] =
      G.modify(reifyStatus.modify(_.insert(root, status)))

    def srcIVRepair(oldRepair: JoinFunc): JoinFunc =
      oldRepair >>= (_.fold(rebaseV(func.LeftSide), func.RightSide))

    def updateIV[A](srcIV: FreeMapA[A], ids: FreeMapA[A], v: FreeMapA[A]): FreeMapA[A] =
      makeIV(func.ConcatMaps(lookupIdentities >> srcIV, ids), v)

    val reified = graph.rewriteM[G]({

      case g @ E.Distinct(source) =>
        preserveIV(source, g) as g

      case g @ E.LeftShift(source, struct, IdOnly, repair) =>
        emitsIVMap(source).tuple(isReferenced(Access.identity(g.root, g.root))) flatMap {
          case (true, true) =>
            onNeedsIV(g) as {
              val newRepair =
                updateIV(func.LeftSide, makeI1(g.root, func.RightSide), srcIVRepair(repair))

              g.overwriteAtRoot(O.leftShift(source.root, rebaseV(struct), IdOnly, newRepair))
            }

          case (true, false) =>
            onNeedsIV(g) as {
              val newRepair =
                makeIV(lookupIdentities >> func.LeftSide, srcIVRepair(repair))

              g.overwriteAtRoot(O.leftShift(source.root, rebaseV(struct), IdOnly, newRepair))
            }

          case (false, true) =>
            onNeedsIV(g) as {
              val newRepair = makeIV(makeI1(g.root, func.RightSide), repair)
              g.overwriteAtRoot(O.leftShift(source.root, struct, IdOnly, newRepair))
            }

          case (false, false) =>
            setStatus(g.root, false) as g
        }

      case g @ E.LeftShift(source, struct, ExcludeId, repair) =>
        emitsIVMap(source).tuple(isReferenced(Access.identity(g.root, g.root))) flatMap {
          case (true, true) =>
            onNeedsIV(g) as {
              val newRepair =
                updateIV(
                  func.LeftSide,
                  makeI1(g.root, func.ProjectIndex(func.RightSide, func.Constant(EJson.int(0)))),
                  includeIdRepair(repair, rebaseV(func.LeftSide)))

              g.overwriteAtRoot(O.leftShift(source.root, rebaseV(struct), IncludeId, newRepair))
            }

          case (true, false) =>
            onNeedsIV(g) as {
              val newRepair =
                makeIV(lookupIdentities >> func.LeftSide, srcIVRepair(repair))

              g.overwriteAtRoot(O.leftShift(source.root, rebaseV(struct), ExcludeId, newRepair))
            }

          case (false, true) =>
            onNeedsIV(g) as {
              val newRepair =
                makeIV(
                  makeI1(g.root, func.ProjectIndex(func.RightSide, func.Constant(EJson.int(0)))),
                  includeIdRepair(repair, func.LeftSide))

              g.overwriteAtRoot(O.leftShift(source.root, struct, IncludeId, newRepair))
            }

          case (false, false) =>
            setStatus(g.root, false) as g
        }

      case g @ E.Map(source, fm) =>
        preserveIV(source, g) map { emitsIV =>
          if (emitsIV) {
            val newFunc = makeIV(lookupIdentities, rebaseV(fm))
            g.overwriteAtRoot(O.map(source.root, newFunc))
          } else g
        }

      case g @ E.QSFilter(source, p) =>
        preserveIV(source, g) map { emitsIV =>
          if (emitsIV)
            g.overwriteAtRoot(O.qsFilter(source.root, rebaseV(p)))
          else
            g
        }

      case g @ E.QSReduce(source, buckets, reducers, repair) =>
        val referencedBuckets = buckets.indices.toList.traverse { i =>
          val baccess = Access.Bucket(g.root, i, g.root)
          isReferenced(baccess) map (_ option baccess)
        } map (_.unite.toNel)

        val newReducers = emitsIVMap(source) map {
          case true => Functor[List].compose[ReduceFunc].map(reducers)(rebaseV)
          case false => reducers
        }

        newReducers.tuple(referencedBuckets) flatMap {
          case (reds, Some(refdBuckets)) =>
            onNeedsIV(g) as {
              val newRepair = makeIV(bucketsI(refdBuckets), repair)
              g.overwriteAtRoot(O.qsReduce(source.root, buckets, reds, newRepair))
            }

          case (reds, None) =>
            setStatus(g.root, false) as {
              g.overwriteAtRoot(O.qsReduce(source.root, buckets, reds, repair))
            }
        }

      case g @ E.QSSort(source, buckets, keys) =>
        preserveIV(source, g) map { emitsIV =>
          if (emitsIV) {
            val newKeys = keys map (_ leftMap rebaseV)
            g.overwriteAtRoot(O.qsSort(source.root, buckets, newKeys))
          } else g
        }

      // TODO: Is this correct? Could we ever have identity information we need to propagate from `count`?
      case g @ E.Subset(from, _, _) =>
        preserveIV(from, g) as g

      case g @ E.ThetaJoin(left, right, condition, joinType, combiner) =>
        emitsIVMap(left).tuple(emitsIVMap(right)) flatMap {

          // AutoJoin
          // In this case we can merge the identities as, due to AutoJoin
          // semantics, any keys appearing in both sides will have the same
          // value.
          case (true, true)
            if (!condition.empty && condition.all(Access.value.isEmpty)) =>

            onNeedsIV(g) as {
              val newCombiner =
                makeIV(
                  func.ConcatMaps(
                    lookupIdentities >> func.LeftSide,
                    lookupIdentities >> func.RightSide),
                  rebaseV(combiner))

              g.overwriteAtRoot(O.thetaJoin(left.root, right.root, condition, joinType, newCombiner))
            }

          // "Real" ThetaJoin
          // FIXME
          case (true, true) => scala.Predef.???
            // In this case, we need to nest the identities from each side in a new map
            //   { i: {left: ids(left), right: ids(right)}, v: combiner }
            //
            // We then need to determine which ids could possibly exist in each of the sub maps.
            //   + should be able to use the status map's keySet to help here, or we just grab
            //     all the vertices in each branch. From these sets, we can see what possible
            //     ids are related to those vertices.
            //
            // For each set of identity access, we need to update access by any
            // vertices _not_ visited thus far, adding the appropriate projection
            // into the L/R side of the identity map.

          case (true, false) =>
            onNeedsIV(g) as {
              val newCombiner =
                makeIV(
                  lookupIdentities >> func.LeftSide,
                  combiner >>= (_.fold(rebaseV(func.LeftSide), func.RightSide)))

              g.overwriteAtRoot(O.thetaJoin(left.root, right.root, condition, joinType, newCombiner))
            }

          case (false, true) =>
            onNeedsIV(g) as {
              val newCombiner =
                makeIV(
                  lookupIdentities >> func.RightSide,
                  combiner >>= (_.fold(func.LeftSide, rebaseV(func.RightSide))))

              g.overwriteAtRoot(O.thetaJoin(left.root, right.root, condition, joinType, newCombiner))
            }

          case (false, false) =>
            setStatus(g.root, false) as g
        }

      case g @ E.Union(left, right) =>
        for {
          lstatus  <- emitsIVMap(left)
          rstatus  <- emitsIVMap(right)
          _        <- setStatus(g.root, lstatus || rstatus)
          union2   <- (lstatus, rstatus) match {
                        case (true, false) =>
                          nestBranchValue(right) map { nestedRight =>
                            val vs0 = left.vertices ++ nestedRight.vertices
                            val u = O.union(left.root, nestedRight.root)
                            val vs = vs0 + (g.root -> u)
                            QSUGraph(g.root, vs)
                          }

                        case (false, true) =>
                          nestBranchValue(left) map { nestedLeft =>
                            val vs0 = right.vertices ++ nestedLeft.vertices
                            val u = O.union(nestedLeft.root, right.root)
                            val vs = vs0 + (g.root -> u)
                            QSUGraph(g.root, vs)
                          }

                        case _ => g.point[G]
                      }
        } yield union2

      case g @ E.Unreferenced() =>
        setStatus(g.root, false) as g

    }).run(ReifyState(IMap.empty, refs))

    reified flatMap {
      case (ReifyState(status, reifiedRefs), reifiedGraph) =>
        val finalGraph = if (status.lookup(reifiedGraph.root) | false)
          // The root of the graph emits IV, so we need to project out the value.
          freshName map { newRoot =>
            val QSUGraph(oldRoot, oldVerts) = reifiedGraph
            val updVerts = oldVerts.updated(newRoot, O.map(oldRoot, lookupValue))
            QSUGraph(newRoot, updVerts)
          }
        else
          reifiedGraph.point[F]

        finalGraph map (ResearchedQSU(reifiedRefs, _))
    }
  }
}

object ReifyIdentities {
  final case class ResearchedQSU[T[_[_]]](refs: References[T], graph: QSUGraph[T])

  def apply[T[_[_]]: BirecursiveT]: ReifyIdentities[T] =
    new ReifyIdentities[T]
}
