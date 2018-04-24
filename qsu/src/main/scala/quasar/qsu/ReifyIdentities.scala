/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.qsu

import slamdata.Predef._

import quasar.effect.NameGenerator
import quasar.fs.Planner.PlannerErrorME
import quasar.contrib.scalaz.MonadState_
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.ski.κ
import quasar.qscript.{
  construction,
  ExcludeId,
  Hole,
  IdOnly,
  IdStatus,
  IncludeId,
  MFC,
  ReduceFunc}
import quasar.qscript.RecFreeS._
import quasar.qscript.MapFuncCore.{EmptyMap, StaticMap}
import quasar.qscript.provenance.JoinKey
import quasar.qsu.{QScriptUniform => QSU}, QSU.ShiftTarget
import quasar.qsu.ApplyProvenance.AuthenticatedQSU

import matryoshka.{showTShow, BirecursiveT, ShowT}
import monocle.{Lens, Optional}
import monocle.syntax.fields._1
import scalaz.{Cord, Foldable, Free, Functor, IList, IMap, ISet, Monad, NonEmptyList, Show, StateT, Traverse}
import scalaz.Scalaz._

/** TODO
  * With smarter structural MapFunc simplification, we could just
  * ConcatMaps(LeftSide, <value>) when preserving identities instead of reconstructing
  * the identity key, however this currently defeats the mini structural evaluator
  * that simplifies things like ProjectKey(MakeMap(foo, <bar>), foo) => bar.
  */
final class ReifyIdentities[T[_[_]]: BirecursiveT: ShowT] private () extends QSUTTypes[T] {
  import ReifyIdentities.ResearchedQSU

  def apply[F[_]: Monad: NameGenerator: PlannerErrorME](aqsu: AuthenticatedQSU[T]): F[ResearchedQSU[T]] =
    reifyIdentities[F](gatherReferences(aqsu.graph), aqsu)

  ////

  // Whether the result of a vertex includes reified identities.
  private type ReifiedStatus = IMap[Symbol, Boolean]

  private val ONEL = Traverse[Option].compose[NonEmptyList]

  private val O = QSU.Optics[T]
  private val func = construction.Func[T]
  private val recFunc = construction.RecFunc[T]

  private val IdentitiesK: String = "identities"
  private val ValueK: String = "value"

  private def bucketSymbol(src: Symbol, idx: Int): Symbol =
    Symbol(s"${src.name}_b$idx")

  private def groupKeySymbol(src: Symbol, idx: Int): Symbol =
    Symbol(s"${src.name}_k$idx")

  private val lookupValue: FreeMap =
    func.ProjectKeyS(func.Hole, ValueK)

  private val lookupIdentities: FreeMap =
    func.ProjectKeyS(func.Hole, IdentitiesK)

  private def lookupIdentity(src: Symbol): FreeMap =
    func.ProjectKeyS(lookupIdentities, src.name)

  private val defaultAccess: QAccess[Symbol] => FreeMap = {
    case Access.Id(idAccess, _) => idAccess match {
      case IdAccess.Bucket(src, idx) => lookupIdentity(bucketSymbol(src, idx))
      case IdAccess.GroupKey(src, idx) => lookupIdentity(groupKeySymbol(src, idx))
      case IdAccess.Identity(src) => lookupIdentity(src)
      case IdAccess.Static(ejs) => func.Constant(ejs)
    }
    case Access.Value(_) => func.Hole
  }

  private def bucketIdAccess(src: Symbol, buckets: List[FreeAccess[Hole]]): ISet[QAccess[Symbol]] =
    Foldable[List].compose[FreeMapA].foldMap(buckets) { access =>
      ISet.singleton(access.symbolic(κ(src)))
    }

  private def shiftTargetAccess(src: Symbol, bucket: FreeMapA[ShiftTarget[T]]): ISet[QAccess[Symbol]] =
    Foldable[FreeMapA].foldMap(bucket) {
      case ShiftTarget.AccessLeftTarget(access) => ISet.singleton(access.symbolic(κ(src)))
      case _ => ISet.empty
    }

  private def joinKeyAccess(src: Symbol, jk: JoinKey[QIdAccess]): IList[QAccess[Symbol]] =
    IList(jk.left, jk.right) map { idA =>
      Access.id(idA, IdAccess.symbols.headOption(idA) getOrElse src)
    }

  private def recordAccesses[F[_]: Foldable](by: Symbol, fa: F[QAccess[Symbol]]): References =
    fa.foldLeft(References.noRefs[T, T[EJson]])((r, a) => r.recordAccess(by, a, defaultAccess(a)))

  // We can't use final here due to SI-4440 - it results in warning
  private case class ReifyState(status: ReifiedStatus, refs: References) {
    lazy val seen: ISet[Symbol] = status.keySet
  }

  private val reifyStatus: Lens[ReifyState, ReifiedStatus] =
    Lens((_: ReifyState).status)(stats => _.copy(status = stats))

  private val reifyRefs: Lens[ReifyState, References] =
    Lens((_: ReifyState).refs)(rs => _.copy(refs = rs))

  private def gatherReferences(g: QSUGraph): References =
    g.foldMapUp(g => g.unfold.map(_.root) match {
      case QSU.Distinct(source) =>
        recordAccesses[Id](g.root, Access.value(source))

      case QSU.LeftShift(source, _, _, _, repair, _) =>
        recordAccesses(g.root, shiftTargetAccess(source, repair))

      case QSU.QSReduce(source, buckets, reducers, _) =>
        recordAccesses(g.root, bucketIdAccess(source, buckets))

      case QSU.QSSort(source, buckets, order) =>
        recordAccesses(g.root, bucketIdAccess(source, buckets))

      case QSU.QSAutoJoin(left, right, joinKeys, combiner) =>
        val keysAccess = joinKeys.keys >>= (_.list) >>= (joinKeyAccess(g.root, _))
        recordAccesses(g.root, keysAccess)

      case other => References.noRefs
    })

  private def reifyIdentities[F[_]: Monad: NameGenerator: PlannerErrorME](
      refs: References,
      aqsu: AuthenticatedQSU[T])
      : F[ResearchedQSU[T]] = {

    import QSUGraph.{Extractors => E}

    type ReifyT[X[_], A] = StateT[X, ReifyState, A]
    type G[A] = ReifyT[F, A]
    val G = MonadState_[G, ReifyState]

    def emitsIVMap(g: QSUGraph): G[Boolean] =
      G.gets(_.status.lookup(g.root) getOrElse false)

    def freshName: F[Symbol] =
      freshSymbol("rid")

    def isReferenced(access: QAccess[Symbol]): G[Boolean] =
      G.gets(_.refs.accessed.member(access))

    def includeIdRepair(oldRepair: FreeMapA[ShiftTarget[T]], oldIdStatus: IdStatus): FreeMapA[ShiftTarget[T]] =
      if (oldIdStatus === ExcludeId)
        oldRepair >>= {
          case ShiftTarget.RightTarget() => func.ProjectIndexI(RightTarget[T], 1)
          case tgt => tgt.pure[FreeMapA]
        }
      else oldRepair

    def makeI[F[_]: Foldable, A](assocs: F[(Symbol, FreeMapA[A])]): FreeMapA[A] =
      StaticMap(assocs.toList.map(_.leftMap(s => EJson.str(s.name))))

    def makeI1[A](sym: Symbol, id: FreeMapA[A]): FreeMapA[A] =
      makeI[Id, A](sym -> id)

    def makeIV[A](initialI: FreeMapA[A], initialV: FreeMapA[A]): FreeMapA[A] =
      func.StaticMapS(
        IdentitiesK -> initialI,
        ValueK -> initialV)

    def modifyAccess(of: QAccess[Symbol])(f: FreeMap => FreeMap): G[Unit] =
      G.modify(reifyRefs.modify(_.modifyAccess(of)(f)))

    /** Returns a new graph that applies `func` to the result of `g`. */
    def mapResultOf(g: QSUGraph, func: FreeMap): G[(Symbol, QSUGraph)] =
      for {
        nestedRoot <- freshName.liftM[ReifyT]

        QSUGraph(origRoot, origVerts) = g

        nestedVerts = origVerts.updated(nestedRoot, origVerts(origRoot))

        newVert = O.map(nestedRoot, func.asRec)

        newBranch = QSUGraph(origRoot, nestedVerts.updated(origRoot, newVert))

        replaceOldAccess = reifyRefs.modify(_.replaceAccess(origRoot, nestedRoot))
        nestedAccess = Access.value[T[EJson], Symbol](nestedRoot)
        recordNewAccess = reifyRefs.modify(_.recordAccess(origRoot, nestedAccess, defaultAccess(nestedAccess)))

        _ <- G.modify(replaceOldAccess >>> recordNewAccess)
      } yield (nestedRoot, newBranch)

    /** Nests a graph in a Map vertex that wraps the original value in the value
      * side of an IV Map.
      */
    def nestBranchValue(branch: QSUGraph): G[QSUGraph] =
      for {
        mapped <- mapResultOf(branch, makeIV(Free.roll(MFC(EmptyMap[T, FreeMap])), func.Hole))

        (nestedRoot, newBranch) = mapped

        mapRoot = newBranch.root

        _ <- setStatus(nestedRoot, false)
        _ <- setStatus(mapRoot, true)

        modifyValueAccess = reifyRefs.modify(_.modifyAccess(Access.value(mapRoot))(rebaseV))

        _ <- G.modify(modifyValueAccess)
      } yield newBranch

    /** Handle bookkeeping required when a vertex transitions to emitting IV. */
    def onNeedsIV(g: QSUGraph): G[Unit] =
      setStatus(g.root, true) >> modifyAccess(Access.valueSymbol(g.root))(rebaseV)

    /** Preserves any IV emitted by `src` in the output of `through`, returning
      * whether IV was emitted.
      */
    def preserveIV(src: QSUGraph, through: QSUGraph): G[Boolean] =
      for {
        srcStatus <- emitsIVMap(src)
        _         <- setStatus(through.root, srcStatus)
        _         <- srcStatus.whenM(modifyAccess(Access.valueSymbol(through.root))(rebaseV))
      } yield srcStatus

    /** Rebase the given `FreeMap` to access the value side of an IV map. */
    def rebaseV[A](fm: FreeMapA[A]): FreeMapA[A] =
      fm >>= (lookupValue as _)

    def recRebaseV[A](rfm: RecFreeMapA[A]): RecFreeMapA[A] =
      rfm >>= (r => recFunc.ProjectKeyS(recFunc.Hole, ValueK).as(r))

    def setStatus(root: Symbol, status: Boolean): G[Unit] =
      G.modify(reifyStatus.modify(_.insert(root, status)))

    def updateIV[A](srcIV: FreeMapA[A], ids: FreeMapA[A], v: FreeMapA[A]): FreeMapA[A] =
      makeIV(func.ConcatMaps(lookupIdentities >> srcIV, ids), v)


    // Reifies shift identity and reduce bucket access.
    val reifyNonGroupKeys: PartialFunction[QSUGraph, G[QSUGraph]] = {

      case g @ E.Distinct(source) =>
        preserveIV(source, g) as g

      case g @ E.LeftShift(source, struct, idStatus, onUndefined, repair, rot) =>
        val idA = Access.id(IdAccess.identity[T[EJson]](g.root), g.root)

        (emitsIVMap(source) |@| isReferenced(idA)).tupled flatMap {
          case (true, true) =>
            onNeedsIV(g) as {
              val (newStatus, newRepair) = idStatus match {
                case IdOnly =>
                  (
                    idStatus,
                    updateIV(
                      LeftTarget[T],
                      makeI1(g.root, RightTarget[T]),
                      repair)
                  )

                case ExcludeId | IncludeId =>
                  (
                    IncludeId : IdStatus,
                    updateIV(
                      LeftTarget[T],
                      makeI1(g.root, func.ProjectIndexI(RightTarget[T], 0)),
                      includeIdRepair(repair, idStatus))
                  )
              }

              g.overwriteAtRoot(
                O.leftShift(source.root, recRebaseV(struct), newStatus, onUndefined, newRepair, rot))
            }

          case (true, false) =>
            onNeedsIV(g) as {
              val newRepair =
                makeIV(lookupIdentities >> LeftTarget[T], repair)

              g.overwriteAtRoot(
                O.leftShift(source.root, recRebaseV(struct), idStatus, onUndefined, newRepair, rot))
            }

          case (false, true) =>
            onNeedsIV(g) as {
              val newStatus =
                if (idStatus === ExcludeId) IncludeId else idStatus

              val getId =
                if (idStatus === ExcludeId) func.ProjectIndexI(RightTarget[T], 0) else RightTarget[T]

              val newRepair = makeIV(
                makeI1(g.root, getId),
                includeIdRepair(repair, idStatus))

              g.overwriteAtRoot(O.leftShift(source.root, struct, newStatus, onUndefined, newRepair, rot))
            }

          case (false, false) =>
            setStatus(g.root, false) as g
        }

      case g @ E.Map(source, fm) =>
        preserveIV(source, g) map { emitsIV =>
          if (emitsIV) {
            val newFunc = makeIV(lookupIdentities, rebaseV(fm.linearize))

            g.overwriteAtRoot(O.map(source.root, newFunc.asRec))
          } else g
        }

      case g @ E.QSAutoJoin(left, right, keys, combiner) =>
        (emitsIVMap(left) |@| emitsIVMap(right)).tupled flatMap {
          case (true, true) =>
            onNeedsIV(g) as {
              val newCombiner =
                makeIV(
                  func.ConcatMaps(
                    lookupIdentities >> func.LeftSide,
                    lookupIdentities >> func.RightSide),
                  rebaseV(combiner))

              g.overwriteAtRoot(O.qsAutoJoin(left.root, right.root, keys, newCombiner))
            }

          case (true, false) =>
            onNeedsIV(g) as {
              val newCombiner =
                makeIV(
                  lookupIdentities >> func.LeftSide,
                  combiner >>= (_.fold(rebaseV(func.LeftSide), func.RightSide)))

              g.overwriteAtRoot(O.qsAutoJoin(left.root, right.root, keys, newCombiner))
            }

          case (false, true) =>
            onNeedsIV(g) as {
              val newCombiner =
                makeIV(
                  lookupIdentities >> func.RightSide,
                  combiner >>= (_.fold(func.LeftSide, rebaseV(func.RightSide))))

              g.overwriteAtRoot(O.qsAutoJoin(left.root, right.root, keys, newCombiner))
            }

          case (false, false) =>
            setStatus(g.root, false) as g
        }

      case g @ E.QSFilter(source, predicate) =>
        preserveIV(source, g) map { emitsIV =>
          if (emitsIV)
            g.overwriteAtRoot(O.qsFilter(source.root, recRebaseV(predicate)))
          else
            g
        }

      case g @ E.QSReduce(source, buckets, reducers, repair) =>
        val referencedBuckets = buckets.indices.toList.traverse { i =>
          val baccess = IdAccess.Bucket[T[EJson]](g.root, i)
          isReferenced(Access.id(baccess, g.root)) map (_ option baccess)
        } map (_.unite.toNel)

        val newReducers = emitsIVMap(source) map {
          case true => Functor[List].compose[ReduceFunc].map(reducers)(rebaseV)
          case false => reducers
        }

        (newReducers |@| referencedBuckets).tupled flatMap {
          case (reds, Some(refdBuckets)) =>
            onNeedsIV(g) as {
              val refdIds = makeI(refdBuckets map { ba =>
                bucketSymbol(ba.of, ba.idx) -> func.ReduceIndex(ba.idx.left)
              })

              g.overwriteAtRoot(O.qsReduce(source.root, buckets, reds, makeIV(refdIds, repair)))
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
        (emitsIVMap(left) |@| emitsIVMap(right)).tupled flatMap {
          /** FIXME: https://github.com/quasar-analytics/quasar/issues/3114
            *
            * This implementation is not correct, in general, but should produce
            * correct results unless the left and right identity maps both contain
            * an entry for the same key with a different values.
            */
          case (true, true) =>
            onNeedsIV(g) as {
              val newCondition =
                condition >>= (lookupValue as _)

              val newCombiner =
                makeIV(
                  func.ConcatMaps(
                    lookupIdentities >> func.LeftSide,
                    lookupIdentities >> func.RightSide),
                  rebaseV(combiner))

              g.overwriteAtRoot(O.thetaJoin(left.root, right.root, newCondition, joinType, newCombiner))
            }

          case (true, false) =>
            onNeedsIV(g) as {
              val newCondition =
                condition flatMap (_.fold(rebaseV(func.LeftSide), func.RightSide))

              val newCombiner =
                makeIV(
                  lookupIdentities >> func.LeftSide,
                  combiner >>= (_.fold(rebaseV(func.LeftSide), func.RightSide)))

              g.overwriteAtRoot(O.thetaJoin(left.root, right.root, condition, joinType, newCombiner))
            }

          case (false, true) =>
            onNeedsIV(g) as {
              val newCondition =
                condition flatMap (_.fold(func.LeftSide, rebaseV(func.RightSide)))

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
          lstatus <- emitsIVMap(left)
          rstatus <- emitsIVMap(right)

          _ <- setStatus(g.root, lstatus || rstatus)

          // If both emit or neither does, we're fine. If they're mismatched
          // then modify the side that doesn't emit to instead emit an empty
          // identities map.
          union2 <- (lstatus, rstatus) match {
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
    }

    val groupKeyA: Optional[QAccess[Symbol], (Symbol, Int)] =
      Access.id[T[EJson], Symbol] composePrism IdAccess.groupKey.first composeLens _1

    // Reifies group key access.
    def reifyGroupKeys(auth: QAuth, g: QSUGraph): G[QSUGraph] = {

      def referencedAssocs(indices: IList[Int], valueAccess: FreeMap)
          : G[Option[NonEmptyList[(Symbol, FreeMap)]]] =
        ONEL.traverse(indices.toNel) { i =>
          auth.lookupGroupKeyE[G](g.root, i) map { k =>
            (groupKeySymbol(g.root, i), k >> valueAccess)
          }
        }

      val referencedIndices =
        G.gets(_.refs.accessed.foldlWithKey(IList[Int]()) { (indices, k, _) =>
          val idx = groupKeyA.getOption(k) collect {
            case (sym, idx) if sym === g.root => idx
          }

          idx.fold(indices)(_ :: indices)
        })

      for {
        srcIV <- emitsIVMap(g)

        indices <- referencedIndices

        assocs <- referencedAssocs(indices, srcIV.fold(lookupValue, func.Hole))

        resultG <- assocs.fold(g.point[G]) { as =>
          val fm =
            if (srcIV)
              updateIV(func.Hole, makeI(as), lookupValue)
            else
              makeIV(makeI(as), func.Hole)

          for {
            mapped <- mapResultOf(g, fm)

            (newVert, g2) = mapped

            _ <- setStatus(newVert, srcIV)
            _ <- setStatus(g2.root, true)

            // If the original vertex emitted IV, then need to properly remap
            // access to its new name
            //
            // else we need to remap access to the existing name as it now
            // points to a vertex that emits IV.
            vertexToModify = srcIV.fold(newVert, g2.root)
            _ <- G.modify(reifyRefs.modify(_.modifyAccess(Access.value(vertexToModify))(rebaseV)))
          } yield g2
        }
      } yield resultG
    }

    val reified =
      aqsu.graph.rewriteM[G](reifyNonGroupKeys andThen (_.flatMap(reifyGroupKeys(aqsu.auth, _))))
        .run(ReifyState(IMap.empty, refs))

    reified flatMap {
      case (ReifyState(status, reifiedRefs), reifiedGraph) =>
        val finalGraph = if (status.lookup(reifiedGraph.root) | false)
          // The root of the graph emits IV, so we need to project out the value.
          freshName map { newRoot =>
            val QSUGraph(oldRoot, oldVerts) = reifiedGraph
            val updVerts = oldVerts.updated(newRoot, O.map(oldRoot, lookupValue.asRec))
            QSUGraph(newRoot, updVerts)
          }
        else
          reifiedGraph.point[F]

        finalGraph map (ResearchedQSU(reifiedRefs, _))
    }
  }
}

object ReifyIdentities {
  final case class ResearchedQSU[T[_[_]]](refs: References[T, T[EJson]], graph: QSUGraph[T])

  object ResearchedQSU {
    implicit def show[T[_[_]]: ShowT]: Show[ResearchedQSU[T]] =
      Show.show { rqsu =>
        Cord("ResearchedQSU\n======\n") ++
        rqsu.graph.show ++
        Cord("\n\n") ++
        rqsu.refs.show ++
        Cord("\n======")
      }
}

  def apply[T[_[_]]: BirecursiveT: ShowT, F[_]: Monad: NameGenerator: PlannerErrorME]
      (aqsu: AuthenticatedQSU[T])
      : F[ResearchedQSU[T]] =
    new ReifyIdentities[T].apply[F](aqsu)
}
