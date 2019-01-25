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

import quasar.IdStatus, IdStatus.{ExcludeId, IdOnly, IncludeId}
import quasar.common.effect.NameGenerator
import quasar.contrib.iota._
import quasar.contrib.scalaz.MonadState_
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.ski.κ
import quasar.qscript.{
  construction,
  Hole,
  MFC,
  MonadPlannerErr,
  ReduceFunc}
import quasar.qscript.RecFreeS._
import quasar.qscript.MapFuncCore.{EmptyMap, StaticMap}
import quasar.qscript.provenance.JoinKey
import quasar.qsu.{QScriptUniform => QSU}, QSU.ShiftTarget
import quasar.qsu.ApplyProvenance.AuthenticatedQSU

import matryoshka.{BirecursiveT, ShowT}
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

  def apply[F[_]: Monad: NameGenerator: MonadPlannerErr](aqsu: AuthenticatedQSU[T])
      : F[ResearchedQSU[T]] =
    reifyIdentities[F](gatherReferences(aqsu.graph), aqsu)

  ////

  trait WrapNeed
  final case object NeedValue extends WrapNeed
  final case object NeedBoth extends WrapNeed

  // Whether the result of a vertex includes reified identities.
  private type ReifiedStatus = IMap[Symbol, Option[WrapNeed]]

  private val ONEL = Traverse[Option].compose[NonEmptyList]

  private val O = QSU.Optics[T]
  private val func = construction.Func[T]
  private val recFunc = construction.RecFunc[T]

  private val IdentitiesK: String = "identities"
  private val ValueK: String = "value"
  private val BothK: String = "value"

  private def bucketSymbol(src: Symbol, idx: Int): Symbol =
    Symbol(s"${src.name}_b$idx")

  private def groupKeySymbol(src: Symbol, idx: Int): Symbol =
    Symbol(s"${src.name}_k$idx")

  private def wrapKey(wrap: WrapNeed) = wrap match {
    case NeedValue => ValueK
    case NeedBoth => BothK
  }

  private def lookup(wrap: WrapNeed) =
    func.ProjectKeyS(func.Hole, wrap match {
      case NeedValue => ValueK
      case NeedBoth => BothK
    })

  private val lookupIdentities: FreeMap =
    func.ProjectKeyS(func.Hole, IdentitiesK)

  private def lookupIdentity(src: Symbol): FreeMap =
    func.ProjectKeyS(lookupIdentities, src.name)

  private val lookupBoth: FreeMap =
    lookup(NeedBoth)

  private val defaultAccess: Access[Symbol] => FreeMap = {
    case Access.Id(idAccess, _) => idAccess match {
      case IdAccess.Bucket(src, idx) => lookupIdentity(bucketSymbol(src, idx))
      case IdAccess.GroupKey(src, idx) => lookupIdentity(groupKeySymbol(src, idx))
      case IdAccess.Identity(src) => lookupIdentity(src)
    }
    case Access.Value(_) => func.Hole
  }

  private def bucketIdAccess(src: Symbol, buckets: List[FreeAccess[Hole]]): ISet[Access[Symbol]] =
    Foldable[List].compose[FreeMapA].foldMap(buckets) { access =>
      ISet.singleton(access.symbolic(κ(src)))
    }

  private def shiftTargetAccess(src: Symbol, bucket: FreeMapA[ShiftTarget]): ISet[Access[Symbol]] =
    Foldable[FreeMapA].foldMap(bucket) {
      case ShiftTarget.AccessLeftTarget(access) => ISet.singleton(access.symbolic(κ(src)))
      case _ => ISet.empty
    }

  private def joinKeyAccess(src: Symbol, jk: JoinKey[IdAccess]): IList[Access[Symbol]] =
    IList(jk.left, jk.right) map { idA =>
      Access.id(idA, IdAccess.symbols.headOption(idA) getOrElse src)
    }

  private def recordAccesses[F[_]: Foldable](by: Symbol, fa: F[Access[Symbol]]): References =
    fa.foldLeft(References.noRefs[T])((r, a) => r.recordAccess(by, a, defaultAccess(a)))

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
        val keysAccess = for {
          conj <- joinKeys.keys
          isect <- conj.list
          key <- isect.list
          keyAccess <- joinKeyAccess(g.root, key)
        } yield keyAccess

        recordAccesses(g.root, keysAccess)

      case other => References.noRefs
    })

  private def reifyIdentities[F[_]: Monad: NameGenerator: MonadPlannerErr](
      refs: References,
      aqsu: AuthenticatedQSU[T])
      : F[ResearchedQSU[T]] = {

    import QSUGraph.{Extractors => E}

    type ReifyT[X[_], A] = StateT[X, ReifyState, A]
    type G[A] = ReifyT[F, A]
    val G = MonadState_[G, ReifyState]

    // if the root of the graph contains reified identities
    def emitsIdMap(g: QSUGraph): G[Option[WrapNeed]] =
      G.gets(_.status.lookup(g.root) getOrElse None)

    def freshName: F[Symbol] =
      freshSymbol("rid")

    def isReferenced(access: Access[Symbol]): G[Boolean] =
      G.gets(_.refs.accessed.member(access))

    def includeIdRepair(oldRepair: FreeMapA[ShiftTarget], oldIdStatus: IdStatus)
        : FreeMapA[ShiftTarget] =
      if (oldIdStatus === ExcludeId)
        oldRepair >>= {
          case ShiftTarget.RightTarget => func.ProjectIndexI(RightTarget, 1)
          case tgt => tgt.pure[FreeMapA]
        }
      else oldRepair

    def makeI[F[_]: Foldable, A](assocs: F[(Symbol, FreeMapA[A])]): FreeMapA[A] =
      StaticMap(assocs.toList.map(_.leftMap(s => EJson.str(s.name))))

    def makeI1[A](sym: Symbol, id: FreeMapA[A]): FreeMapA[A] =
      makeI[Id, A](sym -> id)

    def makeIdMap[A](ids: FreeMapA[A], wrapped: FreeMapA[A], wrap: WrapNeed): FreeMapA[A] =
      func.StaticMapS(
        IdentitiesK -> ids,
        wrapKey(wrap) -> wrapped)

    def makeIV[A](initialI: FreeMapA[A], initialV: FreeMapA[A]): FreeMapA[A] =
      makeIdMap(initialI, initialV, NeedValue)

    def makeIB[A](initialI: FreeMapA[A], initialV: FreeMapA[A]): FreeMapA[A] =
      makeIdMap(initialI, initialV, NeedBoth)

    def modifyAccess(of: Access[Symbol])(f: FreeMap => FreeMap): G[Unit] =
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
        nestedAccess = Access.value(nestedRoot)
        recordNewAccess = reifyRefs.modify(_.recordAccess(origRoot, nestedAccess, defaultAccess(nestedAccess)))

        _ <- G.modify(replaceOldAccess >>> recordNewAccess)
      } yield (nestedRoot, newBranch)

    /** Nests a graph in a Map vertex that wraps the original value in the value
      * side of an IV Map.
      */
    def nestBranchValue(branch: QSUGraph, wrap: WrapNeed): G[QSUGraph] =
      for {
        mapped <- mapResultOf(branch, makeIdMap(Free.roll(MFC(EmptyMap[T, FreeMap])), func.Hole, wrap))

        (nestedRoot, newBranch) = mapped

        mapRoot = newBranch.root

        _ <- setStatus(nestedRoot, None)
        _ <- setStatus(mapRoot, Some(wrap))

        modifyValueAccess = reifyRefs.modify(_.modifyAccess(Access.value(mapRoot))(x => rebase(x, wrap)))

        _ <- G.modify(modifyValueAccess)
      } yield newBranch

    /** Handle bookkeeping required when a vertex transitions to emitting IV. */
    def onNeedsIdMap(g: QSUGraph, wrap: WrapNeed): G[Unit] =
      setStatus(g.root, Some(wrap)) >> modifyAccess(Access.value(g.root))(rebase(_, wrap))

    def onNeedsIV(g: QSUGraph): G[Unit] =
      onNeedsIdMap(g, NeedValue)

    /** Preserves any IV emitted by `src` in the output of `through`, returning
      * whether IV was emitted.
      */
    def preserveIdMap(src: QSUGraph, through: QSUGraph): G[Option[WrapNeed]] =
      for {
        srcStatus <- emitsIdMap(src)
        _  <- setStatus(through.root, srcStatus)
        _  <- srcStatus.fold(().point[G]) { wrap =>
          modifyAccess(Access.value(through.root))(x => rebase(x, wrap)) }
      } yield srcStatus

    /** Rebase the given `FreeMap` to access the value side of an IV map. */
    def rebase[A](fm: FreeMapA[A], wrap: WrapNeed): FreeMapA[A] =
      fm >>= (lookup(wrap) as _)

    def recRebase[A](rfm: RecFreeMapA[A], wrap: WrapNeed): RecFreeMapA[A] =
      rfm >>= (r => recFunc.ProjectKeyS(recFunc.Hole, wrapKey(wrap)).as(r))

    def recRebaseV[A](rfm: RecFreeMapA[A]): RecFreeMapA[A] =
      rfm >>= (r => recFunc.ProjectKeyS(recFunc.Hole, ValueK).as(r))

    def rebaseB[A](fm: FreeMapA[A]): FreeMapA[A] =
      fm >>= (lookupBoth as _)

    def recRebaseB[A](rfm: RecFreeMapA[A]): RecFreeMapA[A] =
      rfm >>= (r => recFunc.ProjectKeyS(recFunc.Hole, BothK).as(r))

    def setStatus(root: Symbol, status: Option[WrapNeed]): G[Unit] =
      G.modify(reifyStatus.modify(_.insert(root, status)))

    def updateIdMap[A](src: FreeMapA[A], ids: FreeMapA[A], wrapped: FreeMapA[A], wrap: WrapNeed): FreeMapA[A] =
      makeIdMap(func.ConcatMaps(lookupIdentities >> src, ids), wrapped, wrap)

    def updateIV[A](srcIV: FreeMapA[A], ids: FreeMapA[A], v: FreeMapA[A]): FreeMapA[A] =
      updateIdMap(srcIV, ids, v, NeedValue)

    def updateIB[A](srcIV: FreeMapA[A], ids: FreeMapA[A], b: FreeMapA[A]): FreeMapA[A] =
      updateIdMap(srcIV, ids, b, NeedBoth)

    // Reifies shift identity and reduce bucket access.

    val reifyNonGroupKeys: PartialFunction[QSUGraph, G[QSUGraph]] = {
      case g @ E.Read(file, _) =>
        val idA = Access.id(IdAccess.identity(g.root), g.root)
        isReferenced(idA) flatMap { ref =>
          if (ref) {
            val mapping: FreeMap = makeIB(
              makeI1(g.root, func.ProjectIndexI(func.Hole, 0)),
              func.Hole)

            val newRead = g.overwriteAtRoot(O.read(file, IncludeId))

            for {
              mapped <- mapResultOf(newRead, mapping)

              (nestedRoot, newBranch) = mapped

              mapRoot = newBranch.root

              _ <- setStatus(nestedRoot, None)
              _ <- setStatus(mapRoot, Some(NeedBoth))

              modifyValueAccess = reifyRefs.modify(_.modifyAccess(Access.value(mapRoot))(rebaseB))

              _ <- G.modify(modifyValueAccess)
            } yield newBranch
          } else {
            setStatus(g.root, None) as g
          }
        }
      case g @ E.LPRead(file) =>
        val idA = Access.id(IdAccess.identity(g.root), g.root)
        isReferenced(idA) flatMap { ref =>
          if (ref) {
            val mapping: FreeMap = makeIV(
              makeI1(g.root, func.ProjectIndexI(func.Hole, 0)),
              func.ProjectIndexI(func.Hole, 1))

            val newRead = g.overwriteAtRoot(O.read(file, IncludeId))

            for {
              mapped <- mapResultOf(newRead, mapping)

              (nestedRoot, newBranch) = mapped

              mapRoot = newBranch.root

              _ <- setStatus(nestedRoot, None)
              _ <- setStatus(mapRoot, Some(NeedValue))

              modifyValueAccess = reifyRefs.modify(_.modifyAccess(Access.value(mapRoot))(x => rebase(x, NeedValue)))

              _ <- G.modify(modifyValueAccess)
            } yield newBranch
          } else {
            setStatus(g.root, None) as g.overwriteAtRoot(O.read(file, ExcludeId))
          }
        }
      case g @ E.Distinct(source) =>
        preserveIdMap(source, g) as g

      case g @ E.LeftShift(source, struct, idStatus, onUndefined, repair, rot) =>
        val idA = Access.id(IdAccess.identity(g.root), g.root)

        (emitsIdMap(source) |@| isReferenced(idA)).tupled flatMap {
          case (Some(wrap), true) =>
            onNeedsIdMap(g, wrap) as {
              val (newStatus, newRepair) = idStatus match {
                case IdOnly =>
                  (
                    idStatus,
                    updateIdMap(
                      LeftTarget[T],
                      makeI1(g.root, RightTarget[T]),
                      repair,
                      wrap)
                  )

                case ExcludeId | IncludeId =>
                  (
                    IncludeId : IdStatus,
                    updateIdMap(
                      LeftTarget[T],
                      makeI1(g.root, func.ProjectIndexI(RightTarget[T], 0)),
                      includeIdRepair(repair, idStatus),
                      wrap)
                  )
              }

              g.overwriteAtRoot(
                O.leftShift(source.root, recRebase(struct, wrap), newStatus, onUndefined, newRepair, rot))
            }

          case (Some(wrap), false) =>
            onNeedsIdMap(g, wrap) as {
              val newRepair =
                makeIdMap(lookupIdentities >> LeftTarget[T], repair, wrap)

              g.overwriteAtRoot(
                O.leftShift(source.root, recRebase(struct, wrap), idStatus, onUndefined, newRepair, rot))
            }

          case (None, true) =>
            onNeedsIdMap(g, NeedValue) as {
              val newStatus =
                if (idStatus === ExcludeId) IncludeId else idStatus

              val getId =
                if (idStatus === ExcludeId) func.ProjectIndexI(RightTarget[T], 0) else RightTarget[T]

              val newRepair = makeIV(
                makeI1(g.root, getId),
                includeIdRepair(repair, idStatus))

              g.overwriteAtRoot(O.leftShift(source.root, struct, newStatus, onUndefined, newRepair, rot))
            }

          case (None, false) =>
            setStatus(g.root, None) as g
        }

      case g @ E.Map(source, fm) =>
        preserveIdMap(source, g) map { wrapper =>
          wrapper.fold(g){ w =>
            val newFunc = makeIdMap(lookupIdentities, rebase(fm.linearize, w), w)
            g.overwriteAtRoot(O.map(source.root, newFunc.asRec))
          }
        }

      case g @ E.QSAutoJoin(left, right, keys, combiner) =>
        (emitsIdMap(left) |@| emitsIdMap(right)).tupled flatMap {
          case (None, None) =>
            setStatus(g.root, None) as g
          case (Some(wrap), None) =>
            onNeedsIdMap(g, wrap) as {
              val newCombiner =
                makeIdMap(
                  lookupIdentities >> func.LeftSide,
                  combiner >>= (_.fold(rebase(func.LeftSide, wrap), func.RightSide)),
                  wrap)
              g.overwriteAtRoot(O.qsAutoJoin(left.root, right.root, keys, newCombiner))
            }
          case (None, Some(wrap)) =>
            onNeedsIV(g) as {
              val newCombiner =
                makeIdMap(
                  lookupIdentities >> func.RightSide,
                  combiner >>= (_.fold(func.LeftSide, rebase(func.RightSide, wrap))),
                  wrap)
              g.overwriteAtRoot(O.qsAutoJoin(left.root, right.root, keys, newCombiner))
            }
          case (Some(leftWrap), Some(rightWrap)) =>
            onNeedsIV(g) as {
              val newCombiner =
                makeIV(
                  func.ConcatMaps(
                    lookupIdentities >> func.LeftSide,
                    lookupIdentities >> func.RightSide),
                  combiner >>= (_.fold(rebase(func.LeftSide, leftWrap), rebase(func.RightSide, rightWrap))))

              g.overwriteAtRoot(O.qsAutoJoin(left.root, right.root, keys, newCombiner))
            }

        }
      case g @ E.QSFilter(source, predicate) =>
        preserveIdMap(source, g) map { wrapper =>
          wrapper.fold(g)(w => g.overwriteAtRoot(O.qsFilter(source.root, recRebase(predicate, w))))
        }

      case g @ E.QSReduce(source, buckets, reducers, repair) =>
        val referencedBuckets = buckets.indices.toList.traverse { i =>
          val baccess = IdAccess.Bucket(g.root, i)
          isReferenced(Access.id(baccess, g.root)) map (_ option baccess)
        } map (_.unite.toNel)

        def newReducers(wrap: WrapNeed) =
          Functor[List].compose[ReduceFunc].map(reducers)(x => rebase(x, wrap))

        (emitsIdMap(source) |@| referencedBuckets).tupled flatMap {
          case (Some(wrap), Some(refdBuckets)) =>
            onNeedsIdMap(g, wrap) as {
              val refdIds = makeI(refdBuckets map { ba =>
                bucketSymbol(ba.of, ba.idx) -> func.ReduceIndex(ba.idx.left)
              })
              g.overwriteAtRoot(O.qsReduce(source.root, buckets, newReducers(wrap), makeIdMap(refdIds, repair, wrap)))
            }
          case (optWrap, None) =>
            setStatus(g.root, None) as {
              g.overwriteAtRoot(O.qsReduce(source.root, buckets, optWrap.fold(reducers)(newReducers), repair))
            }
        }

      case g @ E.QSSort(source, buckets, keys) =>
        preserveIdMap(source, g) map { _.fold(g){ wrap =>
          val newKeys = keys map (_ leftMap (a => rebase(a, wrap)))
          g.overwriteAtRoot(O.qsSort(source.root, buckets, newKeys))
        }}
      case g @ E.Subset(from, _, _) =>
        preserveIdMap(from, g) as g

      case g @ E.ThetaJoin(left, right, condition, joinType, combiner) =>
        (emitsIdMap(left) |@| emitsIdMap(right)).tupled flatMap {
          /** FIXME: https://github.com/quasar-analytics/quasar/issues/3114
            *
            * This implementation is not correct, in general, but should produce
            * correct results unless the left and right identity maps both contain
            * an entry for the same key with a different values.
            */
          case (Some(leftWrap), Some(rightWrap)) =>
            onNeedsIdMap(g, NeedValue) as {
              val newCondition =
                condition >>= (_.fold(rebase(func.LeftSide, leftWrap), rebase(func.RightSide, rightWrap)))

              val newCombiner =
                makeIV(
                  func.ConcatMaps(
                    lookupIdentities >> func.LeftSide,
                    lookupIdentities >> func.RightSide),
                  combiner >>= (_.fold(rebase(func.LeftSide, leftWrap), rebase(func.RightSide, rightWrap))))

              g.overwriteAtRoot(O.thetaJoin(left.root, right.root, newCondition, joinType, newCombiner))
            }

          case (Some(wrap), None) =>
            onNeedsIdMap(g, wrap) as {
              val newCondition =
                condition flatMap (_.fold(rebase(func.LeftSide, wrap), func.RightSide))

              val newCombiner =
                makeIdMap(
                  lookupIdentities >> func.LeftSide,
                  combiner >>= (_.fold(rebase(func.LeftSide, wrap), func.RightSide)),
                  wrap)

              g.overwriteAtRoot(O.thetaJoin(left.root, right.root, condition, joinType, newCombiner))
            }

          case (None, Some(wrap)) =>
            onNeedsIdMap(g, wrap) as {
              val newCondition =
                condition flatMap (_.fold(func.LeftSide, rebase(func.RightSide, wrap)))

              val newCombiner =
                makeIdMap(
                  lookupIdentities >> func.RightSide,
                  combiner >>= (_.fold(func.LeftSide, rebase(func.RightSide, wrap))),
                  wrap)
              g.overwriteAtRoot(O.thetaJoin(left.root, right.root, condition, joinType, newCombiner))
            }

          case (None, None) =>
            setStatus(g.root, None) as g
        }

      case g @ E.Union(left, right) =>
        for {
          lstatus <- emitsIdMap(left)
          rstatus <- emitsIdMap(right)

          // If both emit or neither does, we're fine. If they're mismatched
          // then modify the side that doesn't emit to instead emit an empty
          // identities map.
          out <- (lstatus, rstatus) match {
            case (None, None) =>
              setStatus(g.root, None) as g
            case (Some(wrap), None) =>
              nestBranchValue(right, wrap) map { nestedRight =>
                val nestedVertices = left.vertices ++ nestedRight.vertices
                val unionQS = O.union(left.root, nestedRight.root)
                val newVertices = nestedVertices + (g.root -> unionQS)
                QSUGraph(g.root, newVertices)
              }
            case (None, Some(wrap)) =>
              nestBranchValue(left, wrap) map { nestedLeft =>
                val nestedVertices = right.vertices ++ nestedLeft.vertices
                val unionQS = O.union(nestedLeft.root, right.root)
                val newVertices = nestedVertices + (g.root -> unionQS)
                QSUGraph(g.root, newVertices)
              }
            case (Some(leftWrap), Some(rightWrap)) =>
              setStatus(g.root, Some(leftWrap)) as g // TODO, idk what to do here
          }} yield out

      case g @ E.Unreferenced() =>
        setStatus(g.root, None) as g

    }

    val groupKeyA: Optional[Access[Symbol], (Symbol, Int)] =
      Access.id[Symbol] composePrism IdAccess.groupKey.first composeLens _1

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
        optWrap <- emitsIdMap(g)

        indices <- referencedIndices

        assocs <- referencedAssocs(indices, optWrap.fold(func.Hole)(lookup))

        resultG <- assocs.fold(g.point[G]) { as =>
          val (fm, newStatus) = optWrap match {
            case None => (makeIV(makeI(as), func.Hole), NeedValue)
            case Some(wrap) => (updateIdMap(func.Hole, makeI(as), lookup(wrap), wrap), wrap)
          }

          for {
            mapped <- mapResultOf(g, fm)

            (newVert, g2) = mapped

            _ <- setStatus(newVert, optWrap)
            _ <- setStatus(g2.root, Some(newStatus))

            // If the original vertex emitted IV, then need to properly remap
            // access to its new name
            //
            // else we need to remap access to the existing name as it now
            // points to a vertex that emits IV.
            vertexToModify = optWrap.fold(newVert)(x => g2.root)
            _ <- G.modify(reifyRefs.modify(_.modifyAccess(Access.value(vertexToModify))(x => rebase(x, newStatus))))
          } yield g2
        }
      } yield resultG
    }

    val reified =
      aqsu.graph.rewriteM[G](reifyNonGroupKeys andThen (_.flatMap(reifyGroupKeys(aqsu.auth, _))))
        .run(ReifyState(IMap.empty, refs))

    reified flatMap {
      case (ReifyState(status, reifiedRefs), reifiedGraph) =>
        val finalGraph = status.lookup(reifiedGraph.root).flatten.fold(reifiedGraph.point[F]) { wrap =>
          freshName map { newRoot =>
            val QSUGraph(oldRoot, oldVerts) = reifiedGraph
            val updVerts = oldVerts.updated(newRoot, O.map(oldRoot, lookup(wrap).asRec))
            QSUGraph(newRoot, updVerts)
          }}
        finalGraph map (ResearchedQSU(reifiedRefs, _))
    }
  }
}

object ReifyIdentities {
  final case class ResearchedQSU[T[_[_]]](refs: References[T], graph: QSUGraph[T])

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

  def apply[T[_[_]]: BirecursiveT: ShowT, F[_]: Monad: NameGenerator: MonadPlannerErr]
      (aqsu: AuthenticatedQSU[T])
      : F[ResearchedQSU[T]] =
    new ReifyIdentities[T].apply[F](aqsu)
}
