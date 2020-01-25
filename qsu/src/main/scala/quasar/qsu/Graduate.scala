/*
 * Copyright 2014–2019 SlamData Inc.
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

import quasar.api.resource.ResourcePath
import quasar.common.effect.NameGenerator
import quasar.common.JoinType
import quasar.contrib.scalaz.MonadReader_
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.ski.κ
import quasar.qscript.{
  construction,
  educatedToTotal,
  Filter,
  Hole,
  HoleF,
  LeftShift,
  JoinSide,
  Map,
  MonadPlannerErr,
  OnUndefined,
  QCE,
  QScriptEducated,
  Read,
  Reduce,
  ReduceFuncs,
  ReduceIndexF,
  ShiftType,
  Sort,
  SrcHole,
  Subset,
  ThetaJoin,
  Union,
  Unreferenced
}
import quasar.qscript.PlannerError.InternalError
import quasar.qsu.{QScriptUniform => QSU}
import quasar.qsu.QSUGraph.QSUPattern
import quasar.qsu.ReifyIdentities.ResearchedQSU
import quasar.qsu.mra.JoinKey

import cats.data.NonEmptySet
import cats.syntax.reducible._
import cats.syntax.set._

import iotaz.CopK

import matryoshka.{Corecursive, BirecursiveT, CoalgebraM, Recursive, ShowT}
import matryoshka.data._
import matryoshka.patterns.CoEnv

import scalaz.{~>, -\/, \/-, \/, Const, Monad, NaturalTransformation, ReaderT}
import scalaz.Scalaz._

final class Graduate[T[_[_]]: BirecursiveT: ShowT] private () extends QSUTTypes[T] {

  type QSE[A] = QScriptEducated[A]

  def apply[F[_]: Monad: MonadPlannerErr: NameGenerator](rqsu: ResearchedQSU[T]): F[T[QSE]] = {
    type G[A] = ReaderT[F, References, A]

    val grad = graduateƒ[G, QSE](None)(NaturalTransformation.refl[QSE])

    Corecursive[T[QSE], QSE]
      .anaM[G, QSUGraph](rqsu.graph)(grad)
      .run(rqsu.refs)
  }

  ////

  private type QSU[A] = QScriptUniform[A]
  private type RefsR[F[_]] = MonadReader_[F, References]

  // We can't use final here due to SI-4440 - it results in warning
  private case class SrcMerge[A, B](src: A, lval: B, rval: B)

  private val func = construction.Func[T]

  private def mergeSources[F[_]: Monad: MonadPlannerErr: NameGenerator: RefsR](
      left: QSUGraph,
      right: QSUGraph): F[SrcMerge[QSUGraph, FreeQS]] = {

    val lvert = left.vertices
    val rvert = right.vertices

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def lub(
        lefts: Set[Symbol],
        rights: Set[Symbol],
        visited: Set[Symbol]): Set[Symbol] = {

      if (lefts.isEmpty || rights.isEmpty) {
        Set()
      } else {
        val lnodes = lefts.map(lvert)
        val rnodes = rights.map(rvert)

        val lexp = lnodes.flatMap(_.foldLeft(Set[Symbol]())(_ + _))
        val rexp = rnodes.flatMap(_.foldLeft(Set[Symbol]())(_ + _))

        val check: Set[Symbol] =
          (lexp intersect visited) union
          (rexp intersect visited) union
          (lexp intersect rexp)

        if (!check.isEmpty)
          check
        else
          lub(lexp, rexp, visited.union(lexp).union(rexp))
      }
    }

    val source: Set[Symbol] = if (left.root === right.root)
      Set(left.root)
    else
      lub(Set(left.root), Set(right.root), Set(left.root, right.root))

    // we merge the vertices in the result, just in case the graphs are
    // additively applied to a common root
    val mergedVertices: QSUVerts[T] = left.vertices ++ right.vertices

    source.headOption match {
      case hole @ Some(root) =>
        for {
          lGrad <- graduateCoEnv[F](hole, left)
          rGrad <- graduateCoEnv[F](hole, right)
        } yield SrcMerge(QSUGraph[T](root, mergedVertices), lGrad, rGrad)

      case None =>
        for {
          lGrad <- graduateCoEnv[F](None, left)
          rGrad <- graduateCoEnv[F](None, right)
          name <- NameGenerator[F].prefixedName("merged")
        } yield {
          val root: Symbol = Symbol(name)

          val newVertices: QSUVerts[T] =
            mergedVertices + (root -> QSU.Unreferenced[T, Symbol]())

          SrcMerge(QSUGraph[T](root, newVertices), lGrad, rGrad)
        }
    }
  }

  private def educate[F[_]: Monad: MonadPlannerErr: NameGenerator: RefsR]
    (pattern: QSUPattern[T, QSUGraph])
      : F[QSE[QSUGraph]] = pattern match {
    case QSUPattern(name, qsu) =>
      val MR = MonadReader_[F, References]

      def holeAs(sym: Symbol): Hole => Symbol =
        κ(sym)

      def resolveAccess[A, B](fa: FreeMapA[A])(ex: A => Access[B] \/ B)(f: B => Symbol)
          : F[FreeMapA[B]] =
        MR.asks(_.resolveAccess[A, B](name, fa)(f)(ex))

      def eqCond(lroot: Symbol, rroot: Symbol): JoinKey[T[EJson], IdAccess] => F[JoinFunc] = {
        case JoinKey.Dynamic(l, r) =>
          for {
            lside <- resolveAccess(func.Hole as Access.id(l, lroot))(_.left)(κ(lroot))
            rside <- resolveAccess(func.Hole as Access.id(r, rroot))(_.left)(κ(rroot))
          } yield func.Eq(lside >> func.LeftSide, rside >> func.RightSide)

        case JoinKey.StaticL(s, r) =>
          resolveAccess(func.Hole as Access.id(r, rroot))(_.left)(κ(rroot)) map { rside =>
            func.Eq(func.Constant(s), rside >> func.RightSide)
          }

        case JoinKey.StaticR(l, s) =>
          resolveAccess(func.Hole as Access.id(l, lroot))(_.left)(κ(lroot)) map { lside =>
            func.Eq(lside >> func.LeftSide, func.Constant(s))
          }
      }

      qsu match {
        case QSU.Read(path, idStatus) =>
          CopK.Inject[Const[Read[ResourcePath], ?], QSE].inj(
            Const(Read(ResourcePath.leaf(path), idStatus))).point[F]

        case QSU.Map(source, fm) =>
          QCE(Map[T, QSUGraph](source, fm)).point[F]

        case QSU.QSFilter(source, fm) =>
          QCE(Filter[T, QSUGraph](source, fm)).point[F]

        case QSU.QSReduce(source, buckets, reducers, repair) =>
          buckets traverse (resolveAccess(_)(_.left)(holeAs(source.root))) map { bs =>
            QCE(Reduce[T, QSUGraph](source, bs, reducers, repair))
          }

        case QSU.LeftShift(source, struct, idStatus, onUndefined, repair, rot) =>
          val shiftType = rot match {
            case QSU.Rotation.FlattenArray | QSU.Rotation.ShiftArray =>
              ShiftType.Array

            case QSU.Rotation.FlattenMap | QSU.Rotation.ShiftMap =>
              ShiftType.Map
          }

          QCE(LeftShift[T, QSUGraph](source, struct, idStatus, shiftType, onUndefined, repair)).point[F]

        case QSU.QSSort(source, buckets, order) =>
          buckets traverse (resolveAccess(_)(_.left)(holeAs(source.root))) map { bs =>
            QCE(Sort[T, QSUGraph](source, bs, order))
          }

        case QSU.Union(left, right) =>
          mergeSources[F](left, right) map {
            case SrcMerge(source, lBranch, rBranch) =>
              QCE(Union[T, QSUGraph](source, lBranch, rBranch))
          }

        case QSU.Subset(from, op, count) =>
          mergeSources[F](from, count) map {
            case SrcMerge(source, fromBranch, countBranch) =>
              QCE(Subset[T, QSUGraph](source, fromBranch, op, countBranch))
          }

        // TODO distinct should be its own node in qscript proper
        case QSU.Distinct(source) =>
          resolveAccess(HoleF map (Access.value(_)))(_.left)(holeAs(source.root)) map { fm =>
            QCE(Reduce[T, QSUGraph](
              source,
              // Bucket by the value
              List(fm),
              // Emit the input verbatim as it may include identities.
              List(ReduceFuncs.Arbitrary(HoleF)),
              ReduceIndexF(\/-(0))))
          }

        case QSU.Unreferenced() =>
          QCE(Unreferenced[T, QSUGraph]()).point[F]

        case QSU.QSAutoJoin(left, right, autojoin, combiner) =>
          val t = func.Constant[JoinSide](EJson.bool(true))

          val condition = autojoin.keys.toSortedSet.toNes.fold(t.point[F]) { jks =>
            val mkEq = eqCond(left.root, right.root)

            val mkConj =
              (_: NonEmptySet[JoinKey[T[EJson], IdAccess]])
                .reduceRightTo(mkEq)((l, r) => r.map(jf => (mkEq(l) |@| jf)(func.And(_, _))))
                .value

            val cond =
              jks.reduceRightTo(mkConj)((l, r0) => r0.map(r => (mkConj(l) |@| r)(func.Or(_, _))))
                .value

            if (autojoin.onUndefined === OnUndefined.Emit)
              cond.map(func.IfUndefined(_, t))
            else
              cond
          }

          (mergeSources[F](left, right) |@| condition) {
            case (SrcMerge(source, lBranch, rBranch), cond) =>
              CopK.Inject[ThetaJoin, QSE].inj(
                ThetaJoin[T, QSUGraph](source, lBranch, rBranch, cond, JoinType.Inner, combiner))
          }

        case QSU.ThetaJoin(left, right, condition, joinType, combiner) =>
          mergeSources[F](left, right) map {
            case SrcMerge(source, lBranch, rBranch) =>
              CopK.Inject[ThetaJoin, QSE].inj(
                ThetaJoin[T, QSUGraph](source, lBranch, rBranch, condition, joinType, combiner))
          }

        case qsu =>
          MonadPlannerErr[F].raiseError(
            InternalError(s"Found an unexpected LP-ish $qsu.", None)) // TODO use Show to print
      }
  }

  private def graduateƒ[F[_]: Monad: MonadPlannerErr: NameGenerator: RefsR, G[_]](
    halt: Option[(Symbol, F[G[QSUGraph]])])(
    lift: QSE ~> G)
      : CoalgebraM[F, G, QSUGraph] = graph => {

    val pattern: QSUPattern[T, QSUGraph] =
      Recursive[QSUGraph, QSUPattern[T, ?]].project(graph)

    def default: F[G[QSUGraph]] = educate[F](pattern).map(lift)

    halt match {
      case Some((name, output)) =>
        pattern match {
          case QSUPattern(`name`, _) => output
          case _ => default
        }
      case None => default
    }
  }

  private def graduateCoEnv[F[_]: Monad: MonadPlannerErr: NameGenerator: RefsR]
    (hole: Option[Symbol], graph: QSUGraph)
      : F[FreeQS] = {
    type CoEnvTotal[A] = CoEnv[Hole, QScriptTotal, A]

    val halt: Option[(Symbol, F[CoEnvTotal[QSUGraph]])] =
      hole.map((_, CoEnv.coEnv[Hole, QScriptTotal, QSUGraph](-\/(SrcHole)).point[F]))

    val lift: QSE ~> CoEnvTotal =
      educatedToTotal[T].inject andThen PrismNT.coEnv[QScriptTotal, Hole].reverseGet

    Corecursive[FreeQS, CoEnvTotal].anaM[F, QSUGraph](graph)(
      graduateƒ[F, CoEnvTotal](halt)(lift))
  }
}

object Graduate {
  def apply[
      T[_[_]]: BirecursiveT: ShowT,
      F[_]: Monad: MonadPlannerErr: NameGenerator]
      (rqsu: ResearchedQSU[T])
      : F[T[QScriptEducated[T, ?]]] =
    taggedInternalError("Graduate", new Graduate[T].apply[F](rqsu))
}
