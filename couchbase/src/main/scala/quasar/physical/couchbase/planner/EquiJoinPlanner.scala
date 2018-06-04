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

package quasar.physical.couchbase.planner

import slamdata.Predef._
import quasar.effect.NameGenerator
import quasar.fs.Planner.PlannerErrorME
import quasar.common.JoinType
import quasar.contrib.pathy.AFile
import quasar.fp.ski.κ
import quasar.contrib.iota.copkTraverse
import quasar.fp.{:<<:, ACopK}
import quasar.physical.couchbase._,
  common.{ContextReader, DocTypeValue},
  N1QL.{Eq, Unreferenced, _},
  Select.{Filter, Value, _}
import quasar.qscript, qscript.{MapFuncsCore => mfs, _}

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
import iotaz.CopK

// NB: Only handling a limited simple set of cases to start

final class EquiJoinPlanner[
    T[_[_]]: BirecursiveT: ShowT,
    F[_]: Monad: ContextReader: NameGenerator: PlannerErrorME]
  extends Planner[T, F, EquiJoin[T, ?]] {

  object CShiftedRead {
    def unapply[F[a] <: ACopK[a], A](
      fa: F[A]
    )(implicit
      C: Const[ShiftedRead[AFile], ?] :<<: F
    ): Option[Const[ShiftedRead[AFile], A]] =
      C.prj(fa)
  }

  object MetaGuard {
    def unapply[A](mf: RecFreeMapA[T, A]): Boolean = (
      mf.linearize.resume.swap.toOption >>= { case MFC(mfs.Guard(Meta(), _, _, _)) => ().some; case _ => none }
    ).isDefined

    object Meta {
      def unapply[A](mf: FreeMapA[T, A]): Boolean =
        (mf.resume.swap.toOption >>= { case MFC(mfs.Meta(_)) => ().some; case _ => none }).isDefined
    }
  }

  val QC = CopK.Inject[QScriptCore[T, ?], QScriptTotal[T, ?]]

  object BranchCollection {
    def unapply(qs: FreeQS[T]): Option[DocTypeValue] = (qs match {
      case Embed(CoEnv(\/-(CShiftedRead(c))))                              => c.some
      case Embed(CoEnv(\/-(QC(
        qscript.Filter(Embed(CoEnv(\/-(CShiftedRead(c)))),MetaGuard()))))) => c.some
      case _                                                               => none
    }) ∘ (c =>  common.docTypeValueFromPath(c.getConst.path))
  }

  object KeyMetaId {
    def unapply(mf: FreeMap[T]): Boolean = mf.resume match {
      case -\/(MFC(mfs.ProjectKey(Embed(CoEnv(\/-(MFC(mfs.Meta(_))))), mfs.StrLit("id")))) => true
      case _                                                                               => false
    }
  }

  lazy val tPlan: AlgebraM[F, QScriptTotal[T, ?], T[N1QL]] =
    Planner[T, F, QScriptTotal[T, ?]].plan

  lazy val mfPlan: AlgebraM[F, MapFunc[T, ?], T[N1QL]] =
    Planner.mapFuncPlanner[T, F].plan

  def unimpl[A] =
    unimplemented[F, A]("EquiJoin: Not currently mapped to N1QL's key join")

  def keyJoin(
    branch: FreeQS[T], key: FreeMap[T], combine: JoinFunc[T],
    col: String, side: JoinSide, joinType: LookupJoinType
  ): F[T[N1QL]] =
    for {
      id1 <- genId[T[N1QL], F]
      id2 <- genId[T[N1QL], F]
      ctx <- ContextReader[F].ask
      b   <- branch.cataM(interpretM(κ(N1QL.Null[T[N1QL]]().embed.η[F]), tPlan))
      k   <- key.cataM(interpretM(κ(id1.embed.η[F]), mfPlan))
      c   <- combine.cataM(interpretM({
               case `side` => id1.embed.η[F]
               case _      => Arr(List(N1QL.Null[T[N1QL]]().embed, id2.embed)).embed.η[F]
             }, mfPlan))
    } yield Select(
      Value(true),
      ResultExpr(c, none).wrapNel,
      Keyspace(b, id1.some).some,
      LookupJoin(N1QL.Id(ctx.bucket.v), id2.some, k, joinType).some,
      unnest = none, let = nil,
      Filter(Eq(
        SelectField(id2.embed, Data[T[N1QL]](quasar.Data.Str(ctx.docTypeKey.v)).embed).embed,
        Data[T[N1QL]](quasar.Data.Str(col)).embed).embed).some,
      groupBy = none, orderBy = nil).embed

  def plan: AlgebraM[F, EquiJoin[T, ?], T[N1QL]] = {
    case EquiJoin(
        Embed(Unreferenced()),
        lBranch, BranchCollection(rCol),
        List((lKey, KeyMetaId())),
        joinType, combine) =>
      joinType match {
        case JoinType.Inner     =>
          keyJoin(lBranch, lKey, combine, rCol.v, LeftSide, JoinType.Inner.right)
        case JoinType.LeftOuter =>
          keyJoin(lBranch, lKey, combine, rCol.v, LeftSide, JoinType.LeftOuter.left)
        case _         =>
          unimpl
      }
    case EquiJoin(
        Embed(Unreferenced()),
        BranchCollection(lCol), rBranch,
        List((KeyMetaId(), rKey)),
        joinType, combine) =>
      joinType match {
        case JoinType.Inner     =>
          keyJoin(rBranch, rKey, combine, lCol.v, RightSide, JoinType.Inner.right)
        case JoinType.RightOuter =>
          keyJoin(rBranch, rKey, combine, lCol.v, RightSide, JoinType.LeftOuter.left)
        case _         =>
          unimpl
      }
    case _ =>
      unimpl
  }
}
