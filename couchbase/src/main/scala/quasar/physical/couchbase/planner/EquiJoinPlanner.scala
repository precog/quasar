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

package quasar.physical.couchbase.planner

import slamdata.Predef._
import quasar.common.{JoinType, PhaseResultT}
import quasar.connector.PlannerErrT
import quasar.contrib.pathy.AFile
import quasar.contrib.scalaz.eitherT._
import quasar.ejson
import quasar.fp.ski.κ
import quasar.NameGenerator
import quasar.physical.couchbase._,
  common.{ContextReader, DocTypeValue},
  N1QL.{Eq, Unreferenced, _},
  Select.{Filter, Value, _}
import quasar.qscript, qscript.{MapFuncs => mfs, _}, MapFunc.StaticArray

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._

// NB: Only handling a limited simple set of cases to start

final class EquiJoinPlanner[T[_[_]]: BirecursiveT: ShowT, F[_]: Monad: ContextReader: NameGenerator]
  extends Planner[T, F, EquiJoin[T, ?]] {

  object CShiftedRead {
    def unapply[F[_], A](
      fa: F[A]
    )(implicit
      C: Const[ShiftedRead[AFile], ?] :<: F
    ): Option[Const[ShiftedRead[AFile], A]] =
      C.prj(fa)
  }

  object MetaGuard {
    def unapply[A](mf: FreeMapA[T, A]): Boolean = (
      mf.resume.swap.toOption >>= { case mfs.Guard(Meta(), _, _, _) => ().some; case _ => none }
    ).isDefined

    object Meta {
      def unapply[A](mf: FreeMapA[T, A]): Boolean =
        (mf.resume.swap.toOption >>= { case mfs.Meta(_) => ().some; case _ => none }).isDefined
    }
  }

  val QC = Inject[QScriptCore[T, ?], QScriptTotal[T, ?]]

  object BranchCollection {
    def unapply(qs: FreeQS[T]): Option[DocTypeValue] = (qs match {
      case Embed(CoEnv(\/-(CShiftedRead(c))))                              => c.some
      case Embed(CoEnv(\/-(QC(
        qscript.Filter(Embed(CoEnv(\/-(CShiftedRead(c)))),MetaGuard()))))) => c.some
      case _                                                               => none
    }) ∘ (c =>  common.docTypeValueFromPath(c.getConst.path))
  }

  object KeyMetaId {
    def unapply(mf: FreeMap[T]): Boolean = mf match {
      case Embed(StaticArray(v :: Nil)) => v.resume match {
        case -\/(mfs.ProjectField(src, field)) => (src.resume, field.resume) match {
          case (-\/(mfs.Meta(_)), -\/(mfs.Constant(Embed(MapFunc.EC(ejson.Str(v2)))))) => true
          case _                                                                       => false
        }
        case v => false
      }
      case _ => false
    }
  }

  lazy val tPlan: AlgebraM[M, QScriptTotal[T, ?], T[N1QL]] =
    Planner[T, F, QScriptTotal[T, ?]].plan

  lazy val mfPlan: AlgebraM[M, MapFunc[T, ?], T[N1QL]] =
    Planner.mapFuncPlanner[T, F].plan

  def unimpl[F[_]: Applicative, A] =
    unimplementedP[F, A]("EquiJoin: Not currently mapped to N1QL's key join")

  def keyJoin(
    branch: FreeQS[T], key: FreeMap[T], combine: JoinFunc[T],
    col: String, side: JoinSide, joinType: LookupJoinType
  ): M[T[N1QL]] =
    for {
      id1 <- genId[T[N1QL], M]
      id2 <- genId[T[N1QL], M]
      ctx <- ContextReader[F].ask.liftM[PhaseResultT].liftM[PlannerErrT]
      b   <- branch.cataM(interpretM(κ(N1QL.Null[T[N1QL]]().embed.η[M]), tPlan))
      k   <- key.cataM(interpretM(κ(id1.embed.η[M]), mfPlan))
      c   <- combine.cataM(interpretM({
               case `side` => id1.embed.η[M]
               case _      => Arr(List(N1QL.Null[T[N1QL]]().embed, id2.embed)).embed.η[M]
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

  def plan: AlgebraM[M, EquiJoin[T, ?], T[N1QL]] = {
    case EquiJoin(
        Embed(Unreferenced()),
        lBranch, BranchCollection(rCol),
        lKey, KeyMetaId(),
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
        KeyMetaId(), rKey,
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
