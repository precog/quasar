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
import quasar.{Data => QData}
import quasar.fs.Planner.{PlannerErrorME, InternalError}
import quasar.effect.NameGenerator
import quasar.ejson
import quasar.fp._
import quasar.fp.ski.κ
import quasar.physical.couchbase._,
  common.ContextReader,
  N1QL.{Id, Union, Unreferenced, _},
  planner.Planner._,
  Select.{Filter, Value, _}, Case._
import quasar.qscript, qscript._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._, NonEmptyList.nels

final class QScriptCorePlanner[
    T[_[_]]: BirecursiveT: ShowT,
    F[_]: Monad: ContextReader: NameGenerator: PlannerErrorME
  ] extends Planner[T, F, QScriptCore[T, ?]] {

  def int(i: Int) = Data[T[N1QL]](QData.Int(i)).embed

  def processFreeMapDefault(f: FreeMap[T], id: Id[T[N1QL]]): F[T[N1QL]] =
    f.cataM(interpretM(κ(id.embed.η[F]), mapFuncPlanner[T, F].plan))

  def processFreeMap(f: FreeMap[T], id: Id[T[N1QL]]): F[T[N1QL]] =
    f.project match {
      case MapFuncCore.StaticMap(elems) =>
        elems.traverse(_.bitraverse(
          // TODO: Revisit String key requirement
          {
            case Embed(MapFuncCore.EC(ejson.Str(key))) =>
              Data[T[N1QL]](QData.Str(key)).embed.η[F]
            case key =>
              PlannerErrorME[F].raiseError[T[N1QL]](
                InternalError.fromMsg(s"Unsupported object key: ${key.shows}"))
          },
          v => processFreeMapDefault(v, id)
        )) ∘ (Obj(_).embed)
      case _ =>
        processFreeMapDefault(f, id)
    }

  def plan: AlgebraM[F, QScriptCore[T, ?], T[N1QL]] = {
    case qscript.Map(src, f) =>
      for {
        id1 <- genId[T[N1QL], F]
        ff  <- processFreeMap(f.linearize, id1)
      } yield Select(
        Value(true),
        ResultExpr(ff, none).wrapNel,
        Keyspace(wrapSelect(src), id1.some).some,
        join    = none,
        unnest  = none,
        let     = nil,
        filter  = none,
        groupBy = none,
        orderBy = nil).embed

    // FIXME: Handle `onUndef`
    case LeftShift(src, struct, id, _, onUndef, repair) =>
      for {
        id1 <- genId[T[N1QL], F]
        id2 <- genId[T[N1QL], F]
        id3 <- genId[T[N1QL], F]
        s   <- struct.linearize.cataM(interpretM(
                 κ(id1.embed.η[F]),
                 mapFuncPlanner[T, F].plan))
        sr  =  ArrRange(int(0), LengthArr(s).embed, none)
        u   <- (id match {
                 case IdOnly =>
                   IfNull(
                     ObjNames(s).embed,
                     sr.embed)
                 case IncludeId =>
                   Case(
                     WhenThen(IsObj(s).embed, s)
                   )(
                     Else(ArrFor(
                       expr   = Arr(List(id3.embed, SelectElem(s, id3.embed).embed)).embed,
                       `var`  = id3.embed,
                       inExpr = sr.embed).embed)
                   )
                 case ExcludeId =>
                   IfNull(ObjValues(s).embed, s)
               }).embed.η[F]
        r   <- repair.cataM(interpretM(
                 {
                   case LeftSide  =>
                     id1.embed.η[F]
                   case RightSide =>
                     Select(
                       Value(true),
                       ResultExpr(id2.embed, none).wrapNel,
                       Keyspace(wrapSelect(src), id1.some).some,
                       join    = none,
                       Unnest(u, id2.some).some,
                       let     = nil,
                       filter  = none,
                       groupBy = none,
                       orderBy = nil).embed.η[F]
                 },
                 mapFuncPlanner[T, F].plan))
      } yield r

    case qscript.Reduce(src, bucket, reducers, repair) =>
      for {
        id1 <- genId[T[N1QL], F]
        id2 <- genId[T[N1QL], F]
        id3 <- genId[T[N1QL], F]
        b   <- processFreeMap(bucket match {
          case Nil => MapFuncsCore.NullLit()
          case _   => MapFuncCore.StaticArray(bucket)
        }, id1)
        red <- reducers.traverse(_.traverse(processFreeMap(_, id1)) >>=
                 reduceFuncPlanner[T, F].plan)
        rep <- repair.cataM(interpretM(
                 _.idx.fold(i => SelectElem(SelectElem(ArrAgg(b).embed, int(0)).embed, int(i)).embed, red(_)).point[F],
                 mapFuncPlanner[T, F].plan))
      } yield {
        val s = Select(
          Value(false),
          ResultExpr(rep, id2.some).wrapNel,
          Keyspace(src, id1.some).some,
          join    = none,
          unnest  = none,
          let     = nil,
          filter  = none,
          GroupBy(b).some,
          orderBy = nil).embed

        Select(
          Value(true),
          ResultExpr(id2.embed, none).wrapNel,
          Keyspace(s, id3.some).some,
          join    = none,
          unnest  = none,
          let     = nil,
          Filter(IsNotNull(id2.embed).embed).some,
          groupBy = none,
          orderBy = nil).embed
      }

    case qscript.Sort(src, bucket, order) =>
      for {
        id1 <- genId[T[N1QL], F]
        id2 <- genId[T[N1QL], F]
        id3 <- genId[T[N1QL], F]
        b   <- processFreeMap(MapFuncCore.StaticArray(bucket), id1)
        o   <- order.traverse { case (or, d) =>
                 (processFreeMap(or, id3) ∘ (OrderBy(_, d)))
               }
      } yield {
        val s =  Select(
           Value(true),
           ResultExpr(ArrAgg(id1.embed).embed, none).wrapNel,
           Keyspace(src, id1.some).some,
           join    = none,
           unnest  = none,
           let     = nil,
           filter  = none,
           GroupBy(IfNull(b, id1.embed).embed).some,
           orderBy = nil).embed

        Select(
          Value(true),
          ResultExpr(id3.embed, none).wrapNel,
          Keyspace(s, id2.some).some,
          join    = none,
          Unnest(id2.embed, id3.some).some,
          let     = nil,
          filter  = none,
          groupBy = none,
          orderBy = o.toList).embed
      }

    case qscript.Filter(src, f) =>
      for {
        id1  <- genId[T[N1QL], F]
        ff   <- processFreeMap(f.linearize, id1)
      } yield Select(
        Value(true),
        ResultExpr(id1.embed, none).wrapNel,
        Keyspace(src, id1.some).some,
        join    = none,
        unnest  = none,
        let     = nil,
        Filter(ff).some,
        groupBy = none,
        orderBy = nil).embed

    case qscript.Union(src, lBranch, rBranch) =>
      for {
        idLB <- genId[T[N1QL], F]
        idRB <- genId[T[N1QL], F]
        lb   <- lBranch.cataM(interpretM(
                  κ(idLB.embed.η[F]),
                  Planner[T, F, QScriptTotal[T, ?]].plan))
        rb   <- rBranch.cataM(interpretM(
                  κ(idRB.embed.η[F]),
                  Planner[T, F, QScriptTotal[T, ?]].plan))
      } yield Union(
        SelectField(src, lb).embed,
        SelectField(src, rb).embed).embed

    case qscript.Subset(src, from, op, count) => op match {
      case Drop   => takeOrDrop(src, from, count.right)
      case Take   => takeOrDrop(src, from, count.left)
      case Sample => unimplemented("Sample")
    }

    case qscript.Unreferenced() =>
      Unreferenced[T[N1QL]]().embed.η[F]
  }

  def takeOrDrop(src: T[N1QL], from: FreeQS[T], takeOrDrop: FreeQS[T] \/ FreeQS[T]): F[T[N1QL]] =
    for {
      id1 <- genId[T[N1QL], F]
      id2 <- genId[T[N1QL], F]
      id3 <- genId[T[N1QL], F]
      id4 <- genId[T[N1QL], F]
      id5 <- genId[T[N1QL], F]
      f   <- from.cataM(interpretM(
               κ(id1.embed.η[F]),
               Planner[T, F, QScriptTotal[T, ?]].plan))
      c   <- takeOrDrop.merge.cataM(interpretM(
               κ(id1.embed.η[F]),
               Planner[T, F, QScriptTotal[T, ?]].plan))
    } yield {
      val s =
        Select(
          Value(false),
          nels(
            ResultExpr(f, id2.some),
            ResultExpr(c, id3.some)),
          Keyspace(src, id1.some).some,
          join    = none,
          unnest  = none,
          let     = nil,
          filter  = none,
          groupBy = none,
          orderBy = nil).embed

      val cnt = SelectElem(id3.embed, int(0)).embed

      val slc =
        takeOrDrop.bimap(
           κ(Slice(
             int(0),
             Least(LengthArr(id2.embed).embed, cnt).embed.some)),
           κ(Slice(cnt, none))
         ).merge.embed

      Select(
        Value(true),
        ResultExpr(id5.embed, none).wrapNel,
        Keyspace(s, id4.some).some,
        join    = none,
        Unnest(SelectElem(id2.embed, slc).embed, id5.some).some,
        let     = nil,
        filter  = none,
        groupBy = none,
        orderBy = nil).embed
    }
}
