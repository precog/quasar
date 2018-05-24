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

package quasar.physical.mongodb.planner

import slamdata.Predef._
import quasar.{RenderTree, RenderTreeT, Type}
import quasar.common.SortDir
import quasar.contrib.matryoshka._
import quasar.contrib.scalaz._
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.ski._
import quasar.fs.{Planner => QPlanner, _}, QPlanner._
import quasar.jscore
import quasar.jscore.JsFn
import quasar.physical.mongodb._
import quasar.physical.mongodb.WorkflowBuilder.{Subset => _, _}
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner.{selector => sel}
import quasar.physical.mongodb.planner.common._
import quasar.physical.mongodb.planner.exprOp._
import quasar.physical.mongodb.planner.workflow._
import quasar.physical.mongodb.planner.javascript._
import quasar.physical.mongodb.planner.selector._
import quasar.physical.mongodb.selector.Selector
import quasar.physical.mongodb.workflow._
import quasar.qscript._

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._

class QScriptCorePlanner[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] extends
    Planner[QScriptCore[T, ?]] {

  import MapFuncsCore._
  import MapFuncCore._
  import QScriptCorePlanner._
  import fixExprOp._

  type IT[G[_]] = T[G]

  def plan
    [M[_]: Monad: ExecTimeR: MonadFsErr,
      WF[_]: Functor: Coalesce: Crush,
      EX[_]: Traverse]
    (cfg: PlannerConfig[T, EX, WF, M])
    (implicit
      ev0: WorkflowOpCoreF :<: WF,
      ev1: RenderTree[WorkflowBuilder[WF]],
      WB: WorkflowBuilder.Ops[WF],
      ev3: ExprOpCoreF :<: EX,
      ev4: EX :<: ExprOp) = {
    case quasar.qscript.Map(src, f) =>
      getExprBuilder[T, M, WF, EX](cfg.funcHandler, cfg.staticHandler, cfg.bsonVersion)(src, f.linearize)
    case LeftShift(src, struct, id, shiftType, onUndef, repair) => {
      def rewriteUndefined[A]: CoMapFuncR[T, A] => Option[CoMapFuncR[T, A]] = {
        case CoEnv(\/-(MFC(Guard(exp, tpe @ Type.FlexArr(_, _, _), exp0, Embed(CoEnv(\/-(MFC(Undefined()))))))))
          if (onUndef === OnUndefined.Emit) =>
            rollMF[T, A](MFC(Guard(exp, tpe, exp0, Free.roll(MFC(MakeArray(Free.roll(MFC(Undefined())))))))).some
        case _ => none
      }

      if (repair.contains(LeftSideF)) {
        val rootKey = BsonField.Name("s")
        val structKey = BsonField.Name("f")

        val exprMerge: JoinFunc[T] => M[Fix[ExprOp]] =
          getExprMerge[T, M, EX](
            cfg.funcHandler, cfg.staticHandler)(DocField(rootKey), DocField(structKey))
        val jsMerge: JoinFunc[T] => M[JsFn] =
          getJsMerge[T, M](
            jscore.Select(jscore.Ident(JsFn.defaultName), rootKey.value),
            jscore.Select(jscore.Ident(JsFn.defaultName), structKey.value))

        val struct0 = struct.linearize.transCata[FreeMap[T]](orOriginal(rewriteUndefined[Hole]))
        val repair0 = repair.transCata[JoinFunc[T]](orOriginal(rewriteUndefined[JoinSide]))

        val src0: M[WorkflowBuilder[WF]] =
          getStructBuilder[T, M, WF, EX](
            handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, _), cfg.bsonVersion)(
            src, struct0, rootKey, structKey)

        src0 >>= (src1 =>
          getBuilder[T, M, WF, EX, JoinSide](
            exprOrJs(_)(exprMerge, jsMerge), cfg.bsonVersion)(
            FlatteningBuilder(
              src1,
              Set(StructureType.mk(shiftType, structKey, id)),
              List(rootKey).some),
            repair0))

      } else {

        val struct0 = struct.linearize.transCata[FreeMap[T]](orOriginal(rewriteUndefined[Hole]))
        val repair0 =
          repair.as[Hole](SrcHole).transCata[FreeMap[T]](orOriginal(rewriteUndefined[Hole])) >>=
            κ(Free.roll(MFC(MapFuncsCore.ProjectKey[T, FreeMap[T]](HoleF[T], MapFuncsCore.StrLit(Keys.wrap)))))

        val wrapKey = BsonField.Name(Keys.wrap)

        getBuilder[T, M, WF, EX, Hole](
          handleFreeMap[T, M, EX](
            cfg.funcHandler, cfg.staticHandler, _), cfg.bsonVersion)(src, struct0) >>= (builder =>
          getBuilder[T, M, WF, EX, Hole](
            handleFreeMap[T, M, EX](
              cfg.funcHandler, cfg.staticHandler, _),
            cfg.bsonVersion)(
            FlatteningBuilder(
              DocBuilder(builder, ListMap(wrapKey -> docVarToExpr(DocVar.ROOT()))),
              Set(StructureType.mk(shiftType, wrapKey, id)),
              List().some),
            repair0))
      }

    }
    case Reduce(src, bucket, reducers, repair) =>
      (bucket.traverse(handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, _)) ⊛
        reducers.traverse(_.traverse(handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, _))))((b, red) => {
          getReduceBuilder[T, M, WF, EX](
            cfg.funcHandler, cfg.staticHandler, cfg.bsonVersion)(
            // TODO: This work should probably be done in `toWorkflow`.
            semiAlignExpr[λ[α => List[ReduceFunc[α]]]](red)(Traverse[List].compose).fold(
              WB.groupBy(
                DocBuilder(
                  src,
                  // FIXME: Doesn’t work with UnshiftMap
                  red.unite.zipWithIndex.map(_.map(i => BsonField.Name(createFieldName("f", i))).swap).toListMap ++
                    b.zipWithIndex.map(_.map(i => BsonField.Name(createFieldName("b", i))).swap).toListMap),
                b.zipWithIndex.map(p => docVarToExpr(DocField(BsonField.Name(createFieldName("b", p._2))))),
                red.zipWithIndex.map(ai =>
                  (BsonField.Name(createFieldName("f", ai._2)),
                    exprOp.accumulator(ai._1.as($field(createFieldName("f", ai._2)))))).toListMap))(
              exprs => WB.groupBy(src,
                b,
                exprs.zipWithIndex.map(ai =>
                  (BsonField.Name(createFieldName("f", ai._2)),
                    exprOp.accumulator(ai._1))).toListMap)),
              repair)
        }).join

    case Sort(src, bucket, order) =>
      val (keys, dirs) = (bucket.toIList.map((_, SortDir.asc)) <::: order).unzip
      keys.traverse(handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, _))
        .map(ks => WB.sortBy(src, ks.toList, dirs.toList))

    case Filter(src0, cond) => {

      def selectors(fm: FreeMap[T]): Output[T] =
        getSelector[T, M, EX, Hole](
          fm,
          defaultSelector[T].some,
          sel.selector[T](cfg.bsonVersion) ∘ (_ <+> defaultSelector[T].some))

      def typeSelectors(fm: FreeMap[T]): Output[T] = getSelector[T, M, EX, Hole](
        fm, none, typeSelector[T])

      def filterBuilder(
        src: WorkflowBuilder[WF],
        partialSel: PartialSelector[T],
        fm: FreeMap[T])
          : M[WorkflowBuilder[WF]] =
        getFilterBuilder.filterBuilder(
          { fm0: FreeMap[T] => handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, fm0) },
          src,
          partialSel,
          fm)

      (selectors(cond.linearize), typeSelectors(cond.linearize)) match {
        case (None, Some(typeSel)) => filterBuilder(src0, typeSel, cond.linearize)
        case (Some(sel), None) => filterBuilder(src0, sel, cond.linearize)
        case (Some(sel), Some(typeSel)) =>
          filterBuilder(src0, typeSel, cond.linearize) >>= (filterBuilder(_, sel, cond.linearize))
        case _ =>
          handleFreeMap[T, M, EX](cfg.funcHandler, cfg.staticHandler, cond.linearize).map {
            // TODO: Postpone decision until we know whether we are going to
            //       need mapReduce anyway.
            case cond @ HasThat(_) => WB.filter(src0, List(cond), {
              case f :: Nil => Selector.Doc(f -> Selector.Eq(Bson.Bool(true)))
            })
            case \&/.This(js) => WB.filter(src0, Nil, {
              case Nil => Selector.Where(js(jscore.ident("this")).toJs)
            })
          }
      }
    }
    case Union(src, lBranch, rBranch) =>
      (rebaseWB[T, M, WF, EX](cfg, lBranch, src) ⊛
        rebaseWB[T, M, WF, EX](cfg, rBranch, src))(
        UnionBuilder(_, _))
    case Subset(src, from, sel, count) =>
      (rebaseWB[T, M, WF, EX](cfg, from, src) ⊛
        (rebaseWB[T, M, WF, EX](cfg, count, src) >>= (HasInt[M, WF](_))))(
        sel match {
          case Drop => WB.skip
          case Take => WB.limit
          // TODO: Better sampling
          case Sample => WB.limit
        })
    case Unreferenced() =>
      CollectionBuilder($pure(Bson.Null), WorkflowBuilder.Root(), none).point[M]
  }
}

object QScriptCorePlanner {
  // TODO: Need `Delay[Show, WorkflowBuilder]`
  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  def HasLiteral[M[_]: Applicative: MonadFsErr, WF[_]]
    (wb: WorkflowBuilder[WF])
    (implicit ev0: WorkflowOpCoreF :<: WF)
      : M[Bson] =
    asLiteral(wb).fold(
      raisePlannerError[M, Bson](NonRepresentableEJson(wb.toString)))(
      _.point[M])

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  def HasInt[M[_]: Monad: MonadFsErr, WF[_]]
    (wb: WorkflowBuilder[WF])
    (implicit ev0: WorkflowOpCoreF :<: WF)
      : M[Long] =
    HasLiteral[M, WF](wb) >>= {
      case Bson.Int32(v) => v.toLong.point[M]
      case Bson.Int64(v) => v.point[M]
      case x => raisePlannerError(NonRepresentableEJson(x.toString))
    }
}
