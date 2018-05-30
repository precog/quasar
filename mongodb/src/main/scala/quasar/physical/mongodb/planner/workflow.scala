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
import quasar._
import quasar.contrib.scalaz._
import quasar.fp._
import quasar.fp.ski._
import quasar.fs.MonadFsErr
import quasar.jscore.JsFn
import quasar.physical.mongodb.{BsonField, BsonVersion}
import quasar.physical.mongodb.WorkflowBuilder, WorkflowBuilder.{Subset => _, _}
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner.common._
import quasar.physical.mongodb.planner.exprOp._
import quasar.physical.mongodb.planner.javascript._
import quasar.physical.mongodb.workflow.{ExcludeId => _, IncludeId => _, _}
import quasar.qscript._

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._

object workflow {
  def getStructBuilder
    [T[_[_]]: BirecursiveT: ShowT: RenderTreeT, M[_]: Monad, WF[_]: WorkflowBuilder.Ops[?[_]], EX[_]: Traverse]
    (handler: FreeMap[T] => M[Expr], v: BsonVersion)
    (src: WorkflowBuilder[WF], struct: FreeMap[T], rootKey: BsonField.Name, structKey: BsonField.Name)
    (implicit ev: EX :<: ExprOp): M[WorkflowBuilder[WF]] =
    getFilterBuilder[T, M, WF, EX, Hole](handler, v)(src, struct) >>= { case (source, f) =>
      handler(f) ∘ (fm => DocBuilder(source, ListMap(rootKey -> docVarToExpr(DocVar.ROOT()), structKey -> fm)))
    }

  def getBuilder
    [T[_[_]]: BirecursiveT: ShowT: RenderTreeT, M[_]: Monad: MonadFsErr, WF[_]: WorkflowBuilder.Ops[?[_]], EX[_]: Traverse, A]
    (handler: FreeMapA[T, A] => M[Expr], v: BsonVersion)
    (src: WorkflowBuilder[WF], fm: FreeMapA[T, A])
    (implicit ev: EX :<: ExprOp)
     : M[WorkflowBuilder[WF]] =
    getFilterBuilder[T, M, WF, EX, A](handler, v)(src, fm) >>= { case (src0, fm0) =>
      fm0.project match {
        case MapFuncCore.StaticMap(elems) =>
          elems.traverse(_.bitraverse({
            case Embed(MapFuncCore.EC(ejson.Str(key))) => BsonField.Name(key).point[M]
            case key => raiseInternalError[M, BsonField.Name](s"Unsupported object key: ${key.shows}")
          }, handler)) ∘ (es => DocBuilder(src0, es.toListMap))
        case _ => handler(fm0) ∘ (ExprBuilder(src0, _))
      }
    }

  def getExprBuilder
    [T[_[_]]: BirecursiveT: ShowT: RenderTreeT, M[_]: Monad: ExecTimeR: MonadFsErr, WF[_], EX[_]: Traverse]
    (funcHandler: AlgebraM[M, MapFunc[T, ?], Fix[EX]], staticHandler: StaticHandler[T, EX], v: BsonVersion)
    (src: WorkflowBuilder[WF], fm: FreeMap[T])
    (implicit EX: ExprOpCoreF :<: EX, ev: EX :<: ExprOp, WF: WorkflowBuilder.Ops[WF])
      : M[WorkflowBuilder[WF]] =
    getBuilder[T, M, WF, EX, Hole](handleFreeMap[T, M, EX](funcHandler, staticHandler, _), v)(src, fm)

  def getReduceBuilder
    [T[_[_]]: BirecursiveT: ShowT: RenderTreeT, M[_]: Monad: ExecTimeR: MonadFsErr, WF[_], EX[_]: Traverse]
    (funcHandler: AlgebraM[M, MapFunc[T, ?], Fix[EX]], staticHandler: StaticHandler[T, EX], v: BsonVersion)
    (src: WorkflowBuilder[WF], fm: FreeMapA[T, ReduceIndex])
    (implicit EX: ExprOpCoreF :<: EX, ev: EX :<: ExprOp, WF: WorkflowBuilder.Ops[WF])
      : M[WorkflowBuilder[WF]] =
    getBuilder[T, M, WF, EX, ReduceIndex](handleRedRepair[T, M, EX](funcHandler, staticHandler, _), v)(src, fm)

  def exprOrJs[M[_]: Applicative: MonadFsErr, A]
    (a: A)
    (exf: A => M[Fix[ExprOp]], jsf: A => M[JsFn])
      : M[Expr] = {
    // TODO: Return _both_ errors
    val js = jsf(a)
    val expr = exf(a)
    handleErr[M, Expr](
      (js ⊛ expr)(\&/.Both(_, _)))(
      _ => handleErr[M, Expr](js.map(-\&/))(_ => expr.map(\&/-)))
  }

  def handleFreeMap[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: ExecTimeR: MonadFsErr, EX[_]: Traverse]
    (funcHandler: AlgebraM[M, MapFunc[T, ?], Fix[EX]], staticHandler: StaticHandler[T, EX], fm: FreeMap[T])
    (implicit EX: ExprOpCoreF :<: EX, ev: EX :<: ExprOp)
      : M[Expr] =
    exprOrJs(fm)(getExpr[T, M, EX](funcHandler, staticHandler), getJsFn[T, M])

  def handleRedRepair[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: ExecTimeR: MonadFsErr, EX[_]: Traverse]
    (funcHandler: AlgebraM[M, MapFunc[T, ?], Fix[EX]], staticHandler: StaticHandler[T, EX], jr: FreeMapA[T, ReduceIndex])
    (implicit EX: ExprOpCoreF :<: EX, ev: EX :<: ExprOp)
      : M[Expr] =
    exprOrJs(jr)(getExprRed[T, M, EX](funcHandler, staticHandler), getJsRed[T, M])

  def rebaseWB
    [T[_[_]]: EqualT, M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush, EX[_]: Traverse]
    (cfg: PlannerConfig[T, EX, WF, M],
      free: FreeQS[T],
      src: WorkflowBuilder[WF])
    (implicit
      F: Planner.Aux[T, QScriptTotal[T, ?]],
      ev0: WorkflowOpCoreF :<: WF,
      ev1: RenderTree[WorkflowBuilder[WF]],
      ev2: WorkflowBuilder.Ops[WF],
      ev3: ExprOpCoreF :<: EX,
      ev4: EX :<: ExprOp)
      : M[WorkflowBuilder[WF]] =
    free.cataM(
      interpretM[M, QScriptTotal[T, ?], qscript.Hole, WorkflowBuilder[WF]](κ(src.point[M]), F.plan(cfg)))

}
