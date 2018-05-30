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
import quasar.RenderTreeT
import quasar.contrib.matryoshka._
import quasar.fp.ski._
import quasar.physical.mongodb.{BsonVersion, WorkflowBuilder}, WorkflowBuilder._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner.{selector => sel}
import quasar.physical.mongodb.planner.selector._
import quasar.physical.mongodb.selector.Selector
import quasar.qscript._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._

object getFilterBuilder {

  def filterBuilder[T[_[_]], M[_]: Applicative, WF[_], A](
    handler: FreeMapA[T, A] => M[Expr],
    src: WorkflowBuilder[WF],
    partialSel: PartialSelector[T],
    fm: FreeMapA[T, A])
    (implicit WB: WorkflowBuilder.Ops[WF])
      : M[WorkflowBuilder[WF]] = {
    val (sel, inputs) = partialSel

    inputs.traverse(f => handler(f(fm))) ∘ (WB.filter(src, _, sel))
  }

  /* Given a handler of type FreeMapA[T, A] => Expr, a FreeMapA[T, A]
   *  and a source WorkflowBuilder, return a new WorkflowBuilder
   *  filtered according to the `Cond`s found in the FreeMapA[T, A].
   *  The result is tupled with whatever remains to be planned out
   *  of the FreeMapA[T, A]
   */
  def apply
    [T[_[_]]: BirecursiveT: ShowT: RenderTreeT, M[_]: Monad, WF[_], EX[_]: Traverse, A]
    (handler: FreeMapA[T, A] => M[Expr], v: BsonVersion)
    (src: WorkflowBuilder[WF], fm: FreeMapA[T, A])
    (implicit ev: EX :<: ExprOp, WB: WorkflowBuilder.Ops[WF])
     : M[(WorkflowBuilder[WF], FreeMapA[T, A])] = {

    import MapFuncCore._
    import MapFuncsCore._

    def elideCond: CoMapFuncR[T, A] => Option[CoMapFuncR[T, A]] = {
      case CoEnv(\/-(MFC(Cond(if_, then_, Embed(CoEnv(\/-(MFC(Undefined())))))))) =>
        CoEnv(then_.resume.swap).some
      case _ => none
    }

    def toCofree[B](ann: B): Algebra[CoEnv[A, MapFunc[T, ?], ?], Cofree[MapFunc[T, ?], B]] =
      interpret(κ(Cofree(ann, MFC(Undefined()))), attributeAlgebra[MapFunc[T, ?], B](κ(ann)))

    val undefinedF: MapFunc[T, Cofree[MapFunc[T, ?], Boolean] \/ FreeMapA[T, A]] = MFC(Undefined())

    val gcoalg: GCoalgebra[Cofree[MapFunc[T, ?], Boolean] \/ ?, EnvT[Boolean, MapFunc[T, ?], ?], FreeMapA[T, A]] =
      _.fold(κ(envT(false, undefinedF)), {
        case MFC(Cond(if_, then_, undef @ Embed(CoEnv(\/-(MFC(Undefined())))))) =>
          envT(false, MFC(Cond(if_.cata(toCofree(true)).left, then_.cata(toCofree(false)).left, undef.cata(toCofree(false)).left)))

        case otherwise => envT(false, otherwise ∘ (_.right))
      })

    val galg: GAlgebra[(Cofree[MapFunc[T, ?], Boolean], ?), EnvT[Boolean, MapFunc[T, ?], ?], Output[T]] = { node =>
      def forgetAnn: Cofree[MapFunc[T, ?], Boolean] => T[MapFunc[T, ?]] = _.transCata[T[MapFunc[T, ?]]](_.lower)

      node.runEnvT match {
        case (true, wa) =>
          /* The `selector` algebra requires one side of a comparison
           * to be a Constant. The comparisons present inside Cond do
           * not necessarily have this shape, hence the decorator
           */
          sel.selector[T](v).apply(wa.map { case (tree, sl) => (forgetAnn(tree), sl) }) <+> (wa match {
            case MFC(Eq(_, _))
               | MFC(Neq(_, _))
               | MFC(Lt(_, _))
               | MFC(Lte(_, _))
               | MFC(Gt(_, _))
               | MFC(Gte(_, _))
               | MFC(Undefined()) => defaultSelector[T].some
            /** The cases here don't readily imply selectors, but
              *  still need to be handled in case a `Cond` is nested
              *  inside one of these.  For instance, if ConcatMaps
              *  includes `Cond`s in BOTH maps, these are extracted
              *  and `Or`ed using Selector.Or. In the case of unary
              *  `MapFunc`s, we simply need to fix the InputFinder to
              *  look in the right place.
              */
            case MFC(MakeMap((_, _), (_, v))) => v.map { case (sel, inputs) => (sel, inputs.map(There(1, _))) }
            case MFC(ConcatMaps((_, lhs), (_, rhs))) => invoke2Rel(lhs, rhs)(Selector.Or(_, _))
            case MFC(Guard((_, if_), _, _, _)) => if_.map { case (sel, inputs) => (sel, inputs.map(There(0, _))) }
            case _ => none
          })

        case (false, wa) => wa match {
          case MFC(MakeMap((_, _), (_, v))) => v.map { case (sel, inputs) => (sel, inputs.map(There(1, _))) }
          case MFC(ProjectKey((_, v), _)) => v.map { case (sel, inputs) => (sel, inputs.map(There(0, _))) }
          case MFC(ConcatMaps((_, lhs), (_, rhs))) => invoke2Rel(lhs, rhs)(Selector.Or(_, _))
          case MFC(Guard((_, if_), _, _, _)) => if_.map { case (sel, inputs) => (sel, inputs.map(There(0, _))) }
          case MFC(Cond((_, pred), _, _)) => pred.map { case (sel, inputs) => (sel, inputs.map(There(0, _))) }
          case _ => none
        }
      }
    }

    val sels: Option[PartialSelector[T]] =
      fm.ghylo[(Cofree[MapFunc[T, ?], Boolean], ?), Cofree[MapFunc[T, ?], Boolean] \/ ?]
        [EnvT[Boolean, MapFunc[T, ?], ?], Output[T]](distPara, distApo, galg, gcoalg).toOption

    (sels ∘ (filterBuilder(handler, src, _, fm))).cata(
      _ strengthR fm.transCata[FreeMapA[T, A]](orOriginal(elideCond)),
      (src, fm).point[M])
  }
}
