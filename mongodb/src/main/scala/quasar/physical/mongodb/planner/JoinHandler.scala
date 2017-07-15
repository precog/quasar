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

package quasar.physical.mongodb.planner

import slamdata.Predef._
import quasar.Planner.PlannerError
import quasar.RenderTree
import quasar.common.JoinType
import quasar.contrib.scalaz._
import quasar.fp._
import quasar.fp.ski._
import quasar.javascript._
import quasar.jscore, jscore.{JsCore, JsFn}
import quasar.physical.mongodb._, WorkflowBuilder._
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.workflow._
import quasar.qscript.ExcludeId
import quasar.sql.JoinDir

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._

final case class JoinHandler[WF[_], F[_]](run: (JoinType, JoinSource[WF], JoinSource[WF]) => F[WorkflowBuilder[WF]]) {

  def apply(tpe: JoinType, left: JoinSource[WF], right: JoinSource[WF]): F[WorkflowBuilder[WF]] =
    run(tpe, left, right)
}

final case class JoinSource[WF[_]](src: Fix[WorkflowBuilderF[WF, ?]], keys: List[Expr])

object JoinHandler {

  // FIXME: these have to match the names used in the logical plan. Should
  //        change this to ensure left0/right0 are `Free` and pull the names
  //        from those.
  val LeftName: BsonField.Name = BsonField.Name(JoinDir.Left.name)
  val RightName: BsonField.Name = BsonField.Name(JoinDir.Right.name)

  // NB: it's only safe to emit "core" expr ops here, but we always use the
  // largest type in WorkflowOp, so they're immediately injected into ExprOp.
  import fixExprOp._

  def fallback[WF[_], F[_]: Monad](
      first: JoinHandler[WF, OptionT[F, ?]],
      second: JoinHandler[WF, F]): JoinHandler[WF, F] =
    JoinHandler((tpe, l, r) => first(tpe, l, r) getOrElseF second(tpe, l, r))

  def padEmpty(side: BsonField): Fix[ExprOp] =
    $cond($eq($size($var(DocField(side))), $literal(Bson.Int32(0))),
      $literal(Bson.Arr(List(Bson.Doc()))),
      $var(DocField(side)))

  // TODO: Generate `Both` not `That`.
  // TODO: Modify this to include the flattening, and on 3.2+, use
  //       `preserveNullAndEmptyArrays: true` rather than `padEmpty`.
  def buildProjection[WF[_]]
    (src: WorkflowBuilder[WF],
      leftField: BsonField.Name, l: BsonField.Name => Fix[ExprOp],
      rightField: BsonField.Name, r: BsonField.Name => Fix[ExprOp])
      : WorkflowBuilder[WF] =
    DocBuilder(src, ListMap(
      leftField -> \&/-(l(leftField)),
      rightField -> \&/-(r(rightField))))

  /** When possible, plan a join using the more efficient \$lookup operator in
    * the aggregation pipeline.
    */
  def pipeline[M[_]: Monad, WF[_]: Functor: Coalesce: Crush: Crystallize]
    (stats: Collection => Option[CollectionStatistics],
      indexes: Collection => Option[Set[Index]])
    (implicit
      M: MonadError_[M, PlannerError],
      C: Classify[WF],
      ev0: WorkflowOpCoreF :<: WF,
      ev1: WorkflowOp3_2F :<: WF,
      ev2: RenderTree[WorkflowBuilder[WF]],
      ev3: ExprOpOps.Uni[ExprOp])
    : JoinHandler[WF, OptionT[M, ?]] = JoinHandler({ (tpe, left, right) =>

    val WB = WorkflowBuilder.Ops[WF]

    /** True if the collection is definitely known to be unsharded. */
    def unsharded(coll: Collection): Boolean =
      stats(coll).cata(!_.sharded, false)

    /** Check for an index which can be used for lookups on a certain field. The
      * field must appear first in the key for the index to apply.
      */
    def indexed(coll: Collection, field: BsonField): Boolean =
      indexes(coll).cata(
        _.exists(_.primary ≟ field),
        false)

    // TODO: Make this `AlgebraM`?
    def wfSourceDb: Algebra[WF, Option[DatabaseName]] = {
      case ev0($ReadF(Collection(db, _))) => db.some
      case op                             => C.pipeline(op).flatMap(_.src)
      // TODO: deal with non-pipeline sources where it's possible to identify the DB
    }

    def sourceDb: Algebra[WorkflowBuilderF[WF, ?], Option[DatabaseName]] = {
      case CollectionBuilderF(op, _, _)       => op.cata(wfSourceDb)
      case DocBuilderF(src, _)                => src
      case ExprBuilderF(src, _)               => src
      case FlatteningBuilderF(src, _)         => src
      case GroupBuilderF(src, _, _)           => src
      case ShapePreservingBuilderF(src, _, _) => src
      case UnionBuilderF(lSrc, rSrc)          => if (lSrc ≟ rSrc) lSrc else none
    }

    def lookup(
      lSrc: WorkflowBuilder[WF], lKey: Expr, lName: BsonField.Name,
      rColl: CollectionName, rField: BsonField, rName: BsonField.Name) = {

      // NB: filtering on the left prior to the join is not strictly necessary,
      // but it avoids a runtime explosion in the common error case that the
      // field referenced on one side of the condition is not found.
      def filterExists(wb: WorkflowBuilder[WF], field: BsonField): WorkflowBuilder[WF] =
        WB.filter(wb,
          List(docVarToExpr(DocField(field))),
          { case List(f) => Selector.Doc(f -> Selector.Exists(true)) })

      lKey match {
        case HasThat($var(DocVar(_, Some(lField)))) =>
          val left = WB.makeObject(lSrc, lName.asText)
          val filtered = filterExists(left, lName \ lField)
          generateWorkflow[M, WF](filtered).map { case (left, _) =>
            CollectionBuilder(
              chain[Fix[WF]](
                left,
                $lookup(rColl, lName \ lField, rField, rName)),
              Root(),
              None)
          }

        case _ =>
          for {
            t <- generateWorkflow[M, WF](
              DocBuilder(lSrc, ListMap(
                lName               -> docVarToExpr(DocVar.ROOT()),
                BsonField.Name("0") -> lKey)))
            (src, _) = t
          } yield
              DocBuilder(
                CollectionBuilder(
                  chain[Fix[WF]](
                    src,
                    $lookup(rColl, BsonField.Name("0"), rField, rName)),
                  Root(),
                  None),
                ListMap(
                  lName -> docVarToExpr(DocField(lName)),
                  rName -> docVarToExpr(DocField(rName))))
      }
    }

    (tpe, left, right) match {
      case (JoinType.Inner, JoinSource(src, List(key)), IsLookupFrom(coll, field))
            if unsharded(coll) && indexed(coll, field) && src.cata(sourceDb) ≟ coll.database.some =>
        lookup(
          src, key, LeftName,
          coll.collection, field, RightName).liftM[OptionT] ∘
          (FlatteningBuilder(_, Set(StructureType.Array(DocField(RightName), ExcludeId))))

      case (JoinType.LeftOuter, JoinSource(src, List(key)), IsLookupFrom(coll, field))
            if unsharded(coll) && indexed(coll, field) && src.cata(sourceDb) ≟ coll.database.some =>
        lookup(
          src, key, LeftName,
          coll.collection, field, RightName).liftM[OptionT] ∘
          (look =>
            FlatteningBuilder(
              buildProjection(look, LeftName, n => $var(DocField(n)), RightName, padEmpty),
              Set(StructureType.Array(DocField(RightName), ExcludeId))))

      case (JoinType.Inner, IsLookupFrom(coll, field), JoinSource(src, List(key)))
            if unsharded(coll) && indexed(coll, field) && src.cata(sourceDb) ≟ coll.database.some =>
        lookup(
          src, key, RightName,
          coll.collection, field, LeftName).liftM[OptionT] ∘
          (FlatteningBuilder(_, Set(StructureType.Array(DocField(LeftName), ExcludeId))))

      case (JoinType.RightOuter, IsLookupFrom(coll, field), JoinSource(src, List(key)))
            if unsharded(coll) && indexed(coll, field) && src.cata(sourceDb) ≟ coll.database.some =>
        lookup(
          src, key, RightName,
          coll.collection, field, LeftName).liftM[OptionT] ∘
          (look =>
            FlatteningBuilder(
              buildProjection(look, LeftName, padEmpty, RightName, n => $var(DocField(n))),
              Set(StructureType.Array(DocField(LeftName), ExcludeId))))

      case _ => OptionT.none
    }
  })

  object IsLookupFrom {
    /** Matches a source which is suitable to be the "right" side of a
      * \$lookup-based join; this has to be a \$project of a single field from
      * a raw \$read.
      */
    def unapply[F[_]](arg: JoinSource[F])
      (implicit ev0: WorkflowOpCoreF :<: F, ev1: Equal[Fix[WorkflowBuilderF[F, ?]]])
      : Option[(Collection, BsonField)] =
      arg match {
        case JoinSource(
              Fix(CollectionBuilderF(Fix(ev0($ReadF(coll))), Root(), None)),
              List(HasThat($var(DocVar(_, Some(field)))))) =>
          (coll, field).some
        case _ =>
          None
      }
  }

  /** Plan an arbitrary join using only "core" operators, which always means a
    * map-reduce.
    */
  def mapReduce[M[_]: Monad, WF[_]: Functor: Coalesce: Crush: Crystallize]
    (implicit M: MonadError[M, PlannerError], ev0: WorkflowOpCoreF :<: WF, ev1: RenderTree[WorkflowBuilder[WF]], ev2: ExprOpOps.Uni[ExprOp])
    : JoinHandler[WF, M] = JoinHandler({ (tpe, left0, right0) =>

    val WB = WorkflowBuilder.Ops[WF]

    val ops = Ops[WF]
    import ops._

    val leftField0 = LeftName
    val rightField0 = RightName

    def keyMap(base: Base, keyExpr: List[JsFn], rootField: BsonField.Name, otherField: BsonField.Name): Js.AnonFunDecl =
      $MapF.mapKeyVal(("key", "value"),
        keyExpr match {
          case Nil => Js.Null
          case _   =>
            jscore.Obj(keyExpr.map(_(base.toDocVar.toJs(jscore.ident("value")))).zipWithIndex.foldLeft[ListMap[jscore.Name, JsCore]](ListMap[jscore.Name, JsCore]()) {
              case (acc, (j, i)) => acc + (jscore.Name(i.toString) -> j)
            }).toJs
        },
        Js.AnonObjDecl(List(
          (otherField.asText, Js.AnonElem(Nil)),
          (rootField.asText, Js.AnonElem(List(base.toDocVar.toJs(jscore.ident("value")).toJs))))))

    def jsReduce(src: WorkflowBuilder[WF], key: List[JsFn],
                 rootField: BsonField.Name, otherField: BsonField.Name):
        (WorkflowBuilder[WF], Base => FixOp[WF]) =
      (src, b => $map[WF](keyMap(b, key, rootField, otherField), ListMap()))

    def wbReduce
      (src: WorkflowBuilder[WF], key: List[Expr],
        rootField: BsonField.Name, otherField: BsonField.Name)
        : (WorkflowBuilder[WF], Base => FixOp[WF]) =
      (DocBuilder(
        // TODO: If we can identify cases where the group key _is_ the `_id`
        //       field, then we can avoid the `$group` / `$unwind` on whichever
        //       side that applies to, since we know there’s only one entry per
        //       `_id`.
        groupBy(src, key, ListMap(BsonField.Name("0") -> $push($$ROOT))),
        ListMap(
          rootField             -> \&/-($field("0")),
          otherField            -> \&/-($literal(Bson.Arr())),
          BsonField.Name("_id") -> \&/-($include()))),
        // FIXME: Don’t ignore the base.
        _ => ι)

    val (left, right, leftField, rightField) =
      (left0.keys.map(HasThis.unapply).sequence, right0.keys.map(HasThis.unapply).sequence) match {
        case (Some(js), _)
            if preferMapReduce(left0.src) && !preferMapReduce(right0.src) =>
          (wbReduce(right0.src, right0.keys, rightField0, leftField0),
            jsReduce(left0.src, js, leftField0, rightField0),
            rightField0, leftField0)
        case (_, Some(js)) =>
          (wbReduce(left0.src, left0.keys, leftField0, rightField0),
            jsReduce(right0.src, js, rightField0, leftField0),
            leftField0, rightField0)
        case (Some(js), _) =>
          (wbReduce(right0.src, right0.keys, rightField0, leftField0),
            jsReduce(left0.src, js, leftField0, rightField0),
            rightField0, leftField0)
        case (None, None) =>
          (wbReduce(left0.src, left0.keys, leftField0, rightField0),
            wbReduce(right0.src, right0.keys, rightField0, leftField0),
            leftField0, rightField0)
      }

    // TODO: Do we get any benefit from pre-filtering the join before unwinding?
    //       We should check.
    val nonEmpty: Selector.SelectorExpr = Selector.NotExpr(Selector.Size(0))

    def buildJoin(src: WorkflowBuilder[WF], tpe: JoinType): WorkflowBuilder[WF] =
      tpe match {
        case JoinType.FullOuter =>
          FlatteningBuilder(
            buildProjection(src, leftField, padEmpty, rightField, padEmpty),
            Set(
              StructureType.Array(DocField(leftField), ExcludeId),
              StructureType.Array(DocField(rightField), ExcludeId)))
        case JoinType.LeftOuter =>
          FlatteningBuilder(
            buildProjection(
              WB.filter(src,
                List(docVarToExpr(DocField(leftField))),
                { case l :: Nil => Selector.Doc(ListMap(l -> nonEmpty)) }),
              leftField, n => $var(DocField(n)),
              rightField, padEmpty),
            Set(
              StructureType.Array(DocField(leftField), ExcludeId),
              StructureType.Array(DocField(rightField), ExcludeId)))
        case JoinType.RightOuter =>
          FlatteningBuilder(
            buildProjection(
              WB.filter(
                src,
                List(docVarToExpr(DocField(rightField))),
                { case r :: Nil => Selector.Doc(ListMap(r -> nonEmpty)) }),
              leftField, padEmpty,
              rightField, n => $var(DocField(n))),
            Set(
              StructureType.Array(DocField(leftField), ExcludeId),
              StructureType.Array(DocField(rightField), ExcludeId)))
        case JoinType.Inner =>
          FlatteningBuilder(
            WB.filter(
              src,
              List(
                docVarToExpr(DocField(leftField)),
                docVarToExpr(DocField(rightField))),
              {
                case l :: r :: Nil =>
                  Selector.Doc(ListMap(l -> nonEmpty, r -> nonEmpty))
              }),
            Set(
              StructureType.Array(DocField(leftField), ExcludeId),
              StructureType.Array(DocField(rightField), ExcludeId)))
      }

    val rightReduce = {
      import Js._

      AnonFunDecl(List("key", "values"),
        List(
          VarDef(List(("result",
            AnonObjDecl(List(
              (leftField.asText, AnonElem(Nil)),
              (rightField.asText, AnonElem(Nil))))))),
          Call(Select(Ident("values"), "forEach"),
            List(AnonFunDecl(List("value"),
              // TODO: replace concat here with a more efficient operation
              //      (push or unshift)
              List(
                BinOp("=",
                  Select(Ident("result"), leftField.asText),
                  Call(Select(Select(Ident("result"), leftField.asText), "concat"),
                    List(Select(Ident("value"), leftField.asText)))),
                BinOp("=",
                  Select(Ident("result"), rightField.asText),
                  Call(Select(Select(Ident("result"), rightField.asText), "concat"),
                    List(Select(Ident("value"), rightField.asText)))))))),
          Return(Ident("result"))))
    }

    (generateWorkflow[M, WF](left._1) |@| generateWorkflow[M, WF](right._1)) {
      case ((l, lb), (r, rb)) =>
        buildJoin(
          CollectionBuilder(
            $foldLeft[WF](
              left._2(lb)(l),
              chain(r, right._2(rb), $reduce[WF](rightReduce, ListMap()))),
            Root(),
            None),
          tpe)
    }
  })

  // TODO: This is an approximation. If we could postpone this decision until
  //      `Workflow.crush`, when we actually have a task (whether aggregation or
  //       mapReduce) in hand, we would know for sure.
  private def preferMapReduce[WF[_]: Coalesce: Crush: Crystallize: Functor](wb: WorkflowBuilder[WF])
    (implicit ev0: WorkflowOpCoreF :<: WF, ev1: RenderTree[WorkflowBuilder[WF]], ev2: ExprOpOps.Uni[ExprOp])
    : Boolean = {
    // TODO: Get rid of this when we functorize WorkflowTask
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def checkTask(wt: workflowtask.WorkflowTask): Boolean = wt match {
      case workflowtask.FoldLeftTask(_, _)     => true
      case workflowtask.MapReduceTask(_, _, _) => true
      case workflowtask.PipelineTask(src, _)   => checkTask(src)
      case _                                   => false
    }

    generateWorkflow[PlannerError \/ ?, WF](wb).fold(
      κ(false),
      wf => checkTask(task(Crystallize[WF].crystallize(wf._1))))
  }
}
