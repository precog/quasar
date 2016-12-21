/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._
import quasar.RenderTree
import quasar.fp._
import quasar.fp.ski._
import quasar.javascript._
import quasar.jscore, jscore.{JsCore, JsFn}
import quasar.std.StdLib._
import quasar.physical.mongodb._
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.workflow._
import WorkflowBuilder._
import quasar.sql.JoinDir

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._

final case class JoinHandler[WF[_], F[_]](run: (JoinType, JoinSource[WF], JoinSource[WF]) => F[WorkflowBuilder[WF]]) {

  def apply(tpe: JoinType, left: JoinSource[WF], right: JoinSource[WF]): F[WorkflowBuilder[WF]] =
    run(tpe, left, right)
}

final case class JoinSource[WF[_]](src: Fix[WorkflowBuilderF[WF, ?]], keys: List[WorkflowBuilder[WF]], js: Option[List[JsFn]])

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

  /** When possible, plan a join using the more efficient \$lookup operator in
    * the aggregation pipeline.
    */
  def pipeline[WF[_]: Functor: Coalesce: Crush: Crystallize]
    (stats: Collection => Option[CollectionStatistics],
      indexes: Collection => Option[Set[Index]])
    (implicit
      C: Classify[WF],
      ev0: WorkflowOpCoreF :<: WF,
      ev1: WorkflowOp3_2F :<: WF,
      ev2: RenderTree[WorkflowBuilder[WF]],
      ev3: ExprOpOps.Uni[ExprOp])
    : JoinHandler[WF, OptionT[WorkflowBuilder.M, ?]] = JoinHandler({ (tpe, left, right) =>

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

    def wfSourceDb: Algebra[WF, Option[DatabaseName]] = {
      case $read(Collection(db, _)) => db.some
      case op => C.pipeline(op).flatMap(_.src)
      // TODO: deal with non-pipeline sources where it's possible to identify the DB
    }

    def sourceDb: Algebra[WorkflowBuilderF[WF, ?], Option[DatabaseName]] = {
      case CollectionBuilderF(Fix($read(Collection(db, _))), _, _) => db.some
      case CollectionBuilderF(op, _, _)       => op.cata(wfSourceDb)

      case ArrayBuilderF(src, _)              => src
      case ArraySpliceBuilderF(src, _)        => src
      case DocBuilderF(src, _)                => src
      case ExprBuilderF(src, _)               => src
      case FlatteningBuilderF(src, _)         => src
      case GroupBuilderF(src, _, _)           => src
      case ShapePreservingBuilderF(src, _, _) => src
      case SpliceBuilderF(src, _)             => src
      case ValueBuilderF(_)                   => None
    }

    def lookup(
        lSrc: WorkflowBuilder[WF], lKey: WorkflowBuilder[WF], lName: BsonField.Name,
        rColl: CollectionName, rField: BsonField, rName: BsonField.Name) = {

      // NB: filtering on the left prior to the join is not strictly necessary,
      // but it avoids a runtime explosion in the common error case that the
      // field referenced on one side of the condition is not found.
      def filterExists(wb: WorkflowBuilder[WF], field: BsonField): WorkflowBuilder[WF] =
        WB.filter(wb,
          List(ExprBuilder(wb, \/-($var(DocField(field))))),
          { case List(f) => Selector.Doc(f -> Selector.Exists(true)) })

      lKey match {
        case Fix(ExprBuilderF(src, \/-($var(DocVar(_, Some(lField)))))) if src ≟ lSrc =>
          val left = WB.makeObject(lSrc, lName.asText)
          val filtered = filterExists(left, lName \ lField)
          generateWorkflow(filtered).map { case (left, _) =>
            CollectionBuilder(
              chain[Fix[WF]](
                left,
                $lookup(rColl, lName \ lField, rField, rName),
                $unwind(DocField(rName))),
              Root(),
              None)
            }

        case _ =>
          for {
            tmpName  <- emitSt(freshName)
            left     <- WB.objectConcat(
                          WB.makeObject(lSrc, lName.asText),
                          WB.makeObject(lKey, tmpName.asText))
            t        <- generateWorkflow(left)
            (src, _) = t
          } yield CollectionBuilder(
              chain[Fix[WF]](
                src,
                $lookup(rColl, tmpName, rField, rName),
                $project(
                  Reshape(ListMap(
                    lName -> \/-($var(DocField(lName))),
                    rName -> \/-($var(DocField(rName))))),
                  IgnoreId),
                $unwind(DocField(rName))),
              Root(),
              None)
      }
    }

    (tpe, left, right) match {
      case (set.InnerJoin, JoinSource(src, List(key), _), IsLookupFrom(coll, field))
            if unsharded(coll) && indexed(coll, field) && src.cata(sourceDb) ≟ coll.database.some =>
        lookup(
          src, key, LeftName,
          coll.collection, field, RightName).liftM[OptionT]

      // TODO: will require preserving empty arrays in $unwind (which is easier with a new feature in 3.2)
      // case (set.LeftOuterJoin, JoinSource(src, List(key), _), IsLookupFrom(coll, field))
      //       if unsharded(coll) && indexed(coll, field) && src.cata(sourceDb) ≟ coll.database.some =>
      //   ???

      case (set.InnerJoin, IsLookupFrom(coll, field), JoinSource(src, List(key), _))
            if unsharded(coll) && indexed(coll, field) && src.cata(sourceDb) ≟ coll.database.some =>
        lookup(
          src, key, RightName,
          coll.collection, field, LeftName).liftM[OptionT]

      // TODO: will require preserving empty arrays in $unwind (which is easier with a new feature in 3.2)
      // case (set.RightOuterJoin, IsLookupFrom(coll, field), JoinSource(src, List(key), _))
      //       if unsharded(coll) && indexed(coll, field) && src.cata(sourceDb) ≟ coll.database.some =>
      //   ???

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
              Fix(CollectionBuilderF(Fix($read(coll)), Root(), None)),
              List(Fix(ExprBuilderF(src, \/-($var(DocVar(_, Some(field))))))),
              _) if src ≟ arg.src =>
          (coll, field).some
        case _ =>
          None
      }
  }

  /** Plan an arbitrary join using only "core" operators, which always means a map-reduce. */
  def mapReduce[WF[_]: Functor: Coalesce: Crush: Crystallize]
    (implicit ev0: WorkflowOpCoreF :<: WF, ev1: RenderTree[WorkflowBuilder[WF]], ev2: ExprOpOps.Uni[ExprOp])
    : JoinHandler[WF, WorkflowBuilder.M] = JoinHandler({ (tpe, left0, right0) =>

    val ops = Ops[WF]
    import ops._

    val leftField0 = LeftName
    val rightField0 = RightName

    def keyMap(keyExpr: List[JsFn], rootField: BsonField.Name, otherField: BsonField.Name): Js.AnonFunDecl =
      $MapF.mapKeyVal(("key", "value"),
        keyExpr match {
          case Nil => Js.Null
          case _   =>
            jscore.Obj(keyExpr.map(_(jscore.ident("value"))).zipWithIndex.foldLeft[ListMap[jscore.Name, JsCore]](ListMap[jscore.Name, JsCore]()) {
              case (acc, (j, i)) => acc + (jscore.Name(i.toString) -> j)
            }).toJs
        },
        Js.AnonObjDecl(List(
          (otherField.asText, Js.AnonElem(Nil)),
          (rootField.asText, Js.AnonElem(List(Js.Ident("value")))))))

    def jsReduce(src: WorkflowBuilder[WF], key: List[JsFn],
                 rootField: BsonField.Name, otherField: BsonField.Name):
        (WorkflowBuilder[WF], FixOp[WF]) =
      (src, $map[WF](keyMap(key, rootField, otherField), ListMap()))

    def wbReduce(src: WorkflowBuilder[WF], key: List[WorkflowBuilder[WF]],
                 rootField: BsonField.Name, otherField: BsonField.Name):
        (WorkflowBuilder[WF], FixOp[WF]) =
      (DocBuilder(
        reduce(groupBy(src, key))($push(_)),
        ListMap(
          rootField             -> \/-($$ROOT),
          otherField            -> \/-($literal(Bson.Arr())),
          BsonField.Name("_id") -> \/-($include()))),
        ι)

    val (left, right, leftField, rightField) =
      (left0.js, right0.js) match {
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

    val nonEmpty: Selector.SelectorExpr = Selector.NotExpr(Selector.Size(0))

    def padEmpty(side: BsonField): Fix[ExprOp] =
      $cond($eq($size($var(DocField(side))), $literal(Bson.Int32(0))),
        $literal(Bson.Arr(List(Bson.Doc()))),
        $var(DocField(side)))

    def buildProjection(l: Fix[ExprOp], r: Fix[ExprOp]): FixOp[WF] =
      $project[WF](Reshape(ListMap(leftField -> \/-(l), rightField -> \/-(r)))).apply(_)

    // TODO exhaustive pattern match
    def buildJoin(src: Fix[WF], tpe: JoinType): Fix[WF] =
      tpe match {
        case set.FullOuterJoin =>
          chain(src,
            buildProjection(padEmpty(leftField), padEmpty(rightField)))
        case set.LeftOuterJoin =>
          chain(src,
            $match[WF](Selector.Doc(ListMap(
              leftField.asInstanceOf[BsonField] -> nonEmpty))),
            buildProjection($var(DocField(leftField)), padEmpty(rightField)))
        case set.RightOuterJoin =>
          chain(src,
            $match[WF](Selector.Doc(ListMap(
              rightField.asInstanceOf[BsonField] -> nonEmpty))),
            buildProjection(padEmpty(leftField), $var(DocField(rightField))))
        case set.InnerJoin =>
          chain(
            src,
            $match[WF](
              Selector.Doc(ListMap(
                leftField.asInstanceOf[BsonField] -> nonEmpty,
                rightField -> nonEmpty))))
        case _ => scala.sys.error("How did this get here?")
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

    (generateWorkflow(left._1) |@| generateWorkflow(right._1)) {
      case ((l, _), (r, _)) =>
        CollectionBuilder(
          chain(
            $foldLeft[WF](
              left._2(l),
              chain(r, right._2, $reduce[WF](rightReduce, ListMap()))),
            (op: Fix[WF]) => buildJoin(op, tpe),
            $unwind[WF](DocField(leftField)),
            $unwind[WF](DocField(rightField))),
          Root(),
          None)
    }
  })

  // TODO: This is an approximation. If we could postpone this decision until
  //      `Workflow.crush`, when we actually have a task (whether aggregation or
  //       mapReduce) in hand, we would know for sure.
  private def preferMapReduce[WF[_]: Coalesce: Crush: Crystallize: Functor](wb: WorkflowBuilder[WF])
    (implicit ev0: WorkflowOpCoreF :<: WF, ev1: RenderTree[WorkflowBuilder[WF]], ev2: ExprOpOps.Uni[ExprOp])
    : Boolean = {
    // TODO: Get rid of this when we functorize WorkflowTask
    def checkTask(wt: workflowtask.WorkflowTask): Boolean = wt match {
      case workflowtask.FoldLeftTask(_, _)     => true
      case workflowtask.MapReduceTask(_, _, _) => true
      case workflowtask.PipelineTask(src, _)   => checkTask(src)
      case _                                   => false
    }

    generateWorkflow[WF](wb).evalZero.fold(
      κ(false),
      wf => checkTask(task(Crystallize[WF].crystallize(wf._1))))
  }
}
