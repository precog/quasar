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

package quasar.physical.mongodb.workflow

import scala.Predef.$conforms
import quasar.Predef._
import quasar.{RenderTree, NonTerminal, Terminal}, RenderTree.ops._
import quasar.common.SortDir
import quasar.fp._
import quasar.fp.ski._
import quasar.javascript._, Js.JSRenderTree
import quasar.jscore, jscore.{JsCore, JsFn}
import quasar.physical.mongodb.{Bson, BsonField, Collection, CollectionName, Grouped, MapReduce, Reshape, Selector, sortDirToBson},
  MapReduce.Scope
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.workflowtask._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._

/** Ops that are provided by all supported MongoDB versions (since 2.6), or are
  * internal to quasar and supported everywhere. */
sealed abstract class WorkflowOpCoreF[+A] extends Product with Serializable

final case class $PureF(value: Bson) extends WorkflowOpCoreF[Nothing]
object $pure {
  def apply[F[_]: Coalesce](value: Bson)(implicit I: WorkflowOpCoreF :<: F) =
    Fix(Coalesce[F].coalesce(I.inj($PureF(value))))

  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
    : Option[Bson] =
    I.prj(op) collect {
      case $PureF(value) => (value)
    }
}

final case class $ReadF(coll: Collection) extends WorkflowOpCoreF[Nothing]
object $read {
  def apply[F[_]: Coalesce](coll: Collection)(implicit I: WorkflowOpCoreF :<: F) =
    Fix(Coalesce[F].coalesce(I.inj($ReadF(coll))))

  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
    : Option[Collection] =
    I.prj(op) collect {
      case $ReadF(coll) => (coll)
    }
}

final case class $MatchF[A](src: A, selector: Selector)
  extends WorkflowOpCoreF[A]  { self =>
  def shapePreserving: ShapePreservingF[WorkflowOpCoreF, A] =
    new ShapePreservingF[WorkflowOpCoreF, A] {
      def wf = self
      def src = self.src
      def reparent[B](newSrc: B) = self.copy(src = newSrc).shapePreserving

      def op = "$match"
      def rhs = selector.bson
    }
}
object $match {
  def apply[F[_]: Coalesce](selector: Selector)
    (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
    src => Fix(Coalesce[F].coalesce(I.inj($MatchF(src, selector))))

  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
    : Option[(A, Selector)] =
    I.prj(op) collect {
      case $MatchF(src, sel) => (src, sel)
    }
}

final case class $ProjectF[A](src: A, shape: Reshape[ExprOp], idExclusion: IdHandling)
    extends WorkflowOpCoreF[A] { self =>
  def pipeline: PipelineF[WorkflowOpCoreF, A] =
    new PipelineF[WorkflowOpCoreF, A] {
      def wf = self
      def src = self.src
      def reparent[B](newSrc: B) = self.copy(src = newSrc).pipeline

      def op = "$project"
      def rhs = self.pipelineRhs
    }
  // NB: this is exposed separately so that the type can be narrower
  def pipelineRhs: Bson.Doc = idExclusion match {
    case ExcludeId => Bson.Doc(shape.bson.value + (IdLabel -> Bson.Bool(false)))
    case _         => shape.bson
  }
  def empty: $ProjectF[A] = $ProjectF.EmptyDoc(src)

  def set(field: BsonField, value: Reshape.Shape[ExprOp]): $ProjectF[A] =
    $ProjectF(src,
      shape.set(field, value),
      if (field == IdName) IncludeId else idExclusion)

  def get(ref: DocVar): Option[Reshape.Shape[ExprOp]] = ref match {
    case DocVar(_, Some(field)) => shape.get(field)
    case _                      => Some(-\/(shape))
  }

  def getAll: List[(BsonField, Fix[ExprOp])] = {
    val all = Reshape.getAll(shape)
    idExclusion match {
      case IncludeId => all.collectFirst {
        case (IdName, _) => all
      }.getOrElse((IdName, fixExprOp.$include()) :: all)
      case _         => all
    }
  }

  def setAll(fvs: Iterable[(BsonField, Reshape.Shape[ExprOp])]): $ProjectF[A] =
    $ProjectF(
      src,
      Reshape.setAll(shape, fvs),
      if (fvs.exists(_._1 == IdName)) IncludeId else idExclusion)

  def deleteAll(fields: List[BsonField]): $ProjectF[A] =
    $ProjectF(src,
      Reshape.setAll(Reshape.emptyDoc[ExprOp],
        Reshape.getAll(this.shape)
          .filterNot(t => fields.exists(t._1.startsWith(_)))
          .map(t => t._1 -> \/-(t._2))),
      if (fields.contains(IdName)) ExcludeId else idExclusion)

  def id: $ProjectF[A] = {
    def loop(prefix: Option[BsonField], p: $ProjectF[A]): $ProjectF[A] = {
      def nest(child: BsonField): BsonField =
        prefix.map(_ \ child).getOrElse(child)

      $ProjectF(
        p.src,
        Reshape(
          p.shape.value.transform {
            case (k, v) =>
              v.fold(
                r => -\/(loop(Some(nest(k)), $ProjectF(p.src, r, p.idExclusion)).shape),
                κ(\/-(fixExprOp.$var(DocVar.ROOT(nest(k))))))
          }),
        p.idExclusion)
    }

    loop(None, this)
  }
}
object $ProjectF {
  def EmptyDoc[A](src: A) = $ProjectF(src, Reshape.emptyDoc[ExprOp], ExcludeId)
}
object $project {
  def apply[F[_]: Coalesce](shape: Reshape[ExprOp], id: IdHandling)
    (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
    src => Fix(Coalesce[F].coalesce(I.inj($ProjectF(src, shape, id))))

  def apply[F[_]: Coalesce](shape: Reshape[ExprOp])
    (implicit ev: WorkflowOpCoreF :<: F)
    : FixOp[F] =
    $project[F](
      shape,
      shape.get(IdName).fold[IdHandling](IgnoreId)(κ(IncludeId)))

  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
    : Option[(A, Reshape[ExprOp], IdHandling)] =
    I.prj(op) collect {
      case $ProjectF(src, shape, id) => (src, shape, id)
    }
}

final case class $RedactF[A](src: A, value: Fix[ExprOp])
  extends WorkflowOpCoreF[A] { self =>
  def pipeline(implicit exprOps: ExprOpOps.Uni[ExprOp]): PipelineF[WorkflowOpCoreF, A] =
    new PipelineF[WorkflowOpCoreF, A] {
      def wf = self
      def src = self.src
      def reparent[B](newSrc: B) = self.copy(src = newSrc).pipeline

      def op = "$redact"
      def rhs = value.cata(exprOps.bson)
    }
}
object $RedactF {
  // def make(value: Expression)(src: Workflow): Workflow =
  //   Fix(coalesce($RedactF(src, value)))

  val DESCEND = DocVar(DocVar.Name("DESCEND"),  None)
  val PRUNE   = DocVar(DocVar.Name("PRUNE"),    None)
  val KEEP    = DocVar(DocVar.Name("KEEP"),     None)
}
object $redact {
  def apply[F[_]: Coalesce](value: Fix[ExprOp])
    (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
    src => Fix(Coalesce[F].coalesce(I.inj($RedactF(src, value))))

  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
    : Option[(A, Fix[ExprOp])] =
    I.prj(op) collect {
      case $RedactF(src, value) => (src, value)
    }
}

final case class $LimitF[A](src: A, count: Long)
    extends WorkflowOpCoreF[A] { self =>
  def shapePreserving: ShapePreservingF[WorkflowOpCoreF, A] =
    new ShapePreservingF[WorkflowOpCoreF, A] {
      def wf = self
      def src = self.src
      def reparent[B](newSrc: B) = self.copy(src = newSrc).shapePreserving

      def op = "$limit"
      def rhs = Bson.Int64(count)
    }
}
object $limit {
  def apply[F[_]: Coalesce](count: Long)
    (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
    src => Fix(Coalesce[F].coalesce(I.inj($LimitF(src, count))))

  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F): Option[(A, Long)] =
    I.prj(op).collect {
      case $LimitF(src, count) => (src, count)
    }
}

final case class $SkipF[A](src: A, count: Long)
    extends WorkflowOpCoreF[A] { self =>
  def shapePreserving: ShapePreservingF[WorkflowOpCoreF, A] =
    new ShapePreservingF[WorkflowOpCoreF, A] {
      def wf = self
      def src = self.src
      def reparent[B](newSrc: B) = self.copy(src = newSrc).shapePreserving

      def op = "$skip"
      def rhs = Bson.Int64(count)
    }
}
object $skip {
  def apply[F[_]: Coalesce](count: Long)
    (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
    src => Fix(Coalesce[F].coalesce(I.inj($SkipF(src, count))))

  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F): Option[(A, Long)] =
    I.prj(op).collect {
      case $SkipF(src, count) => (src, count)
    }
}

final case class $UnwindF[A](src: A, field: DocVar)
    extends WorkflowOpCoreF[A] { self =>
  def pipeline: PipelineF[WorkflowOpCoreF, A] =
    new PipelineF[WorkflowOpCoreF, A] {
      def wf = self
      def src = self.src
      def reparent[B](newSrc: B) = self.copy(src = newSrc).pipeline

      def op = "$unwind"
      def rhs = field.bson
    }
  lazy val flatmapop = $SimpleMapF(src, NonEmptyList(FlatExpr(field.toJs)), ListMap())
}
object $unwind {
  def apply[F[_]: Coalesce](field: DocVar)
    (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
    src => Fix(Coalesce[F].coalesce(I.inj($UnwindF(src, field))))

  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
    : Option[(A, DocVar)] =
    I.prj(op) collect {
      case $UnwindF(src, field) => (src, field)
    }
}

final case class $GroupF[A](src: A, grouped: Grouped[ExprOp], by: Reshape.Shape[ExprOp])
    extends WorkflowOpCoreF[A] { self =>

  def pipeline(implicit exprOps: ExprOpOps.Uni[ExprOp]): PipelineF[WorkflowOpCoreF, A] =
    new PipelineF[WorkflowOpCoreF, A] {
      def wf = self
      def src = self.src
      def reparent[B](newSrc: B) = self.copy(src = newSrc).pipeline

      def op = "$group"
      def rhs = {
        val Bson.Doc(m) = grouped.bson
        Bson.Doc(m + (IdLabel -> by.fold(_.bson, _.cata(exprOps.bson))))
      }
    }

  def empty = copy(grouped = Grouped[ExprOp](ListMap()))

  def getAll: List[(BsonField.Name, AccumOp[Fix[ExprOp]])] =
    grouped.value.toList

  def deleteAll(fields: List[BsonField.Name]): $GroupF[A] = {
    empty.setAll(getAll.filterNot(t => fields.contains(t._1)))
  }

  def setAll(vs: Seq[(BsonField.Name, AccumOp[Fix[ExprOp]])]) = copy(grouped = Grouped[ExprOp](ListMap(vs: _*)))
}
object $group {
  def apply[F[_]: Coalesce](grouped: Grouped[ExprOp], by: Reshape.Shape[ExprOp])
    (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
    src => Fix(Coalesce[F].coalesce(I.inj($GroupF(src, grouped, by))))

  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
    : Option[(A, Grouped[ExprOp], Reshape.Shape[ExprOp])] =
    I.prj(op) collect {
      case $GroupF(src, grouped, shape) => (src, grouped, shape)
    }
}

final case class $SortF[A](src: A, value: NonEmptyList[(BsonField, SortDir)])
    extends WorkflowOpCoreF[A] { self =>
  def shapePreserving: ShapePreservingF[WorkflowOpCoreF, A] =
    new ShapePreservingF[WorkflowOpCoreF, A] {
      def wf = self
      def src = self.src
      def reparent[B](newSrc: B) = self.copy(src = newSrc).shapePreserving

      def op = "$sort"
      // Note: ListMap preserves the order of entries.
      def rhs: Bson.Doc = $SortF.keyBson(value)
    }
}
object $SortF {
  def keyBson(value: NonEmptyList[(BsonField, SortDir)]) =
    Bson.Doc(ListMap((value.map { case (k, t) => k.asText -> sortDirToBson(t) }).list.toList: _*))
}
object $sort {
  def apply[F[_]: Coalesce](value: NonEmptyList[(BsonField, SortDir)])
    (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
    src => Fix(Coalesce[F].coalesce(I.inj($SortF(src, value))))

  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
    : Option[(A, NonEmptyList[(BsonField, SortDir)])] =
    I.prj(op) collect {
      case $SortF(src, value) => (src, value)
    }
}

/**
 * TODO: If an \$OutF has anything after it, we need to either do
 * {{{\$seq(\$out(src, dst), after(\$read(dst), ...))}}}
 * or
 * {{{\$Fork(src, List(\$out(_, dst), after(_, ...)))}}}
 * The latter seems preferable, but currently the forking semantics are not
 * clear.
 */
final case class $OutF[A](src: A, collection: CollectionName)
    extends WorkflowOpCoreF[A] { self =>
  def shapePreserving: ShapePreservingF[WorkflowOpCoreF, A] =
    new ShapePreservingF[WorkflowOpCoreF, A] {
      def wf = self
      def src = self.src
      def reparent[B](newSrc: B) =
        self.copy(src = newSrc).shapePreserving

      def op = "$out"
      def rhs = collection.bson
    }
}
object $out {
  def apply[F[_]: Coalesce](collection: CollectionName)
    (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
    src => Fix(Coalesce[F].coalesce(I.inj($OutF(src, collection))))

  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
    : Option[(A, CollectionName)] =
    I.prj(op) collect {
      case $OutF(src, collection) => (src, collection)
    }
}

final case class $GeoNearF[A](
  src: A,
  near: (Double, Double), distanceField: BsonField,
  limit: Option[Int], maxDistance: Option[Double],
  query: Option[Selector], spherical: Option[Boolean],
  distanceMultiplier: Option[Double], includeLocs: Option[BsonField],
  uniqueDocs: Option[Boolean])
    extends WorkflowOpCoreF[A] { self =>
  def pipeline: PipelineF[WorkflowOpCoreF, A] =
    new PipelineF[WorkflowOpCoreF, A] {
      def wf = self
      def src = self.src
      def reparent[B](newSrc: B) = self.copy(src = newSrc).pipeline

      def op = "$geoNear"
      def rhs = Bson.Doc(List(
        List("near"           -> Bson.Arr(Bson.Dec(near._1) :: Bson.Dec(near._2) :: Nil)),
        List("distanceField"  -> distanceField.bson),
        limit.toList.map(limit => "limit" -> Bson.Int32(limit)),
        maxDistance.toList.map(maxDistance => "maxDistance" -> Bson.Dec(maxDistance)),
        query.toList.map(query => "query" -> query.bson),
        spherical.toList.map(spherical => "spherical" -> Bson.Bool(spherical)),
        distanceMultiplier.toList.map(distanceMultiplier => "distanceMultiplier" -> Bson.Dec(distanceMultiplier)),
        includeLocs.toList.map(includeLocs => "includeLocs" -> includeLocs.bson),
        uniqueDocs.toList.map(uniqueDocs => "uniqueDocs" -> Bson.Bool(uniqueDocs))
      ).flatten.toListMap)
    }
}
object $geoNear {
  def apply[F[_]: Coalesce]
    (near: (Double, Double), distanceField: BsonField,
      limit: Option[Int], maxDistance: Option[Double],
      query: Option[Selector], spherical: Option[Boolean],
      distanceMultiplier: Option[Double], includeLocs: Option[BsonField],
      uniqueDocs: Option[Boolean])
    (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
      src => Fix(Coalesce[F].coalesce(I.inj($GeoNearF(
        src, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs))))

  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
    : Option[(A, (Double, Double), BsonField, Option[Int], Option[Double],
      Option[Selector], Option[Boolean], Option[Double], Option[BsonField],
      Option[Boolean])] =
    I.prj(op) collect {
      case $GeoNearF(src, n, df, l, md, q, s, dm, il, ud) =>
        (src, n, df, l, md, q, s, dm, il, ud)
    }
}

sealed trait MapReduceF[A] extends WorkflowOpCoreF[A] {
  def singleSource: SingleSourceF[WorkflowOpCoreF, A]

  def newMR[F[_]](
    base: DocVar,
    src: WorkflowTask,
    sel: Option[Selector],
    sort: Option[NonEmptyList[(BsonField, SortDir)]],
    count: Option[Long])
    : (DocVar, WorkflowTask)
}

/**
  Takes a function of two parameters. The first is the current key (which
  defaults to `this._id`, but may have been overridden by previous
  [Flat]\$MapFs) and the second is the document itself. The function must
  return a 2-element array containing the new key and new value.
  */
final case class $MapF[A](src: A, fn: Js.AnonFunDecl, scope: Scope) extends MapReduceF[A] { self =>
  def singleSource: SingleSourceF[WorkflowOpCoreF, A] =
    new SingleSourceF[WorkflowOpCoreF, A] {
      def wf = self
      def src = self.src
      def reparent[B](newSrc: B) = self.copy(src = newSrc).singleSource
    }

  import $MapF._

  def newMR[F[_]](base: DocVar, src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortDir)]], count: Option[Long]) =
    (ExprVar,
      MapReduceTask(
        src,
        MapReduce(
          mapFn(base match {
            case DocVar(DocVar.ROOT, None) => this.fn
            case _ => compose(this.fn, mapProject(base))
          }),
          $ReduceF.reduceNOP,
          selection = sel, inputSort = sort, limit = count, scope = scope),
        None))
}
object $MapF {
  import quasar.jscore._

  def compose(g: Js.AnonFunDecl, f: Js.AnonFunDecl): Js.AnonFunDecl =
    Js.AnonFunDecl(List("key", "value"), List(
      Js.Return(Js.Call(Js.Select(g, "apply"),
        List(Js.Null, Js.Call(f, List(Js.Ident("key"), Js.Ident("value"))))))))

  def mapProject(base: DocVar) =
    Js.AnonFunDecl(List("key", "value"), List(
      Js.Return(Js.AnonElem(List(Js.Ident("key"), base.toJs(jscore.ident("value")).toJs)))))


  def mapKeyVal(idents: (String, String), key: Js.Expr, value: Js.Expr) =
    Js.AnonFunDecl(List(idents._1, idents._2),
      List(Js.Return(Js.AnonElem(List(key, value)))))
  def mapMap(ident: String, transform: Js.Expr) =
    mapKeyVal(("key", ident), Js.Ident("key"), transform)
  val mapFresh =
    mapKeyVal(("key", "value"), Js.Call(Js.Ident("ObjectId"), Nil), Js.Ident("value"))
  val mapValKey =
    mapKeyVal(("key", "value"), Js.Ident("value"), Js.Ident("value"))
  val mapNOP = mapMap("value", Js.Ident("value"))

  def finalizerFn(fn: JsFn) =
    Js.AnonFunDecl(List("key", "value"),
      List(Js.Return(fn(jscore.Ident(jscore.Name("value"))).toJs)))

  def mapFn(fn: Js.Expr) =
    Js.AnonFunDecl(Nil,
      List(Js.Call(Js.Select(Js.Ident("emit"), "apply"),
        List(
          Js.Null,
          Js.Call(fn, List(Js.Select(Js.This, IdLabel), Js.This))))))
}
object $map {
  def apply[F[_]: Coalesce](fn: Js.AnonFunDecl, scope: Scope)
    (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
    src => Fix(Coalesce[F].coalesce(I.inj($MapF(src, fn, scope))))

  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
    : Option[(A, Js.AnonFunDecl, Scope)] =
    I.prj(op) collect {
      case $MapF(src, fn, scope) => (src, fn, scope)
    }
}

// FIXME: this one should become $MapF, with the other one being replaced by
// a new op that combines a map and reduce operation?
final case class $SimpleMapF[A](src: A, exprs: NonEmptyList[CardinalExpr[JsFn]], scope: Scope)
    extends MapReduceF[A] { self =>
  def singleSource: SingleSourceF[WorkflowOpCoreF, A] =
    new SingleSourceF[WorkflowOpCoreF, A] {
      def wf = self
      def src = self.src
      def reparent[B](newSrc: B) = self.copy(src = newSrc).singleSource
    }
  def getAll: Option[List[BsonField]] = {
    def loop(x: JsCore): Option[List[BsonField]] = x match {
      case jscore.Obj(values) => Some(values.toList.flatMap { case (k, v) =>
        val n = BsonField.Name(k.value)
        loop(v).map(_.map(n \ _)).getOrElse(List(n))
      })
      case _ => None
    }
    // Note: this is not safe if `expr` inspects the argument to decide what
    // JS to construct, but all we need here is names of fields that we may
    // be able to optimize away.
    loop(simpleExpr(jscore.ident("?")))
  }

  def deleteAll(fields: List[BsonField]): $SimpleMapF[A] = {
    def loop(x: JsCore, fields: List[List[BsonField.Name]]): Option[JsCore] = x match {
      case jscore.Obj(values) => Some(jscore.Obj(
        values.collect(Function.unlift[(jscore.Name, JsCore), (jscore.Name, JsCore)] { t =>
          val (k, v) = t
          if (fields contains List(BsonField.Name(k.value))) None
          else {
            val v1 = loop(v, fields.collect {
              case BsonField.Name(k.value) :: tail => tail
            }).getOrElse(v)
            v1 match {
              case jscore.Obj(values) if values.isEmpty => None
              case _ => Some(k -> v1)
            }
          }
        })))
      case _ => Some(x)
    }

    exprs match {
      case NonEmptyList(MapExpr(expr), INil()) =>
        $SimpleMapF(src,
          NonEmptyList(
            MapExpr(JsFn(jscore.Name("base"), loop(expr(jscore.ident("base")), fields.map(_.flatten.toList)).getOrElse(jscore.Literal(Js.Null))))),
          scope)
      case _ => this
    }
  }

  private def fn: Js.AnonFunDecl = {
    import quasar.jscore._

    def body(fs: List[(CardinalExpr[JsFn], String)]) =
      Js.AnonFunDecl(List("key", "value"),
        List(
          Js.VarDef(List("rez" -> Js.AnonElem(Nil))),
          fs.foldRight[JsCore => Js.Stmt](b =>
            Js.Call(Js.Select(Js.Ident("rez"), "push"),
              List(
                Js.AnonElem(List(
                  Js.Call(Js.Ident("ObjectId"), Nil),
                  b.toJs))))){
            case ((MapExpr(m), n), inner) => b =>
              Js.Block(List(
                Js.VarDef(List(n -> m(b).toJs)),
                inner(ident(n))))
            case ((FlatExpr(m), n), inner) => b =>
              Js.ForIn(Js.Ident("elem"), m(b).toJs,
                Js.Block(List(
                  Js.VarDef(List(n -> Js.Call(Js.Ident("clone"), List(b.toJs)))),
                  unsafeAssign(m(ident(n)), Access(m(b), ident("elem"))),
                  inner(ident(n)))))
          }(ident("value")),
          Js.Return(Js.Ident("rez"))))

    body(exprs.toList.zip(Stream.from(0).map("each" + _.toString)))
  }

  def >>>(that: $SimpleMapF[A]) = {
    $SimpleMapF(
      this.src,
      (this.exprs.last, that.exprs.head) match {
        case (MapExpr(l), MapExpr(r)) =>
          this.exprs.init <::: NonEmptyList.nel(MapExpr(l >>> r), that.exprs.tail)
        case _ => this.exprs <+> that.exprs
      },
      this.scope <+> that.scope
    )
  }

  def raw = {
    import quasar.jscore._

    val funcs = (exprs).map(_.copoint(ident("_")).para(findFunctionsƒ)).foldLeft(Set[String]())(_ ++ _)

    exprs match {
      case NonEmptyList(MapExpr(expr), INil()) =>
        $MapF(src,
          Js.AnonFunDecl(List("key", "value"), List(
            Js.Return(Arr(List(
              ident("key"),
              expr(ident("value")))).toJs))),
          scope <+> $SimpleMapF.implicitScope(funcs)
        )
      case _ =>
        // WartRemover seems to be confused by the `+` method on `Set`
        @SuppressWarnings(Array("org.wartremover.warts.StringPlusAny"))
        val newFuncs = funcs + "clone"
        $FlatMapF(src, fn, $SimpleMapF.implicitScope(newFuncs) ++ scope)
    }
  }

  def newMR[F[_]](base: DocVar, src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortDir)]], count: Option[Long]) =
    raw.newMR(base, src, sel, sort, count)

  def simpleExpr = exprs.foldRight(JsFn.identity) {
    case (MapExpr(expr), acc) => expr >>> acc
    case (_,             acc) => acc
  }
}
object $SimpleMapF {
  def implicitScope(fs: Set[String]) =
    $SimpleMapF.jsLibrary.filter(x => fs.contains(x._1))

  val jsLibrary = ListMap(
    "remove" -> Bson.JavaScript(
      Js.AnonFunDecl(List("obj", "field"), List(
        Js.VarDef(List("dest" -> Js.AnonObjDecl(Nil))),
        Js.ForIn(Js.Ident("i"), Js.Ident("obj"),
          Js.If(Js.BinOp("!=", Js.Ident("i"), Js.Ident("field")),
            Js.BinOp("=",
              Js.Access(Js.Ident("dest"), Js.Ident("i")),
              Js.Access(Js.Ident("obj"), Js.Ident("i"))),
            None)),
        Js.Return(Js.Ident("dest"))))),
    "clone" -> Bson.JavaScript(
      Js.AnonFunDecl(List("src"), List(
        Js.If(
          Js.BinOp("||",
            Js.BinOp("!=", Js.UnOp("typeof", Js.Ident("src")), Js.Str("object")),
            Js.BinOp("==", Js.Ident("src"), Js.Null)),
          Js.Return(Js.Ident("src")),
          None),
        Js.VarDef(List("dest" -> Js.New(Js.Select(Js.Ident("src"), "constructor")))),
        Js.ForIn(Js.Ident("i"), Js.Ident("src"),
          Js.BinOp ("=",
            Js.Access(Js.Ident("dest"), Js.Ident("i")),
            Js.Call(Js.Ident("clone"), List(
              Js.Access(Js.Ident("src"), Js.Ident("i")))))),
        Js.Return(Js.Ident("dest"))))))
}
object $simpleMap {
  def apply[F[_]: Coalesce](exprs: NonEmptyList[CardinalExpr[JsFn]], scope: Scope)
    (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
    src => Fix(Coalesce[F].coalesce(I.inj($SimpleMapF(src, exprs, scope))))

  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
    : Option[(A, NonEmptyList[CardinalExpr[JsFn]], Scope)] =
    I.prj(op) collect {
      case $SimpleMapF(src, exprs, scope) => (src, exprs, scope)
    }
}

/**
  Takes a function of two parameters. The first is the current key (which
  defaults to `this._id`, but may have been overridden by previous
  [Flat]\$MapFs) and the second is the document itself. The function must
  return an array of 2-element arrays, each containing a new key and a new
  value.
  */
final case class $FlatMapF[A](src: A, fn: Js.AnonFunDecl, scope: Scope)
    extends MapReduceF[A] { self =>
  def singleSource: SingleSourceF[WorkflowOpCoreF, A] =
    new SingleSourceF[WorkflowOpCoreF, A] {
      def wf = self
      def src = self.src
      def reparent[B](newSrc: B) = self.copy(src = newSrc).singleSource
    }

  import $FlatMapF._

  def newMR[F[_]](base: DocVar, src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortDir)]], count: Option[Long]) =
    (ExprVar,
      MapReduceTask(
        src,
        MapReduce(
          mapFn(base match {
            case DocVar(DocVar.ROOT, None) => this.fn
            case _ => $MapF.compose(this.fn, $MapF.mapProject(base))
          }),
          $ReduceF.reduceNOP,
          selection = sel, inputSort = sort, limit = count, scope = scope),
        None))
}
object $FlatMapF {
  import Js._

  private def composition(g: Js.AnonFunDecl, f: Js.AnonFunDecl) =
    Call(
      Select(Call(f, List(Ident("key"), Ident("value"))), "map"),
      List(AnonFunDecl(List("args"), List(
        Return(Call(Select(g, "apply"), List(Null, Ident("args"))))))))

  def kleisliCompose(g: Js.AnonFunDecl, f: Js.AnonFunDecl) =
    AnonFunDecl(List("key", "value"), List(
      Return(
        Call(
          Select(Select(AnonElem(Nil), "concat"), "apply"),
          List(AnonElem(Nil), composition(g, f))))))

  def mapCompose(g: Js.AnonFunDecl, f: Js.AnonFunDecl) =
    AnonFunDecl(List("key", "value"), List(Return(composition(g, f))))

  def mapFn(fn: Js.Expr) =
    AnonFunDecl(Nil,
      List(
        Call(
          Select(
            Call(fn, List(Select(This, IdLabel), This)),
            "map"),
          List(AnonFunDecl(List("__rez"),
            List(Call(Select(Ident("emit"), "apply"),
              List(Null, Ident("__rez")))))))))
}
object $flatMap {
  def apply[F[_]: Coalesce](fn: Js.AnonFunDecl, scope: Scope)
    (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
    src => Fix(Coalesce[F].coalesce(I.inj($FlatMapF(src, fn, scope))))

  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
    : Option[(A, Js.AnonFunDecl, Scope)] =
    I.prj(op) collect {
      case $FlatMapF(src, fn, scope) => (src, fn, scope)
    }
}

/** Takes a function of two parameters – a key and an array of values. The
  * function must return a single value.
  */
final case class $ReduceF[A](src: A, fn: Js.AnonFunDecl, scope: Scope)
    extends MapReduceF[A] { self =>
  def singleSource: SingleSourceF[WorkflowOpCoreF, A] =
    new SingleSourceF[WorkflowOpCoreF, A] {
      def wf = self
      def src = self.src
      def reparent[B](newSrc: B) = self.copy(src = newSrc).singleSource
    }

  def newMR[F[_]](base: DocVar, src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortDir)]], count: Option[Long]) =
    (ExprVar,
      MapReduceTask(
        src,
        MapReduce(
          $MapF.mapFn(base match {
            case DocVar(DocVar.ROOT, None) => $MapF.mapNOP
            case _                         => $MapF.mapProject(base)
          }),
          this.fn,
          selection = sel, inputSort = sort, limit = count, scope = scope),
        None))
}
object $ReduceF {
  import quasar.jscore._

  val reduceNOP =
    Js.AnonFunDecl(List("key", "values"), List(
      Js.Return(Access(ident("values"), Literal(Js.Num(0, false))).toJs)))

  val reduceFoldLeft =
    Js.AnonFunDecl(List("key", "values"), List(
      Js.VarDef(List("rez" -> Js.AnonObjDecl(Nil))),
      Js.Call(Select(ident("values"), "forEach").toJs,
        List(Js.AnonFunDecl(List("value"),
          List(copyAllFields(ident("value"), Name("rez")))))),
      Js.Return(Js.Ident("rez"))))
}
object $reduce {
  def apply[F[_]: Coalesce](fn: Js.AnonFunDecl, scope: Scope)
    (implicit I: WorkflowOpCoreF :<: F): FixOp[F] =
    src => Fix(Coalesce[F].coalesce(I.inj($ReduceF(src, fn, scope))))

  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
    : Option[(A, Js.AnonFunDecl, Scope)] =
    I.prj(op) collect {
      case $ReduceF(src, fn, scope) => (src, fn, scope)
    }
}

/** Performs a sequence of operations, sequentially, merging their results.
  */
final case class $FoldLeftF[A](head: A, tail: NonEmptyList[A])
    extends WorkflowOpCoreF[A]
object $foldLeft {
  def apply[F[_]: Coalesce](first: Fix[F], second: Fix[F], rest: Fix[F]*)
    (implicit I: WorkflowOpCoreF :<: F): Fix[F] =
    Fix(Coalesce[F].coalesce(I.inj($FoldLeftF(first, NonEmptyList.nel(second, IList.fromList(rest.toList))))))

  // FIXME: the result should be in NonEmptyList, but it gives a compile error
  // saying "this is a GADT skolem"
  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOpCoreF :<: F)
    : Option[(A, List[A])] =
    I.prj(op) collect {
      case $FoldLeftF(head, tail) => (head, tail.toList)
    }
}

object WorkflowOpCoreF {
  // NB: this extractor has to be used instead of the simpler ones provided for
  // each op if you need to bind the op itself, and not just its fields.
  // For example: `case $project(src, shape, id) => ` vs.
  // `case WorkflowOpCoreF(p @ $ProjectF(_, _, _)) =>`
  def unapply[F[_], A](fa: F[A])(implicit I: WorkflowOpCoreF :<: F): Option[WorkflowOpCoreF[A]] =
    I.prj(fa)

  implicit val traverse: Traverse[WorkflowOpCoreF] =
    new Traverse[WorkflowOpCoreF] {
      def traverseImpl[G[_], A, B](fa: WorkflowOpCoreF[A])(f: A => G[B])
        (implicit G: Applicative[G]):
          G[WorkflowOpCoreF[B]] = fa match {
        case x @ $PureF(_)             => G.point(x)
        case x @ $ReadF(_)             => G.point(x)
        case $MapF(src, fn, scope)     => G.apply(f(src))($MapF(_, fn, scope))
        case $FlatMapF(src, fn, scope) => G.apply(f(src))($FlatMapF(_, fn, scope))
        case $SimpleMapF(src, exprs, scope) =>
          G.apply(f(src))($SimpleMapF(_, exprs, scope))
        case $ReduceF(src, fn, scope)  => G.apply(f(src))($ReduceF(_, fn, scope))
        case $FoldLeftF(head, tail)    =>
          G.apply2(f(head), tail.traverse(f))($FoldLeftF(_, _))
        // NB: Would be nice to replace the rest of this impl with the following
        //     line, but the invariant definition of Traverse doesn’t allow it.
        // case p: PipelineF[_]          => PipelineFTraverse.traverseImpl(p)(f)
        case $MatchF(src, sel)         => G.apply(f(src))($MatchF(_, sel))
        case $ProjectF(src, shape, id) => G.apply(f(src))($ProjectF(_, shape, id))
        case $RedactF(src, value)      => G.apply(f(src))($RedactF(_, value))
        case $LimitF(src, count)       => G.apply(f(src))($LimitF(_, count))
        case $SkipF(src, count)        => G.apply(f(src))($SkipF(_, count))
        case $UnwindF(src, field)      => G.apply(f(src))($UnwindF(_, field))
        case $GroupF(src, grouped, by) => G.apply(f(src))($GroupF(_, grouped, by))
        case $SortF(src, value)        => G.apply(f(src))($SortF(_, value))
        case $GeoNearF(src, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs) =>
          G.apply(f(src))($GeoNearF(_, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs))
        case $OutF(src, col)           => G.apply(f(src))($OutF(_, col))
      }
    }

  implicit val refs: Refs[WorkflowOpCoreF] = Refs.fromRewrite[WorkflowOpCoreF](rewriteRefs2_6)

  implicit val crush: Crush[WorkflowOpCoreF] = Crush.injected[WorkflowOpCoreF, WorkflowF]

  implicit val coalesce: Coalesce[WorkflowOpCoreF] = coalesceAll[WorkflowOpCoreF]
  implicit val classify: Classify[WorkflowOpCoreF] =
    new Classify[WorkflowOpCoreF] {
      override def source[A](op: WorkflowOpCoreF[A]) = op match {
        case $ReadF(_) | $PureF(_) => SourceF(op).some
        case _ => None
      }

      override def singleSource[A](op: WorkflowOpCoreF[A]) = op match {
        case op @ $MatchF(_, _)        => op.shapePreserving.widen[A].some
        case op @ $ProjectF(_, _, _)   => op.pipeline.widen[A].some
        case op @ $RedactF(_, _)       => op.pipeline.widen[A].some
        case op @ $SkipF(_, _)         => op.shapePreserving.widen[A].some
        case op @ $LimitF(_, _)        => op.shapePreserving.widen[A].some
        case op @ $UnwindF(_, _)       => op.pipeline.widen[A].some
        case op @ $GroupF(_, _, _)     => op.pipeline.widen[A].some
        case op @ $SortF(_, _)         => op.shapePreserving.widen[A].some
        case op @ $GeoNearF(_, _, _, _, _, _, _, _, _, _) => op.pipeline.widen[A].some
        case op @ $OutF(_, _)          => op.shapePreserving.widen[A].some
        case op @ $MapF(_, _, _)       => op.singleSource.widen[A].some
        case op @ $SimpleMapF(_, _, _) => op.singleSource.widen[A].some
        case op @ $FlatMapF(_, _, _)   => op.singleSource.widen[A].some
        case op @ $ReduceF(_, _, _)    => op.singleSource.widen[A].some
        case _ => None
      }

      override def pipeline[A](op: WorkflowOpCoreF[A]) = op match {
        case op @ $MatchF(_, _)      => op.shapePreserving.widen[A].some
        case op @ $ProjectF(_, _, _) => op.pipeline.widen[A].some
        case op @ $RedactF(_, _)     => op.pipeline.widen[A].some
        case op @ $SkipF(_, _)       => op.shapePreserving.widen[A].some
        case op @ $LimitF(_, _)      => op.shapePreserving.widen[A].some
        case op @ $UnwindF(_, _)     => op.pipeline.widen[A].some
        case op @ $GroupF(_, _, _)   => op.pipeline.widen[A].some
        case op @ $SortF(_, _)       => op.shapePreserving.widen[A].some
        case op @ $GeoNearF(_, _, _, _, _, _, _, _, _, _) => op.pipeline.widen[A].some
        case op @ $OutF(_, _)        => op.shapePreserving.widen[A].some
        case _ => None
      }

      override def shapePreserving[A](op: WorkflowOpCoreF[A]) = op match {
        case op @ $MatchF(_, _) => op.shapePreserving.widen[A].some
        case op @ $SkipF(_, _)  => op.shapePreserving.widen[A].some
        case op @ $LimitF(_, _) => op.shapePreserving.widen[A].some
        case op @ $SortF(_, _)  => op.shapePreserving.widen[A].some
        case op @ $OutF(_, _)   => op.shapePreserving.widen[A].some
        case _ => None
      }
    }

  implicit val renderTree: RenderTree[WorkflowOpCoreF[Unit]] =
    new RenderTree[WorkflowOpCoreF[Unit]] {
      val wfType = "Workflow" :: Nil

      def render(v: WorkflowOpCoreF[Unit]) = v match {
        case $PureF(value)       => Terminal("$PureF" :: wfType, Some(value.shows))
        case $ReadF(coll)        => coll.render.copy(nodeType = "$ReadF" :: wfType)
        case $MatchF(_, sel)     =>
          NonTerminal("$MatchF" :: wfType, None, sel.render :: Nil)
        case $ProjectF(_, shape, xId) =>
          NonTerminal("$ProjectF" :: wfType, None,
            Reshape.renderReshape(shape) :+
              Terminal(xId.shows :: "$ProjectF" :: wfType, None))
        case $RedactF(_, value) => NonTerminal("$RedactF" :: wfType, None,
          value.render ::
            Nil)
        case $LimitF(_, count)  => Terminal("$LimitF" :: wfType, Some(count.shows))
        case $SkipF(_, count)   => Terminal("$SkipF" :: wfType, Some(count.shows))
        case $UnwindF(_, field) => Terminal("$UnwindF" :: wfType, Some(field.shows))
        case $GroupF(_, grouped, -\/(by)) =>
          val nt = "$GroupF" :: wfType
          NonTerminal(nt, None,
            grouped.render ::
              NonTerminal("By" :: nt, None, Reshape.renderReshape(by)) ::
              Nil)
        case $GroupF(_, grouped, \/-(expr)) =>
          val nt = "$GroupF" :: wfType
          NonTerminal(nt, None,
            grouped.render ::
              expr.render.retype(κ("By" :: nt)) ::
              Nil)
        case $SortF(_, value)   =>
          val nt = "$SortF" :: wfType
          NonTerminal(nt, None,
            value.map { case (field, st) => Terminal("SortKey" :: nt, Some(field.asText + " -> " + st)) }.toList)
        case $GeoNearF(_, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs) =>
          val nt = "$GeoNearF" :: wfType
          NonTerminal(nt, None,
            Terminal("Near" :: nt, Some(near.shows)) ::
              Terminal("DistanceField" :: nt, Some(distanceField.shows)) ::
              Terminal("Limit" :: nt, Some(limit.shows)) ::
              Terminal("MaxDistance" :: nt, Some(maxDistance.shows)) ::
              Terminal("Query" :: nt, query ∘ (_.shows)) ::
              Terminal("Spherical" :: nt, Some(spherical.shows)) ::
              Terminal("DistanceMultiplier" :: nt, distanceMultiplier ∘ (_.shows)) ::
              Terminal("IncludeLocs" :: nt, includeLocs ∘ (_.shows)) ::
              Terminal("UniqueDocs" :: nt, Some(uniqueDocs.shows)) ::
              Nil)

        case $MapF(_, fn, scope) =>
          val nt = "$MapF" :: wfType
          NonTerminal(nt, None,
            JSRenderTree.render(fn) ::
              Terminal("Scope" :: nt, Some((scope ∘ (_.toJs.pprint(2))).toString)) ::
              Nil)
        case $FlatMapF(_, fn, scope) =>
          val nt = "$FlatMapF" :: wfType
          NonTerminal(nt, None,
            JSRenderTree.render(fn) ::
              Terminal("Scope" :: nt, Some((scope ∘ (_.toJs.pprint(2))).toString)) ::
              Nil)
        case $SimpleMapF(_, exprs, scope) =>
          val nt = "$SimpleMapF" :: wfType
          NonTerminal(nt, None,
            exprs.toList.map {
              case MapExpr(e)  => NonTerminal("Map" :: nt, None, List(e.render))
        case FlatExpr(e) => NonTerminal("Flatten" :: nt, None, List(e.render))
            } :+
              Terminal("Scope" :: nt, Some((scope ∘ (_.toJs.pprint(2))).toString)))
        case $ReduceF(_, fn, scope) =>
          val nt = "$ReduceF" :: wfType
          NonTerminal(nt, None,
            JSRenderTree.render(fn) ::
              Terminal("Scope" :: nt, Some((scope ∘ (_.toJs.pprint(2))).toString)) ::
              Nil)
        case $OutF(_, coll) => Terminal("$OutF" :: wfType, Some(coll.value))
        case $FoldLeftF(_, _) => Terminal("$FoldLeftF" :: wfType, None)
      }
    }
}
