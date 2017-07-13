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

package quasar.physical.mongodb

import scala.Predef.$conforms
import slamdata.Predef._
import quasar.fp._
import quasar.fp.ski._
import quasar._, Planner._
import quasar.jscore, jscore.{JsFn}
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._

import scala.collection.immutable.Iterable

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._

final case class Grouped[EX[_]](value: ListMap[BsonField.Name, AccumOp[Fix[EX]]]) {
  def bson(implicit ev0: ExprOpOps.Uni[EX], ev1: Functor[EX]) =
    Bson.Doc(value.map(t => t._1.asText -> groupBson(t._2)))

  def rewriteRefs(f: PartialFunction[DocVar, DocVar])
      (implicit ev0: ExprOpOps.Uni[EX], ev1: Functor[EX]): Grouped[EX] =
    Grouped(value.transform((_, v) => rewriteGroupRefs(v)(f)))
}
object Grouped {

  def grouped[EX[_]](shape: (String, AccumOp[Fix[EX]])*) =
    Grouped(ListMap(shape.map { case (k, v) => BsonField.Name(k) -> v}: _*))

  implicit def GroupedRenderTree[EX[_]: Functor](implicit ev: ExprOpOps.Uni[EX])
      : RenderTree[Grouped[EX]] = new RenderTree[Grouped[EX]] {
    val GroupedNodeType = List("Grouped")

    def render(grouped: Grouped[EX]) =
      NonTerminal(GroupedNodeType, None,
        (grouped.value.map { case (name, expr) => Terminal("Name" :: GroupedNodeType, Some(name.bson.toJs.pprint(0) + " -> " + groupBson(expr).toJs.pprint(0))) } ).toList)
  }
}

final case class Reshape[EX[_]](value: ListMap[BsonField.Name, Reshape.Shape[EX]]) {
  import Reshape.Shape

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def toJs(implicit
      ev0: ExprOpOps.Uni[EX],
      ev1: ExprOpCoreF :<: EX,
      ev2: Traverse[EX],
      ev3: Equal[Fix[EX]]): PlannerError \/ JsFn =
    value.map { case (key, expr) =>
      key.asText -> expr.fold[PlannerError \/ JsFn](_.toJs, _.para(expression.toJs[Fix[EX], EX]))
    }.sequence.map { l => JsFn(JsFn.defaultName,
      jscore.Obj(l.map { case (k, v) => jscore.Name(k) -> v(jscore.Ident(JsFn.defaultName)) }))
    }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def bson(implicit ops: ExprOpOps.Uni[EX], ev: Functor[EX]): Bson.Doc = Bson.Doc(value.map {
    case (field, either) => field.asText -> either.fold(_.bson, _.cata(ops.bson))
  })

  private def projectSeq(fs: NonEmptyList[BsonField.Name]): Option[Shape[EX]] =
    fs.foldLeftM[Option, Shape[EX]](-\/(this))((rez, leaf) =>
      rez.fold(
        r => leaf match {
          case n @ BsonField.Name(_) => r.get(n)
          case _                     => None
        },
        κ(None)))

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def rewriteRefs(applyVar: PartialFunction[DocVar, DocVar])(implicit
      ops: ExprOpOps.Uni[EX],
      ev0: ExprOpCoreF :<: EX,
      ev1: Functor[EX]): Reshape[EX] =
    Reshape(value.transform((k, v) => v.bimap(
      _.rewriteRefs(applyVar),
      x => (x match {
        case $include() => (new ExprOpCoreF.fixpoint[Fix[EX], EX](_.embed)).$var(DocField(k))
        case _          => x
      }).cata(ops.rewriteRefs(applyVar)))))

  def \ (f: BsonField): Option[Shape[EX]] = projectSeq(f.flatten)

  def get(field: BsonField): Option[Shape[EX]] =
    field.flatten.foldLeftM[Option, Shape[EX]](
      -\/(this))(
      (rez, elem) => rez.fold(_.value.get(elem), κ(None)))

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def find(expr: Fix[EX]): Option[BsonField] =
    value.foldLeft[Option[BsonField]](
      None) {
      case (acc, (field, value)) =>
        acc.orElse(value.fold(
          _.find(expr).map(field \ _),
          ex => if (ex == expr) field.some else None))
    }

  def set(field: BsonField, newv: Shape[EX]): Reshape[EX] = {
    def getOrDefault(o: Option[Shape[EX]]): Reshape[EX] =
      o.cata(
        _.fold(ι, κ(Reshape.emptyDoc[EX])),
        Reshape.emptyDoc[EX])

    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements", "org.wartremover.warts.Recursion"))
    def set0(cur: Reshape[EX], els: List[BsonField.Name]): Reshape[EX] = els match {
      case Nil => ??? // TODO: Refactor els to be NonEmptyList
      case (x @ BsonField.Name(_)) :: Nil => Reshape(cur.value + (x -> newv))
      case (x @ BsonField.Name(_)) :: xs =>
        Reshape(cur.value + (x -> -\/(set0(getOrDefault(cur.value.get(x)), xs))))
    }

    set0(this, field.flatten.toList)
  }
}

object Reshape {
  type Shape[EX[_]] = Reshape[EX] \/ Fix[EX]

  def reshape(shape: (String, Shape[ExprOp])*) =
    Reshape(ListMap(shape.map { case (k, v) => BsonField.Name(k) -> v}: _*))

  def emptyDoc[EX[_]] = Reshape[EX](ListMap())

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def getAll[EX[_]](r: Reshape[EX]): List[(BsonField, Fix[EX])] = {
    def getAll0(f0: BsonField, e: Shape[EX]) = e.fold(
      r => getAll(r).map { case (f, e) => (f0 \ f) -> e },
      e => (f0 -> e) :: Nil)

    r.value.toList.map { case (f, e) => getAll0(f, e) }.flatten
  }

  def setAll[EX[_]](r: Reshape[EX], fvs: Iterable[(BsonField, Shape[EX])]) =
    fvs.foldLeft(r) {
      case (r0, (field, value)) => r0.set(field, value)
    }

  def mergeMaps[A, B](lmap: ListMap[A, B], rmap: ListMap[A, B]):
      Option[ListMap[A, B]] =
    if ((lmap.keySet & rmap.keySet).forall(k => lmap.get(k) == rmap.get(k)))
      Some(lmap ++ rmap)
    else None

  def merge[EX[_]](r1: Reshape[EX], r2: Reshape[EX]): Option[Reshape[EX]] = {
    val lmap = Reshape.getAll(r1).map(t => t._1 -> \/-(t._2)).toListMap
    val rmap = Reshape.getAll(r2).map(t => t._1 -> \/-(t._2)).toListMap
    if ((lmap.keySet & rmap.keySet).forall(k => lmap.get(k) == rmap.get(k)))
      Some(Reshape.setAll(
        r1,
        Reshape.getAll(r2).map(t => t._1 -> \/-(t._2))))
    else None
  }

  private val ProjectNodeType = List("Project")

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private[mongodb] def renderReshape[EX[_]: Functor](shape: Reshape[EX])(implicit ops: ExprOpOps.Uni[EX]): List[RenderedTree] = {
    def renderField(field: BsonField, value: Shape[EX]) = {
      val (label, typ) = field.bson.toJs.pprint(0) -> "Name"
      value match {
        case -\/ (shape)  => NonTerminal(typ :: ProjectNodeType, Some(label), renderReshape(shape))
        case  \/-(exprOp) => Terminal(typ :: ProjectNodeType, Some(label + " -> " + exprOp.cata(ops.bson).toJs.pprint(0)))
      }
    }

    shape.value.map((renderField(_, _)).tupled).toList
  }
}
