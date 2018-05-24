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
import quasar._, Type._
import quasar.contrib.matryoshka._
import quasar.fp.ski._
import quasar.physical.mongodb._
import quasar.physical.mongodb.expression.ExprOp
import quasar.physical.mongodb.selector.Selector
import quasar.physical.mongodb.planner.common._
import quasar.qscript._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._

object selector {
  // TODO: This is generalizable to an arbitrary `Recursive` type, I think.
  sealed abstract class InputFinder[T[_[_]]] {
    def apply[A](t: FreeMapA[T, A]): FreeMapA[T, A]
  }

  final case class Here[T[_[_]]]() extends InputFinder[T] {
    def apply[A](a: FreeMapA[T, A]): FreeMapA[T, A] = a
  }

  final case class There[T[_[_]]](index: Int, next: InputFinder[T])
      extends InputFinder[T] {
    def apply[A](a: FreeMapA[T, A]): FreeMapA[T, A] =
      a.resume.fold(fa => next(fa.toList.apply(index)), κ(a))
  }

  // TODO: Need this until the old connector goes away and we can redefine
  //       `Selector` as `Selector[A, B]`, where `A` is the field type
  //       (naturally `BsonField`), and `B` is the recursive parameter.
  type PartialSelector[T[_[_]]] =
    (PartialFunction[List[BsonField], Selector], List[InputFinder[T]])

  type Output[T[_[_]]] = Option[PartialSelector[T]]

  def defaultSelector[T[_[_]]]: PartialSelector[T] = (
    { case List(field) =>
      Selector.Doc(ListMap(
        field -> Selector.CExpr(Selector.Eq(Bson.Bool(true)))))
    },
    List(Here[T]()))

  def invoke2Nel[T[_[_]]]
    (x: Output[T], y: Output[T])
    (f: (Selector, Selector) => Selector)
      : Output[T] =
    (x ⊛ y) { case ((f1, p1), (f2, p2)) =>
      ({ case list =>
        f(f1(list.take(p1.size)), f2(list.drop(p1.size)))
      },
        p1.map(There(0, _)) ++ p2.map(There(1, _)))
    }

  def invoke2Rel[T[_[_]]]
    (x: Output[T], y: Output[T])
    (f: (Selector, Selector) => Selector)
      : Output[T] =
    (x, y) match {
      case (Some((f1, p1)), Some((f2, p2)))=>
        invoke2Nel(x, y)(f)
      case (Some((f1, p1)), None) =>
        (f1, p1.map(There(0, _))).some
      case (None, Some((f2, p2))) =>
        (f2, p2.map(There(1, _))).some
      case _ => none
    }

  def typeSelector[T[_[_]]: RecursiveT: ShowT]:
      GAlgebra[(T[MapFunc[T, ?]], ?), MapFunc[T, ?], Output[T]] = { node =>

    import MapFuncsCore._

    node match {
      // NB: the pick of Selector for these two cases determine how restrictive the
      //     extracted typechecks are. See qz-3500 for more details
      case MFC(And(a, b)) => invoke2Rel(a._2, b._2)(Selector.And(_, _))
      case MFC(Or(a, b))  => invoke2Rel(a._2, b._2)(Selector.Or(_, _))

      // NB: we want to extract typechecks from both sides of a comparison operator
      //     Typechecks extracted from both sides are ANDed. Similarly to the `And`
      //     and `Or` case above, the selector choice can be tweaked depending on how
      //     strict we want to be with extracted typechecks. See qz-3500
      case MFC(Eq(a, b))  => invoke2Rel(a._2, b._2)(Selector.And(_, _))
      case MFC(Neq(a, b)) => invoke2Rel(a._2, b._2)(Selector.And(_, _))
      case MFC(Lt(a, b))  => invoke2Rel(a._2, b._2)(Selector.And(_, _))
      case MFC(Lte(a, b)) => invoke2Rel(a._2, b._2)(Selector.And(_, _))
      case MFC(Gt(a, b))  => invoke2Rel(a._2, b._2)(Selector.And(_, _))
      case MFC(Gte(a, b)) => invoke2Rel(a._2, b._2)(Selector.And(_, _))

      // NB: Undefined() is Hole in disguise here. We don't have a way to directly represent
      //     a FreeMap's leaves with this fixpoint, so we use Undefined() instead.
      case MFC(Guard((Embed(MFC(ProjectKey(Embed(MFC(Undefined())), _))), _), typ, cont, _)) =>
        def selCheck: Type => Option[BsonField => Selector] =
          generateTypeCheck[BsonField, Selector](Selector.Or(_, _)) {
            case Type.Null => ((f: BsonField) =>  Selector.Doc(f -> Selector.Type(BsonType.Null)))
            case Type.Dec => ((f: BsonField) => Selector.Doc(f -> Selector.Type(BsonType.Dec)))
            case Type.Int =>
              ((f: BsonField) => Selector.Or(
                Selector.Doc(f -> Selector.Type(BsonType.Int32)),
                Selector.Doc(f -> Selector.Type(BsonType.Int64))))
            case Type.Int ⨿ Type.Dec ⨿ Type.Interval =>
              ((f: BsonField) =>
                Selector.Or(
                  Selector.Doc(f -> Selector.Type(BsonType.Int32)),
                  Selector.Doc(f -> Selector.Type(BsonType.Int64)),
                  Selector.Doc(f -> Selector.Type(BsonType.Dec))))
            case Type.Str => ((f: BsonField) => Selector.Doc(f -> Selector.Type(BsonType.Text)))
            case Type.Obj(_, _) =>
              ((f: BsonField) => Selector.Doc(f -> Selector.Type(BsonType.Doc)))

            // NB: Selector.Type(BsonType.Arr) will not match arrays, instead we use the suggestion in Mongo docs
            // See: https://docs.mongodb.com/manual/reference/operator/query/type/#document-querying-by-array-type
            case Type.FlexArr(_, _, _) =>
              ((f: BsonField) => Selector.Doc(f -> Selector.ElemMatch(Selector.Exists(true).right)))
            case Type.Binary =>
              ((f: BsonField) => Selector.Doc(f -> Selector.Type(BsonType.Binary)))
            case Type.Id =>
              ((f: BsonField) => Selector.Doc(f -> Selector.Type(BsonType.ObjectId)))
            case Type.Bool =>
              ((f: BsonField) => Selector.Doc(f -> Selector.Type(BsonType.Bool)))
            case Type.OffsetDateTime | Type.OffsetDate | Type.OffsetTime |
                Type.LocalDateTime | Type.LocalDate | Type.LocalTime =>
              ((f: BsonField) => Selector.Doc(f -> Selector.Type(BsonType.Date)))
          }

        def g(f: BsonField => Selector): PartialSelector[T] =
          cont._2.fold[PartialSelector[T]](
            ({ case List(field) => f(field) }, List(There(0, Here[T]()))))(
            { case (f2, p2) => (
              { case head :: tail => Selector.And(f(head), f2(tail)) },
              There(0, Here[T]()) :: p2.map(There(1, _)))})

        selCheck(typ).map(g)

      case _ => none
    }
  }

  /** The selector phase tries to turn expressions into MongoDB selectors – i.e.
    * Mongo query expressions. Selectors are only used for the filtering
    * pipeline op, so it's quite possible we build more stuff than is needed
    * (but it doesn’t matter, unneeded annotations will be ignored by the
    * pipeline phase).
    *
    * Like the expression op phase, this one requires bson field annotations.
    *
    * Most expressions cannot be turned into selector expressions without using
    * the "\$where" operator, which allows embedding JavaScript
    * code. Unfortunately, using this operator turns filtering into a full table
    * scan. We should do a pass over the tree to identify partial boolean
    * expressions which can be turned into selectors, factoring out the
    * leftovers for conversion using \$where.
    */
  def selector[T[_[_]]: BirecursiveT: ShowT: RenderTreeT](v: BsonVersion)
      : GAlgebra[(T[MapFunc[T, ?]], ?), MapFunc[T, ?], Output[T]] = { node =>
    import MapFuncsCore._

    object IsBson {
      def unapply(x: (T[MapFunc[T, ?]], Output[T])): Option[Bson] =
        x._1.project match {
          case MFC(Constant(b)) => b.cataM(BsonCodec.fromEJson(v)).toOption
          case _ => None
        }
    }

    object IsBool {
      def unapply(v: (T[MapFunc[T, ?]], Output[T])): Option[Boolean] =
        v match {
          case IsBson(Bson.Bool(b)) => b.some
          case _                    => None
        }
    }

    object IsText {
      def unapply(v: (T[MapFunc[T, ?]], Output[T])): Option[String] =
        v match {
          case IsBson(Bson.Text(str)) => Some(str)
          case _                      => None
        }
    }

    val relFunc: MapFunc[T, _] => Option[Bson => Selector.Condition] = {
      case MFC(Eq(_, _))  => Some(Selector.Eq)
      case MFC(Neq(_, _)) => Some(Selector.Neq)
      case MFC(Lt(_, _))  => Some(Selector.Lt)
      case MFC(Lte(_, _)) => Some(Selector.Lte)
      case MFC(Gt(_, _))  => Some(Selector.Gt)
      case MFC(Gte(_, _)) => Some(Selector.Gte)
      case _              => None
    }

    val default: PartialSelector[T] = defaultSelector[T]

    def invoke(func: MapFunc[T, (T[MapFunc[T, ?]], Output[T])]): Output[T] = {
      /**
        * All the relational operators require a field as one parameter, and
        * BSON literal value as the other parameter. So we have to try to
        * extract out both a field annotation and a selector and then verify
        * the selector is actually a BSON literal value before we can
        * construct the relational operator selector. If this fails for any
        * reason, it just means the given expression cannot be represented
        * using MongoDB's query operators, and must instead be written as
        * Javascript using the "$where" operator.
        */
      def relop
        (x: (T[MapFunc[T, ?]], Output[T]), y: (T[MapFunc[T, ?]], Output[T]))
        (f: Bson => Selector.Condition, r: Bson => Selector.Condition):
          Output[T] =
        (x, y) match {
          case (_, IsBson(v2)) =>
            Some(({ case List(f1) => Selector.Doc(ListMap(f1 -> Selector.CExpr(f(v2)))) }, List(There(0, Here[T]()))))
          case (IsBson(v1), _) =>
            Some(({ case List(f2) => Selector.Doc(ListMap(f2 -> Selector.CExpr(r(v1)))) }, List(There(1, Here[T]()))))

          case (_, _) => none
        }

      val flipCore: MapFuncCore[T, _] => Option[MapFuncCore[T, _]] = {
        case Eq(a, b)  => Some(Eq(a, b))
        case Neq(a, b) => Some(Neq(a, b))
        case Lt(a, b)  => Some(Gt(a, b))
        case Lte(a, b) => Some(Gte(a, b))
        case Gt(a, b)  => Some(Lt(a, b))
        case Gte(a, b) => Some(Lte(a, b))
        case And(a, b) => Some(And(a, b))
        case Or(a, b)  => Some(Or(a, b))
        case _         => None
      }

      val flip: MapFunc[T, _] => Option[MapFunc[T, _]] = {
        case MFC(mfc) => flipCore(mfc).map(MFC(_))
        case _ => None
      }

      def reversibleRelop(x: (T[MapFunc[T, ?]], Output[T]), y: (T[MapFunc[T, ?]], Output[T]))(f: MapFunc[T, _]): Output[T] =
        (relFunc(f) ⊛ flip(f).flatMap(relFunc))(relop(x, y)(_, _)).join

      func match {
        case MFC(Constant(_)) => default.some
        case MFC(And(a, b))   => invoke2Nel(a._2, b._2)(Selector.And.apply _)
        case MFC(Or(a, b))    => invoke2Nel(a._2, b._2)(Selector.Or.apply _)

        case MFC(Eq(a, b))  => reversibleRelop(a, b)(func)
        case MFC(Neq(a, b)) => reversibleRelop(a, b)(func)
        case MFC(Lt(a, b))  => reversibleRelop(a, b)(func)
        case MFC(Lte(a, b)) => reversibleRelop(a, b)(func)
        case MFC(Gt(a, b))  => reversibleRelop(a, b)(func)
        case MFC(Gte(a, b)) => reversibleRelop(a, b)(func)

        // NB: workaround patmat exhaustiveness checker bug. Merge with previous `match`
        //     once solved.
        case x => x match {
          case MFC(Within(a, b)) =>
            relop(a, b)(
              Selector.In.apply _,
              x => Selector.ElemMatch(\/-(Selector.In(Bson.Arr(List(x))))))

          case MFC(Search(_, IsText(patt), IsBool(b))) =>
            Some(({ case List(f1) =>
              Selector.Doc(ListMap(f1 -> Selector.CExpr(Selector.Regex(patt, b, true, false, false)))) },
              List(There(0, Here[T]()))))

          case MFC(Between(_, IsBson(lower), IsBson(upper))) =>
            Some(({ case List(f) => Selector.And(
              Selector.Doc(f -> Selector.Gte(lower)),
              Selector.Doc(f -> Selector.Lte(upper)))
            },
              List(There(0, Here[T]()))))

          case MFC(Not((_, v))) =>
            v.map { case (sel, inputs) => (sel andThen (_.negate), inputs.map(There(0, _))) }

          case MFC(Guard(_, typ, (_, cont), (Embed(MFC(Undefined())), _))) =>
            cont.map { case (sel, inputs) => (sel, inputs.map(There(1, _))) }
          case MFC(Guard(_, typ, (_, cont), (Embed(MFC(MakeArray(Embed(MFC(Undefined()))))), _))) =>
            cont.map { case (sel, inputs) => (sel, inputs.map(There(1, _))) }

          case _ => none
        }
      }
    }

    invoke(node)
  }

  def getSelector
    [T[_[_]]: BirecursiveT: ShowT, M[_]: Monad, EX[_]: Traverse, A]
    (fm: FreeMapA[T, A], default: Output[T], galg: GAlgebra[(T[MapFunc[T, ?]], ?), MapFunc[T, ?], Output[T]])
    (implicit inj: EX :<: ExprOp)
      : Output[T] =
    fm.zygo(
      interpret[MapFunc[T, ?], A, T[MapFunc[T, ?]]](
        κ(MFC(MapFuncsCore.Undefined[T, T[MapFunc[T, ?]]]()).embed),
        _.embed),
      ginterpret[(T[MapFunc[T, ?]], ?), MapFunc[T, ?], A, Output[T]](
        κ(default), galg))
}
