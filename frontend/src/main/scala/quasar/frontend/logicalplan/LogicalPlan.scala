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

package quasar.frontend.logicalplan

import slamdata.Predef._
import quasar._, RenderTree.ops._
import quasar.common.{JoinType, SortDir}
import quasar.contrib.pathy.{FPath, refineTypeAbs}
import quasar.contrib.shapeless._
import quasar.fp._
import quasar.fp.binder._
import quasar.std.TemporalPart

import scala.Symbol
import scala.Predef.$conforms

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._
import shapeless.{Nat, Sized}
import pathy.Path.posixCodec

sealed abstract class LogicalPlan[A] extends Product with Serializable {
  // TODO this should be removed, but usage of `==` is so pervasive in
  // external dependencies (and scalac) that removal may never be possible
  override def equals(that: scala.Any): Boolean = that match {
    case lp: LogicalPlan[A] => LogicalPlan.equal(Equal.equalA[A]).equal(this, lp)
    case _                  => false
  }
}

final case class Read[A](path: FPath) extends LogicalPlan[A]
final case class Constant[A](data: Data) extends LogicalPlan[A]

final case class Invoke[N <: Nat, A](func: GenericFunc[N], values: Func.Input[A, N])
    extends LogicalPlan[A]
// TODO we create a custom `unapply` to bypass a scalac pattern matching bug
// https://issues.scala-lang.org/browse/SI-5900
object InvokeUnapply {
  def unapply[N <: Nat, A](in: Invoke[N, A])
      : Some[(GenericFunc[N], Func.Input[A, N])] =
    Some((in.func, in.values))
}

final case class JoinSideName[A](name: Symbol) extends LogicalPlan[A]
final case class JoinCondition[A](leftName: Symbol, rightName: Symbol, value: A)
final case class Join[A](left: A, right: A, tpe: JoinType, condition: JoinCondition[A])
    extends LogicalPlan[A]

final case class Free[A](name: Symbol) extends LogicalPlan[A]
final case class Let[A](let: Symbol, form: A, in: A) extends LogicalPlan[A]

final case class Sort[A](src: A, order: NonEmptyList[(A, SortDir)])
    extends LogicalPlan[A]

final case class TemporalTrunc[A](part: TemporalPart, src: A) extends LogicalPlan[A]

// NB: This should only be inserted by the type checker. In future, this
//     should only exist in BlackShield – the checker will annotate nodes
//     where runtime checks are necessary, then they will be added during
//     compilation to BlackShield.
final case class Typecheck[A](expr: A, typ: Type, cont: A, fallback: A)
    extends LogicalPlan[A]

object LogicalPlan {
  import quasar.std.StdLib._

  def funcFromJoinType(tpe: JoinType) = tpe match {
    case JoinType.Inner => set.InnerJoin
    case JoinType.LeftOuter => set.LeftOuterJoin
    case JoinType.RightOuter => set.RightOuterJoin
    case JoinType.FullOuter => set.FullOuterJoin
  }

  implicit val traverse: Traverse[LogicalPlan] =
    new Traverse[LogicalPlan] {
      def traverseImpl[G[_], A, B](
        fa: LogicalPlan[A])(
        f: A => G[B])(
        implicit G: Applicative[G]):
          G[LogicalPlan[B]] =
        fa match {
          case Read(coll)               => G.point(Read(coll))
          case Constant(data)           => G.point(Constant(data))
          case Invoke(func, values)     => values.traverse(f).map(Invoke(func, _))
          case JoinSideName(v)          => G.point(JoinSideName(v))
          case Join(l, r, tpe, JoinCondition(lName, rName, cond)) =>
            (f(l) ⊛ f(r) ⊛ f(cond))((v1, v2, v3) => Join(v1, v2, tpe, JoinCondition(lName, rName, v3)))
          case Free(v)                  => G.point(Free(v))
          case Let(ident, form, in)     => (f(form) ⊛ f(in))(Let(ident, _, _))
          case Sort(src, ords)          =>
            (f(src) ⊛ ords.traverse { case (a, d) => f(a) strengthR d })(Sort(_, _))
          case TemporalTrunc(part, src) => f(src) ∘ (TemporalTrunc(part, _))
          case Typecheck(expr, typ, cont, fallback) =>
            (f(expr) ⊛ f(cont) ⊛ f(fallback))(Typecheck(_, typ, _, _))
        }

      override def map[A, B](v: LogicalPlan[A])(f: A => B): LogicalPlan[B] =
        v match {
          case Read(coll)               => Read(coll)
          case Constant(data)           => Constant(data)
          case Invoke(func, values)     => Invoke(func, values.map(f))
          case JoinSideName(v)          => JoinSideName(v)
          case Join(l, r, tpe, JoinCondition(lName, rName, cond)) =>
            Join(f(l), f(r), tpe, JoinCondition(lName, rName, f(cond)))
          case Free(v)                  => Free(v)
          case Let(ident, form, in)     => Let(ident, f(form), f(in))
          case Sort(src, ords)          => Sort(f(src), ords map (f.first))
          case TemporalTrunc(part, src) => TemporalTrunc(part, f(src))
          case Typecheck(expr, typ, cont, fallback) =>
            Typecheck(f(expr), typ, f(cont), f(fallback))
        }

      override def foldMap[A, B](fa: LogicalPlan[A])(f: A => B)(implicit B: Monoid[B]): B =
        fa match {
          case Read(_)               => B.zero
          case Constant(_)           => B.zero
          case Invoke(_, values)     => values.foldMap(f)
          case JoinSideName(_)       => B.zero
          case Join(l, r, _, JoinCondition(_, _, v)) =>
            f(l) ⊹ f(r) ⊹ f(v)
          case Free(_)               => B.zero
          case Let(_, form, in)      => f(form) ⊹ f(in)
          case Sort(src, ords)       => f(src) ⊹ ords.foldMap { case (a, _) => f(a) }
          case TemporalTrunc(_, src) => f(src)
          case Typecheck(expr, _, cont, fallback) =>
            f(expr) ⊹ f(cont) ⊹ f(fallback)
        }

      override def foldRight[A, B](fa: LogicalPlan[A], z: => B)(f: (A, => B) => B): B =
        fa match {
          case Read(_)               => z
          case Constant(_)           => z
          case Invoke(_, values)     => values.foldRight(z)(f)
          case JoinSideName(_)       => z
          case Join(l, r, _, JoinCondition(_, _, v)) =>
            f(l, f(r, f(v, z)))
          case Free(_)               => z
          case Let(ident, form, in)  => f(form, f(in, z))
          case Sort(src, ords)       => f(src, ords.foldRight(z) { case ((a, _), b) => f(a, b) })
          case TemporalTrunc(_, src) => f(src, z)
          case Typecheck(expr, _, cont, fallback) =>
            f(expr, f(cont, f(fallback, z)))
        }
    }

  implicit val show: Delay[Show, LogicalPlan] =
    new Delay[Show, LogicalPlan] {
      def apply[A](sa: Show[A]): Show[LogicalPlan[A]] = {
        implicit val showA: Show[A] = sa
        Show.show {
          case Read(v) =>
            Cord("Read(") ++ v.show ++ Cord(")")
          case Constant(v) =>
            Cord("Constant(") ++ v.show ++ Cord(")")
          case Invoke(func, values) =>
            // TODO remove trailing comma
            func.show ++ Cord("(") ++
            values.foldLeft(Cord("")){ case (acc, v) => acc ++ sa.show(v) ++ Cord(", ") } ++ Cord(")")
          case JoinSideName(n) =>
            Cord("JoinSideName(") ++ Cord(n.toString) ++ Cord(")")
          case Join(l, r, tpe, JoinCondition(lName, rName, v)) =>
            Cord("Join(") ++
            l.show ++ Cord(", ") ++
            r.show ++ Cord(", ") ++
            tpe.show ++ Cord(", ") ++
            Cord(lName.toString) ++ Cord(", ") ++
            Cord(rName.toString) ++ Cord(", ") ++
            v.show ++ Cord(")")
          case Free(n) =>
            Cord("Free(") ++ Cord(n.toString) ++ Cord(")")
          case Let(n, f, b) =>
            Cord("Let(") ++ Cord(n.toString) ++ Cord(",") ++
            sa.show(f) ++ Cord(",") ++ sa.show(b) ++ Cord(")")
          case Sort(src, ords) =>
            Cord("Sort(") ++ sa.show(src) ++ Cord(", ") ++ ords.show ++ Cord(")")
          case TemporalTrunc(part, src) =>
            Cord("TemporalTrunc(") ++ part.show ++ Cord(",") ++ sa.show(src) ++ Cord(")")
          case Typecheck(e, t, c, f) =>
            Cord("Typecheck(") ++ sa.show(e) ++ Cord(",") ++ t.show ++ Cord(",") ++
            sa.show(c) ++ Cord(",") ++ sa.show(f) ++ Cord(")")
        }
      }
    }

  implicit val renderTree: Delay[RenderTree, LogicalPlan] =
    new Delay[RenderTree, LogicalPlan] {
      def apply[A](ra: RenderTree[A]): RenderTree[LogicalPlan[A]] =
        new RenderTree[LogicalPlan[A]] {
          val nodeType = "LogicalPlan" :: Nil

          def render(v: LogicalPlan[A]) = v match {
            // NB: a couple of special cases for readability
            case Constant(Data.Str(str)) => Terminal("Str" :: "Constant" :: nodeType, Some(str.shows))
            case InvokeUnapply(func @ structural.ObjectProject, Sized(expr, name)) =>
              (ra.render(expr), ra.render(name)) match {
                case (exprR @ RenderedTree(_, Some(_), Nil), RenderedTree(_, Some(n), Nil)) =>
                  Terminal("ObjectProject" :: nodeType, Some(exprR.shows + "{" + n + "}"))
                case (x, n) => NonTerminal("Invoke" :: nodeType, Some(func.shows), x :: n :: Nil)
            }

            case Read(file)                => Terminal("Read" :: nodeType, Some(posixCodec.printPath(file)))
            case Constant(data)            => Terminal("Constant" :: nodeType, Some(data.shows))
            case InvokeUnapply(func, args) => NonTerminal("Invoke" :: nodeType, Some(func.shows), args.unsized.map(ra.render))
            case JoinSideName(name)        => Terminal("JoinSideName" :: nodeType, Some(name.toString))
            case Join(l, r, t, JoinCondition(lName, rName, c)) =>
              NonTerminal("Join" :: nodeType, None, List(
                ra.render(l),
                ra.render(r),
                RenderTree[JoinType].render(t),
                Terminal("LeftSide" :: nodeType, Some(lName.toString)),
                Terminal("RightSide" :: nodeType, Some(rName.toString)),
                ra.render(c)))
            case Free(name)                => Terminal("Free" :: nodeType, Some(name.toString))
            case Let(ident, form, body)    => NonTerminal("Let" :: nodeType, Some(ident.toString), List(ra.render(form), ra.render(body)))
            case Sort(src, ords)           =>
              NonTerminal("Sort" :: nodeType, None,
                (ra.render(src) :: ords.list.flatMap {
                  case (a, d) => IList(ra.render(a), d.render)
                }).toList)
            case TemporalTrunc(part, src) =>
              NonTerminal("TemporalTrunc" :: nodeType, Some(part.shows), List(ra.render(src)))
            case Typecheck(expr, typ, cont, fallback) =>
              NonTerminal("Typecheck" :: nodeType, Some(typ.shows),
                List(ra.render(expr), ra.render(cont), ra.render(fallback)))
          }
        }
    }

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  implicit val equal: Delay[Equal, LogicalPlan] =
    new Delay[Equal, LogicalPlan] {
      def apply[A](fa: Equal[A]) = {
        implicit val eqA: Equal[A] = fa
        Equal.equal {
          case (Read(n1), Read(n2)) => refineTypeAbs(n1) ≟ refineTypeAbs(n2)
          case (Constant(d1), Constant(d2)) => d1 ≟ d2
          case (InvokeUnapply(f1, v1), InvokeUnapply(f2, v2)) => f1 == f2 && v1.unsized ≟ v2.unsized  // TODO impl `scalaz.Equal` for `GenericFunc`
          case (JoinSideName(n1), JoinSideName(n2)) => n1 ≟ n2
          case (Join(l1, r1, t1, JoinCondition(lName1, rName1, c1)), Join(l2, r2, t2, JoinCondition(lName2, rName2, c2))) =>
            l1 ≟ l2 && r1 ≟ r2 && t1 ≟ t2 && lName1 ≟ lName2 && rName1 ≟ rName2 && c1 ≟ c2
          case (Free(n1), Free(n2)) => n1 ≟ n2
          case (Let(ident1, form1, in1), Let(ident2, form2, in2)) =>
            ident1 ≟ ident2 && form1 ≟ form2 && in1 ≟ in2
          case (Sort(s1, o1), Sort(s2, o2)) => s1 ≟ s2 && o1 ≟ o2
          case (TemporalTrunc(part1, src1), TemporalTrunc(part2, src2)) =>
            part1 ≟ part2 && src1 ≟ src2
          case (Typecheck(expr1, typ1, cont1, fb1), Typecheck(expr2, typ2, cont2, fb2)) =>
            expr1 ≟ expr2 && typ1 ≟ typ2 && cont1 ≟ cont2 && fb1 ≟ fb2
          case _ => false
        }
      }
    }

  implicit val unzip: Unzip[LogicalPlan] = new Unzip[LogicalPlan] {
    def unzip[A, B](f: LogicalPlan[(A, B)]) = (f.map(_._1), f.map(_._2))
  }

  implicit val binder: Binder[LogicalPlan] = new Binder[LogicalPlan] {
    type G[A] = Map[Symbol, A]
    val G = Traverse[G]

    def initial[A] = Map[Symbol, A]()

    def bindings[T, A]
      (t: LogicalPlan[T], b: G[A])
      (f: LogicalPlan[T] => A)
      (implicit T: Recursive.Aux[T, LogicalPlan])
        : G[A] =
      t match {
        case Let(ident, form, _) => b + (ident -> f(form.project))
        case _                   => b
      }

    def subst[T, A]
      (t: LogicalPlan[T], b: G[A])
      (implicit T: Recursive.Aux[T, LogicalPlan])
        : Option[A] =
      t match {
        case Free(symbol) => b.get(symbol)
        case _            => None
      }
  }
}
