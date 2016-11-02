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

package quasar.logicalplan

import quasar.Predef._
import quasar._
import quasar.contrib.pathy.{FPath, refineTypeAbs}
import quasar.contrib.shapeless._
import quasar.fp._
import quasar.fp.binder._

import scala.Symbol
import scala.Predef.$conforms

import matryoshka._, Recursive.ops._
import scalaz._, Scalaz._
import shapeless.{Nat, Sized}
import pathy.Path.posixCodec

sealed abstract class LogicalPlan[A] extends Product with Serializable

final case class Read[A](path: FPath) extends LogicalPlan[A]
final case class Constant[A](data: Data) extends LogicalPlan[A]
final case class Invoke[A, N <: Nat](func: GenericFunc[N], values: Func.Input[A, N])
    extends LogicalPlan[A] {
  override def equals(that: scala.Any): Boolean = scala.sys.error("oops")
}
// TODO we create a custom `unapply` to bypass a scalac pattern matching bug
// https://issues.scala-lang.org/browse/SI-5900
object InvokeUnapply {
  def unapply[A, N <: Nat](in: Invoke[A, N])
      : Some[(GenericFunc[N], Func.Input[A, N])] =
    Some((in.func, in.values))
}
final case class Free[A](name: Symbol) extends LogicalPlan[A]
final case class Let[A](let: Symbol, form: A, in: A) extends LogicalPlan[A]
// NB: This should only be inserted by the type checker. In future, this
//     should only exist in BlackShield – the checker will annotate nodes
//     where runtime checks are necessary, then they will be added during
//     compilation to BlackShield.
final case class Typecheck[A](expr: A, typ: Type, cont: A, fallback: A)
    extends LogicalPlan[A]

object LogicalPlan {
  import quasar.std.StdLib._

  implicit val traverse: Traverse[LogicalPlan] =
    new Traverse[LogicalPlan] {
      def traverseImpl[G[_], A, B](
        fa: LogicalPlan[A])(
        f: A => G[B])(
        implicit G: Applicative[G]):
          G[LogicalPlan[B]] =
        fa match {
          case Read(coll)           => G.point(Read(coll))
          case Constant(data)       => G.point(Constant(data))
          case Invoke(func, values) => values.traverse(f).map(Invoke(func, _))
          case Free(v)              => G.point(Free(v))
          case Let(ident, form, in) => (f(form) ⊛ f(in))(Let(ident, _, _))
          case Typecheck(expr, typ, cont, fallback) =>
            (f(expr) ⊛ f(cont) ⊛ f(fallback))(Typecheck(_, typ, _, _))
        }

      override def map[A, B](v: LogicalPlan[A])(f: A => B): LogicalPlan[B] =
        v match {
          case Read(coll)           => Read(coll)
          case Constant(data)       => Constant(data)
          case Invoke(func, values) => Invoke(func, values.map(f))
          case Free(v)              => Free(v)
          case Let(ident, form, in) => Let(ident, f(form), f(in))
          case Typecheck(expr, typ, cont, fallback) =>
            Typecheck(f(expr), typ, f(cont), f(fallback))
        }

      override def foldMap[A, B](fa: LogicalPlan[A])(f: A => B)(implicit B: Monoid[B]): B =
        fa match {
          case Read(_)              => B.zero
          case Constant(_)          => B.zero
          case Invoke(_, values)    => values.foldMap(f)
          case Free(_)              => B.zero
          case Let(_, form, in)     => f(form) ⊹ f(in)
          case Typecheck(expr, _, cont, fallback) =>
            f(expr) ⊹ f(cont) ⊹ f(fallback)
        }

      override def foldRight[A, B](fa: LogicalPlan[A], z: => B)(f: (A, => B) => B): B =
        fa match {
          case Read(_)              => z
          case Constant(_)          => z
          case Invoke(_, values)    => values.foldRight(z)(f)
          case Free(_)              => z
          case Let(ident, form, in) => f(form, f(in, z))
          case Typecheck(expr, _, cont, fallback) =>
            f(expr, f(cont, f(fallback, z)))
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
                case (RenderedTree(_, Some(x), Nil), RenderedTree(_, Some(n), Nil)) =>
                  Terminal("ObjectProject" :: nodeType, Some(x + "{" + n + "}"))
                case (x, n) => NonTerminal("Invoke" :: nodeType, Some(func.shows), x :: n :: Nil)
              }

            case Read(file)                => Terminal("Read" :: nodeType, Some(posixCodec.printPath(file)))
            case Constant(data)            => Terminal("Constant" :: nodeType, Some(data.shows))
            case InvokeUnapply(func, args) => NonTerminal("Invoke" :: nodeType, Some(func.shows), args.unsized.map(ra.render))
            case Free(name)                => Terminal("Free" :: nodeType, Some(name.toString))
            case Let(ident, form, body)    => NonTerminal("Let" :: nodeType, Some(ident.toString), List(ra.render(form), ra.render(body)))
            case Typecheck(expr, typ, cont, fallback) =>
              NonTerminal("Typecheck" :: nodeType, Some(typ.shows),
                List(ra.render(expr), ra.render(cont), ra.render(fallback)))
          }
        }
    }

  implicit val equal: Delay[Equal, LogicalPlan] =
    new Delay[Equal, LogicalPlan] {
      def apply[A](fa: Equal[A]) = {
        implicit val eqA = fa
        Equal.equal {
          case (Read(n1), Read(n2)) => refineTypeAbs(n1) ≟ refineTypeAbs(n2)
          case (Constant(d1), Constant(d2)) => d1 ≟ d2
          case (InvokeUnapply(f1, v1), InvokeUnapply(f2, v2)) => f1 == f2 && v1.unsized ≟ v2.unsized
          case (Free(n1), Free(n2)) => n1 ≟ n2
          case (Let(ident1, form1, in1), Let(ident2, form2, in2)) =>
            ident1 ≟ ident2 && form1 ≟ form2 && in1 ≟ in2
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

      def bindings[T[_[_]]: Recursive, A](t: LogicalPlan[T[LogicalPlan]], b: G[A])(f: LogicalPlan[T[LogicalPlan]] => A): G[A] =
        t match {
          case Let(ident, form, _) => b + (ident -> f(form.project))
          case _                   => b
        }

      def subst[T[_[_]]: Recursive, A](t: LogicalPlan[T[LogicalPlan]], b: G[A]): Option[A] =
        t match {
          case Free(symbol) => b.get(symbol)
          case _            => None
        }
    }
}
