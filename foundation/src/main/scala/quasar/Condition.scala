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

package quasar

import slamdata.Predef.{None, Option, Product, Serializable, Some, Unit}
import quasar.fp.ski.κ

import monocle.{Iso, PPrism, Prism}
import scalaz._, Scalaz._

sealed trait Condition[E] extends Product with Serializable {
  def flatMap[EE](f: E => Condition[EE]): Condition[EE] =
    this match {
      case Condition.Abnormal(e) => f(e)
      case Condition.Normal()    => Condition.normal()
    }

  def map[EE](f: E => EE): Condition[EE] =
    Condition.pAbnormal.modify(f)(this)

  def orElse(other: => Condition[E]): Condition[E] =
    this match {
      case Condition.Abnormal(_) => this
      case Condition.Normal()    => other
    }
}

object Condition extends ConditionInstances {
  final case class Abnormal[E](error: E) extends Condition[E]
  final case class Normal[E]() extends Condition[E]

  def pAbnormal[E1, E2]: PPrism[Condition[E1], Condition[E2], E1, E2] =
    PPrism[Condition[E1], Condition[E2], E1, E2] {
      case Abnormal(a) => a.right
      case Normal()    => (Normal[E2](): Condition[E2]).left
    } (Abnormal(_))

  def abnormal[E]: Prism[Condition[E], E] =
    pAbnormal[E, E]

  def normal[E]: Prism[Condition[E], Unit] =
    Prism.partial[Condition[E], Unit] {
      case Normal() => ()
    } (κ(Normal()))

  def disjunctionIso[E]: Iso[Condition[E], E \/ Unit] =
    Iso[Condition[E], E \/ Unit] {
      case Abnormal(e) => e.left
      case Normal()    => ().right
    } (_.fold(Abnormal(_), κ(Normal())))

  def optionIso[E]: Iso[Condition[E], Option[E]] =
    Iso[Condition[E], Option[E]] {
      case Abnormal(e) => Some(e)
      case Normal()    => None
    } {
      case Some(e)     => Abnormal(e)
      case None        => Normal()
    }
}

sealed abstract class ConditionInstances extends ConditionInstances0 {
  import Condition.optionIso

  implicit val covariant: Align[Condition] with Cobind[Condition] with MonadPlus[Condition] with Traverse[Condition] =
    new Align[Condition] with Cobind[Condition] with MonadPlus[Condition] with Traverse[Condition] {
      def alignWith[A, B, C](f: A \&/ B => C) = {
        case (a, b) =>
          optionIso.reverseGet(optionIso.get(a).alignWith(optionIso.get(b))(f))
      }

      def bind[A, B](fa: Condition[A])(f: A => Condition[B]) =
        fa flatMap f

      def cobind[A, B](fa: Condition[A])(f: Condition[A] => B) =
        fa map (a => f(Condition.abnormal(a)))

      def empty[A] =
        Condition.normal()

      override def foldMap[A, B: Monoid](fa: Condition[A])(f: A => B) =
        Condition.abnormal.asFold.foldMap(f)(fa)

      override def map[A, B](fa: Condition[A])(f: A => B) =
        fa map f

      def plus[A](a: Condition[A], b: => Condition[A]) =
        a orElse b

      def point[A](a: => A) =
        Condition.abnormal(a)

      def traverseImpl[F[_]: Applicative, A, B](fa: Condition[A])(f: A => F[B]) =
        Condition.pAbnormal.modifyF(f)(fa)
    }

  implicit val cozip: Cozip[Condition] =
    new Cozip[Condition] {
      def cozip[A, B](x: Condition[A \/ B]) =
        Cozip[Option].cozip(optionIso.get(x))
          .bimap(optionIso.reverseGet, optionIso.reverseGet)
    }

  implicit def monoid[E: Semigroup]: Monoid[Condition[E]] =
    Monoid.instance[Condition[E]](
      (x, y) => optionIso.reverseGet(Monoid[Option[E]].append(optionIso.get(x), optionIso.get(y))),
      Condition.normal())

  implicit val unzip: Unzip[Condition] =
    new Unzip[Condition] {
      def unzip[A, B](x: Condition[(A, B)]) =
        optionIso.get(x)
          .unfzip
          .bimap(optionIso.reverseGet, optionIso.reverseGet)
    }

  implicit val zip: Zip[Condition] =
    new Zip[Condition] {
      def zip[A, B](a: => Condition[A], b: => Condition[B]) =
        optionIso.reverseGet(optionIso.get(a).fzip(optionIso.get(b)))
    }

  implicit def order[E: Order]: Order[Condition[E]] =
    Order.orderBy(optionIso.get(_))

  implicit def show[E: Show]: Show[Condition[E]] =
    Show.show {
      case Condition.Abnormal(e) => Cord("Abnormal(") ++ e.show ++ Cord(")")
      case Condition.Normal()    => Cord("Normal")
    }
}

sealed abstract class ConditionInstances0 {
  implicit def equal[E: Equal]: Equal[Condition[E]] =
    Equal.equalBy(Condition.optionIso.get(_))
}
