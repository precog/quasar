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

package quasar.physical.mongodb.accumulator

import slamdata.Predef._
import quasar.{RenderTree, Terminal}
import quasar.fp.ski._
import quasar.physical.mongodb.expression.ExprOpOps

import matryoshka._
import matryoshka.data.Fix
import scalaz._, Scalaz._

sealed abstract class AccumOp[A]

object AccumOp {
  final case class $addToSet[A](value: A) extends AccumOp[A]
  final case class $push[A](value: A)     extends AccumOp[A]
  final case class $first[A](value: A)    extends AccumOp[A]
  final case class $last[A](value: A)     extends AccumOp[A]
  final case class $max[A](value: A)      extends AccumOp[A]
  final case class $min[A](value: A)      extends AccumOp[A]
  final case class $avg[A](value: A)      extends AccumOp[A]
  final case class $sum[A](value: A)      extends AccumOp[A]

  implicit val instance: Traverse1[AccumOp] with Comonad[AccumOp]  =
    new Traverse1[AccumOp] with Comonad[AccumOp] {
      def cobind[A, B](fa: AccumOp[A])(f: (AccumOp[A]) ⇒ B) = map(fa)(κ(f(fa)))

      def copoint[A](p: AccumOp[A]) =
        p match {
          case $addToSet(value) => value
          case $avg(value)      => value
          case $first(value)    => value
          case $last(value)     => value
          case $max(value)      => value
          case $min(value)      => value
          case $push(value)     => value
          case $sum(value)      => value
        }

      def foldMapRight1[A, B](fa: AccumOp[A])(z: (A) ⇒ B)(f: (A, ⇒ B) ⇒ B) =
        z(copoint(fa))

      def traverse1Impl[G[_], A, B](fa: AccumOp[A])(f: A => G[B])(implicit G: Apply[G]) =
        fa match {
          case $addToSet(value) => G.map(f(value))($addToSet(_))
          case $avg(value)      => G.map(f(value))($avg(_))
          case $first(value)    => G.map(f(value))($first(_))
          case $last(value)     => G.map(f(value))($last(_))
          case $max(value)      => G.map(f(value))($max(_))
          case $min(value)      => G.map(f(value))($min(_))
          case $push(value)     => G.map(f(value))($push(_))
          case $sum(value)      => G.map(f(value))($sum(_))
        }
    }

  implicit def equal[A: Equal]: Equal[AccumOp[A]] = new Equal[AccumOp[A]] {

    def equal(left: AccumOp[A], right: AccumOp[A]) = (left, right) match {
      case ($addToSet(lv), $addToSet(rv)) => lv === rv
      case ($push(lv), $push(rv)) => lv === rv
      case ($first(lv), $first(rv)) => lv === rv
      case ($last(lv), $last(rv)) => lv === rv
      case ($max(lv), $max(rv)) => lv === rv
      case ($min(lv), $min(rv)) => lv === rv
      case ($avg(lv), $avg(rv)) => lv === rv
      case ($sum(lv), $sum(rv)) => lv === rv
      case _ => false
    }

    override def equalIsNatural = Equal[A].equalIsNatural
  }

  implicit val show: Delay[Show, AccumOp] = new Delay[Show, AccumOp] {
    def apply[A](s: Show[A]) = Show.show {
      case $addToSet(v) => Cord("$addToSet(") ++ s.show(v) ++ Cord(")")
      case $avg(v) => Cord("$avg(") ++ s.show(v) ++ Cord(")")
      case $first(v) => Cord("$first(") ++ s.show(v) ++ Cord(")")
      case $last(v) => Cord("$last(") ++ s.show(v) ++ Cord(")")
      case $max(v) => Cord("$max(") ++ s.show(v) ++ Cord(")")
      case $min(v) => Cord("$min(") ++ s.show(v) ++ Cord(")")
      case $push(v) => Cord("$push(") ++ s.show(v) ++ Cord(")")
      case $sum(v) => Cord("$sum(") ++ s.show(v) ++ Cord(")")
    }
  }

  implicit def renderTree[EX[_]: Functor](implicit exprOps: ExprOpOps.Uni[EX])
      : RenderTree[AccumOp[Fix[EX]]] =
    new RenderTree[AccumOp[Fix[EX]]] {
      def render(v: AccumOp[Fix[EX]]) =
        Terminal(List("AccumOp"), groupBson(v).toJs.pprint(0).some)
    }
}

object $addToSet {
  def apply[A](value: A): AccumOp[A] = AccumOp.$addToSet[A](value)
  def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
    case AccumOp.$addToSet(value) => Some(value)
    case _                      => None
  }
}
object $push {
  def apply[A](value: A): AccumOp[A] = AccumOp.$push[A](value)
  def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
    case AccumOp.$push(value) => Some(value)
    case _                  => None
  }
}
object $first {
  def apply[A](value: A): AccumOp[A] = AccumOp.$first[A](value)
  def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
    case AccumOp.$first(value) => Some(value)
    case _                   => None
  }
}
object $last {
  def apply[A](value: A): AccumOp[A] = AccumOp.$last[A](value)
  def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
    case AccumOp.$last(value) => Some(value)
    case _                  => None
  }
}
object $max {
  def apply[A](value: A): AccumOp[A] = AccumOp.$max[A](value)
  def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
    case AccumOp.$max(value) => Some(value)
    case _                 => None
  }
}
object $min {
  def apply[A](value: A): AccumOp[A] = AccumOp.$min[A](value)
  def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
    case AccumOp.$min(value) => Some(value)
    case _                 => None
  }
}
object $avg {
  def apply[A](value: A): AccumOp[A] = AccumOp.$avg[A](value)
  def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
    case AccumOp.$avg(value) => Some(value)
    case _                 => None
  }
}
object $sum {
  def apply[A](value: A): AccumOp[A] = AccumOp.$sum[A](value)
  def unapply[A](obj: AccumOp[A]): Option[A] = obj match {
    case AccumOp.$sum(value) => Some(value)
    case _          => None
  }
}
