/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.ejson

import slamdata.Predef._
import quasar.{RenderTree, NonTerminal, Terminal}, RenderTree.ops._
import quasar.fp.ski.κ

import matryoshka._
import monocle.Prism
import scalaz.{Applicative, Equal, Order, Scalaz, Show, Traverse}, Scalaz._

sealed abstract class Common[A]
final case class Arr[A](value: List[A])    extends Common[A]
final case class Null[A]()                 extends Common[A]
final case class Bool[A](value: Boolean)   extends Common[A]
final case class Str[A](value: String)     extends Common[A]
final case class Dec[A](value: BigDecimal) extends Common[A]

object Common extends CommonInstances {
  object Optics {
    def arr[A] =
      Prism.partial[Common[A], List[A]] { case Arr(a) => a } (Arr(_))

    def bool[A] =
      Prism.partial[Common[A], Boolean] { case Bool(b) => b } (Bool(_))

    def dec[A] =
      Prism.partial[Common[A], BigDecimal] { case Dec(bd) => bd } (Dec(_))

    def nul[A] =
      Prism.partial[Common[A], Unit] { case Null() => () } (κ(Null()))

    def str[A] =
      Prism.partial[Common[A], String] { case Str(s) => s } (Str(_))
  }
}

sealed abstract class CommonInstances extends CommonInstances0 {
  implicit val traverse: Traverse[Common] = new Traverse[Common] {
    def traverseImpl[G[_], A, B](
      fa: Common[A])(
      f: A => G[B])(
      implicit G: Applicative[G]):
        G[Common[B]] =
      fa match {
        case Arr(value)  => value.traverse(f).map(Arr(_))
        case Null()      => G.point(Null())
        case Bool(value) => G.point(Bool(value))
        case Str(value)  => G.point(Str(value))
        case Dec(value)  => G.point(Dec(value))
      }
  }

  implicit val order: Delay[Order, Common] =
    new Delay[Order, Common] {
      def apply[α](ord: Order[α]) = {
        implicit val ordA: Order[α] = ord
        Order.orderBy(generic)
      }
    }

  implicit val show: Delay[Show, Common] =
    new Delay[Show, Common] {
      def apply[α](eq: Show[α]) = Show.shows(a => a match {
        case Arr(v)  => s"Arr($v)"
        case Null()  => "Null()"
        case Bool(v) => s"Bool($v)"
        case Str(v)  => s"Str($v)"
        case Dec(v)  => s"Dec($v)"
      })
    }

  implicit val renderTree: Delay[RenderTree, Common] =
    new Delay[RenderTree, Common] {
      def apply[A](rt: RenderTree[A]) = {
        implicit val rtA = rt
        RenderTree.make {
          case Arr(vs) => NonTerminal("Array" :: c, none, vs map (_.render))
          case Null()  => Terminal("Null" :: c, none)
          case Bool(b) => t("Bool", b)
          case Str(v)  => t("Str", v)
          case Dec(v)  => t("Dec", v)
        }
      }

      val c = List("Common")

      def t[A: Show](l: String, a: A) =
        Terminal(l :: c, some(a.shows))
    }
}

sealed abstract class CommonInstances0 {
  import Common.Optics._

  implicit val equal: Delay[Equal, Common] =
    new Delay[Equal, Common] {
      def apply[α](eql: Equal[α]) = {
        implicit val eqlA: Equal[α] = eql
        Equal.equalBy(generic)
      }
    }

  ////

  private[ejson] def generic[A](c: Common[A]) = (
    arr.getOption(c) ,
    bool.getOption(c),
    dec.getOption(c) ,
    nul.nonEmpty(c),
    str.getOption(c)
  )
}
