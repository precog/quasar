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

package quasar.ejson

import slamdata.Predef.{Char => SChar, _}
import quasar.{RenderTree, NonTerminal, Terminal}, RenderTree.ops._
import quasar.contrib.matryoshka._
import quasar.fp._

import matryoshka._
import monocle.{Iso, Prism}
import scalaz.{Applicative, Cord, Equal, IMap, Order, Scalaz, Show, Traverse}, Scalaz._

/** This is an extension to JSON that allows arbitrary expressions as map (née
  * object) keys and adds additional primitive types, including characters
  * and distinct integers. It also adds metadata, which allows arbitrary
  * annotations on values.
  */
sealed abstract class Extension[A]
final case class Meta[A](value: A, meta: A)  extends Extension[A]
final case class Map[A](value: List[(A, A)]) extends Extension[A]
final case class Char[A](value: SChar)       extends Extension[A]
final case class Int[A](value: BigInt)       extends Extension[A]

object Extension extends ExtensionInstances {
  def fromObj[A](f: String => A): Obj[A] => Extension[A] =
    obj => Optics.map(obj.value.toList.map(_.leftMap(f)))

  object Optics {
    def char[A] =
      Prism.partial[Extension[A], scala.Char] { case Char(c) => c } (Char(_))

    def int[A] =
      Prism.partial[Extension[A], BigInt] { case Int(i) => i } (Int(_))

    def imap[A: Order] =
      map[A] composeIso listMapIso[A]

    def map[A] =
      Prism.partial[Extension[A], List[(A, A)]] { case Map(m) => m } (Map(_))

    def meta[A] =
      Prism.partial[Extension[A], (A, A)] {
        case Meta(v, m) => (v, m)
      } ((Meta(_: A, _: A)).tupled)

    ////

    private def listMapIso[A: Order]: Iso[List[(A, A)], IMap[A, A]] =
      Iso(IMap.fromList[A, A])(_.toList)
  }
}

sealed abstract class ExtensionInstances {
  import Extension.Optics._

  /** Structural ordering, which _does_ consider metadata and thus needs to
    * be elided before using for proper semantics.
    */
  val structuralOrder: Delay[Order, Extension] =
    new Delay[Order, Extension] {
      def apply[α](ord: Order[α]) = {
        implicit val ordA: Order[α] = ord
        // TODO: Not sure why this isn't found?
        implicit val ordC: Order[SChar] = scalaz.std.anyVal.char
        Order.orderBy { e =>
          val g = generic(e)
          g.copy(_3 = g._3 map (IMap.fromFoldable(_)))
        }
      }
    }

  /** Structural equality, which _does_ consider metadata and thus needs to
    * be elided before using for proper semantics.
    */
  val structuralEqual: Delay[Equal, Extension] =
    new Delay[Equal, Extension] {
      def apply[α](eql: Equal[α]) = {
        implicit val eqlA: Equal[α] = eql
        // TODO: Not sure why this isn't found?
        implicit val eqlC: Equal[SChar] = scalaz.std.anyVal.char
        Equal.equalBy { e =>
          val g = generic(e)
          g.copy(_3 = g._3 map (EqMap.fromFoldable(_)))
        }
      }
    }

  implicit val traverse: Traverse[Extension] = new Traverse[Extension] {
    def traverseImpl[G[_], A, B](
      fa: Extension[A])(
      f: A => G[B])(
      implicit G: Applicative[G]):
        G[Extension[B]] =
      fa match {
        case Meta(value, meta) => (f(value) ⊛ f(meta))(Meta(_, _))
        case Map(value)        => value.traverse(_.bitraverse(f, f)).map(Map(_))
        case Char(value)       => G.point(Char(value))
        case Int(value)        => G.point(Int(value))
      }
  }

  implicit val show: Delay[Show, Extension] =
    new Delay[Show, Extension] {
      def apply[α](eq: Show[α]) = Show.show(a => a match {
        case Meta(v, m) => Cord(s"Meta($v, $m)")
        case Map(v)     => Cord(s"Map($v)")
        case Char(v)    => Cord(s"Char($v)")
        case Int(v)     => Cord(s"Int($v)")
      })
    }

  implicit val renderTree: Delay[RenderTree, Extension] =
    new Delay[RenderTree, Extension] {
      def apply[A](rt: RenderTree[A]) = {
        implicit val rtA = rt
        implicit val sc: Show[SChar] = scalaz.std.anyVal.char
        RenderTree.make {
          case Meta(v, m) =>
            NonTerminal("Meta" :: c, none, List(
              nt("Value", v),
              nt("Metadata", m)))

          case Map(v)  =>
            NonTerminal("Map" :: c, none, v map (_.render))

          case Char(v)  => t("Char", v)
          case Int(v)  => t("Int", v)
        }
      }

      val c = List("Extension")

      def nt[A: RenderTree](l: String, a: A) =
        NonTerminal(l :: c, none, a.render :: Nil)

      def t[A: Show](l: String, a: A) =
        Terminal(l :: c, some(a.shows))
    }

  ////

  private def generic[A](e: Extension[A]) =
    (
      char.getOption(e),
      int.getOption(e),
      map.getOption(e),
      meta.getOption(e)
    )
}
