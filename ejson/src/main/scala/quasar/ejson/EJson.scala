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

package quasar.ejson

import slamdata.Predef.{Byte => SByte, Char => SChar, Map => SMap, _}
import quasar.{RenderTree, NonTerminal, Terminal}, RenderTree.ops._
import quasar.contrib.matryoshka._
import quasar.fp._

import matryoshka._
import scalaz._, Scalaz._

sealed abstract class Common[A]
final case class Arr[A](value: List[A])    extends Common[A]
final case class Null[A]()                 extends Common[A]
final case class Bool[A](value: Boolean)   extends Common[A]
final case class Str[A](value: String)     extends Common[A]
final case class Dec[A](value: BigDecimal) extends Common[A]

object Common extends CommonInstances

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
      def apply[α](eq: Show[α]) = Show.show(a => a match {
        case Arr(v)  => Cord(s"Arr($v)")
        case Null()  => Cord("Null()")
        case Bool(v) => Cord(s"Bool($v)")
        case Str(v)  => Cord(s"Str($v)")
        case Dec(v)  => Cord(s"Dec($v)")
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

final case class Obj[A](value: ListMap[String, A])

object Obj extends ObjInstances

sealed abstract class ObjInstances extends ObjInstances0 {
  implicit val traverse: Traverse[Obj] = new Traverse[Obj] {
    def traverseImpl[G[_]: Applicative, A, B](fa: Obj[A])(f: A => G[B]):
        G[Obj[B]] =
      fa.value.traverse(f).map(Obj(_))
  }

  implicit val order: Delay[Order, Obj] =
    new Delay[Order, Obj] {
      def apply[α](ord: Order[α]) = {
        implicit val ordA: Order[α] = ord
        Order.orderBy(_.value: SMap[String, α])
      }
    }

  implicit val show: Delay[Show, Obj] =
    new Delay[Show, Obj] {
      def apply[α](shw: Show[α]) = {
        implicit val shwA: Show[α] = shw
        Show.show(o => (o.value: SMap[String, α]).show)
      }
    }

  implicit val renderTree: Delay[RenderTree, Obj] =
    new Delay[RenderTree, Obj] {
      def apply[A](rt: RenderTree[A]) = {
        implicit val rtA = rt
        RenderTree.make(obj => NonTerminal(List("Obj"), None, List(obj.value.render)))
      }
    }
}

sealed abstract class ObjInstances0 {
  implicit val equal: Delay[Equal, Obj] =
    new Delay[Equal, Obj] {
      def apply[α](eql: Equal[α]) = {
        implicit val eqlA: Equal[α] = eql
        Equal.equalBy(_.value: SMap[String, α])
      }
    }
}

/** This is an extension to JSON that allows arbitrary expressions as map (née
  * object) keys and adds additional primitive types, including characters,
  * bytes, and distinct integers. It also adds metadata, which allows arbitrary
  * annotations on values.
  */
sealed abstract class Extension[A]
final case class Meta[A](value: A, meta: A)  extends Extension[A]
final case class Map[A](value: List[(A, A)]) extends Extension[A]
final case class Byte[A](value: SByte)       extends Extension[A]
final case class Char[A](value: SChar)       extends Extension[A]
final case class Int[A](value: BigInt)       extends Extension[A]

object Extension extends ExtensionInstances {
  def fromObj[A](f: String => A): Obj[A] => Extension[A] =
    obj => map(obj.value.toList.map(_.leftMap(f)))
}

sealed abstract class ExtensionInstances {
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
          g.copy(_4 = g._4 map (IMap.fromFoldable(_)))
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
          g.copy(_4 = g._4 map (EqMap.fromFoldable(_)))
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
        case Byte(value)       => G.point(Byte(value))
        case Char(value)       => G.point(Char(value))
        case Int(value)        => G.point(Int(value))
      }
  }

  implicit val show: Delay[Show, Extension] =
    new Delay[Show, Extension] {
      def apply[α](eq: Show[α]) = Show.show(a => a match {
        case Meta(v, m) => Cord(s"Meta($v, $m)")
        case Map(v)     => Cord(s"Map($v)")
        case Byte(v)    => Cord(s"Byte($v)")
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

          case Byte(b) => t("Byte", b)
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
      byte.getOption(e),
      char.getOption(e),
      int.getOption(e) ,
      map.getOption(e) ,
      meta.getOption(e)
    )
}
