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

import quasar.Predef._
import quasar.fp._

import matryoshka._
import scalaz._, Scalaz._

sealed trait Common[A]
final case class Arr[A](value: List[A])    extends Common[A]
final case class Null[A]()                 extends Common[A]
final case class Bool[A](value: Boolean)   extends Common[A]
final case class Str[A](value: String)     extends Common[A]
final case class Dec[A](value: BigDecimal) extends Common[A]

object Common {
  def unapply[F[_], A](fa: F[A])(implicit C: Common :<: F): Option[Common[A]] =
    C.prj(fa)

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

  implicit val equal: Delay[Equal, Common] =
    new Delay[Equal, Common] {
      def apply[α](eq: Equal[α]) = Equal.equal((a, b) => (a, b) match {
        case (Arr(l),       Arr(r))  => listEqual(eq).equal(l, r)
        case (Arr(_),       _)       => false
        case (Null(),       Null())  => true
        case (Null(),       _)       => false
        case (Bool(l),      Bool(r)) => l ≟ r
        case (Bool(_),      _)       => false
        case (Str(l),       Str(r))  => l ≟ r
        case (Str(_),       _)       => false
        case (Dec(l),       Dec(r))  => l ≟ r
        case (Dec(_),       _)       => false
      })
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
}

final case class Obj[A](value: ListMap[String, A])

object Obj {
  // TODO: This means we have the same names for the projection and the Obj
  //       pattern matcher. It could go away if this unapply were defined on
  //       `scalaz.Inject`. (scalaz/scalaz#1311)
  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  def unapply[F[_], A](fa: F[A])(implicit O: Obj :<: F): Option[Obj[A]] =
    O.prj(fa)

  implicit val traverse: Traverse[Obj] = new Traverse[Obj] {
    def traverseImpl[G[_]: Applicative, A, B](fa: Obj[A])(f: A => G[B]):
        G[Obj[B]] =
      fa.value.traverse(f).map(Obj(_))
  }

  implicit val equal: Delay[Equal, Obj] =
    new Delay[Equal, Obj] {
      def apply[α](eq: Equal[α]) = Equal.equal((a, b) =>
        mapEqual(stringInstance, eq).equal(a.value, b.value))
    }

  implicit val show: Delay[Show, Obj] =
    new Delay[Show, Obj] {
      def apply[α](shw: Show[α]) = Show.show(a =>
        mapShow(stringInstance, shw).show(a.value))
    }
}

/** This is an extension to JSON that allows arbitrary expressions as map (née
  * object) keys and adds additional primitive types, including characters,
  * bytes, and distinct integers. It also adds metadata, which allows arbitrary
  * annotations on values.
  */
sealed trait Extension[A]
final case class Meta[A](value: A, meta: A)  extends Extension[A]
final case class Map[A](value: List[(A, A)]) extends Extension[A]
final case class Byte[A](value: scala.Byte)  extends Extension[A]
final case class Char[A](value: scala.Char)  extends Extension[A]
final case class Int[A](value: BigInt)       extends Extension[A]

object Extension {
  def unapply[F[_], A](fa: F[A])(implicit E: Extension :<: F): Option[Extension[A]] =
    E.prj(fa)

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

  // TODO need Equal[Char]
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  implicit val equal: Delay[Equal, Extension] =
    new Delay[Equal, Extension] {
      def apply[α](eq: Equal[α]) = Equal.equal((a, b) => (a, b) match {
        case (Meta(l, _), Meta(r, _)) => eq.equal(l, r)
        case (Meta(_, _), _)          => false
        case (Map(l),     Map(r))     =>
          listEqual(tuple2Equal(eq, eq)).equal(l, r)
        case (Map(_),     _)          => false
        case (Byte(l),    Byte(r))    => l ≟ r
        case (Byte(_),    _)          => false
        case (Char(l),    Char(r))    => l == r
        case (Char(_),    _)          => false
        case (Int(l),     Int(r))     => l ≟ r
        case (Int(_),     _)          => false
      })
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

  def fromObj[A](f: String => A): Obj[A] => Extension[A] =
    obj => Map(obj.value.toList.map(_.leftMap(f)))
}
