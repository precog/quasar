/*
 * Copyright 2014 - 2015 SlamData Inc.
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

import scala.collection.{immutable => I}
import scala.{Predef => P, collection => C}
import scala.inline

object Predef extends LowPriorityImplicits with SKI {
  type tailrec = scala.annotation.tailrec
  type SuppressWarnings = java.lang.SuppressWarnings

  type Array[T] = scala.Array[T]
  val  Array = scala.Array
  type Boolean = scala.Boolean
  type Byte = scala.Byte
  type Char = scala.Char
  type Double = scala.Double
  val  Function = scala.Function
  type Int = scala.Int
  val  Int = scala.Int
  type Long = scala.Long
  val  Long = scala.Long
  type PartialFunction[-A, +B] = scala.PartialFunction[A, B]
  val  PartialFunction         = scala.PartialFunction
  type String = P.String
  val  StringContext = scala.StringContext
  type Symbol = scala.Symbol
  val  Symbol  = scala.Symbol
  type Unit = scala.Unit
  type Vector[+A] = scala.Vector[A]
  val  Vector     = scala.Vector

  type BigDecimal = scala.math.BigDecimal
  val  BigDecimal = scala.math.BigDecimal
  type BigInt = scala.math.BigInt
  val  BigInt = scala.math.BigInt

  type Iterable[+A] = C.Iterable[A]
  type IndexedSeq[+A] = C.IndexedSeq[A]

  type ListMap[A, +B] = I.ListMap[A, B]
  val  ListMap        = I.ListMap
  type Map[A, +B] = I.Map[A, B]
  val  Map        = I.Map
  type Set[A] = I.Set[A]
  val  Set    = I.Set
  type Seq[+A] = I.Seq[A]
  type Stream[+A] = I.Stream[A]
  val  Stream     = I.Stream
  val  #::        = Stream.#::

  // NB: not using scala.Predef.??? or scala.NotImplementedError because specs2
  // intercepts the result in a useless way.
  def ??? : Nothing = throw new java.lang.RuntimeException("not implemented")

  def implicitly[T](implicit e: T) = P.implicitly[T](e)

  implicit def $conforms[A] = P.$conforms[A]
  implicit def ArrowAssoc[A] = P.ArrowAssoc[A] _
  implicit def augmentString(x: String) = P.augmentString(x)
  implicit def genericArrayOps[T] = P.genericArrayOps[T] _
  implicit val wrapString = P.wrapString _
  implicit val unwrapString = P.unwrapString _
  @inline implicit val booleanWrapper = P.booleanWrapper _
  @inline implicit val charWrapper = P.charWrapper _
  @inline implicit val intWrapper = P.intWrapper _

  // would rather not have these, but …
  def print(x: scala.Any)   = scala.Console.print(x)
  def println()             = scala.Console.println()
  def println(x: scala.Any) = scala.Console.println(x)

  // remove or replace these
  type List[+A] = I.List[A] // use scalaz.IList instead
  val  List     = I.List
  val  Nil      = I.Nil
  val  ::       = I.::
  type Option[A] = scala.Option[A] // use scalaz.Maybe instead
  val  Option    = scala.Option
  val  None      = scala.None
  val  Some      = scala.Some
  type Nothing = scala.Nothing // make functors invariant
  type Throwable = java.lang.Throwable
  type RuntimeException = java.lang.RuntimeException

  /** Accept a value (forcing the argument expression to be evaluated for its
    * effects), and then discard it, returning Unit. Makes it explicit that
    * you're discarding the result, and effectively suppresses the
    * "NonUnitStatement" warning from wartremover.
    */
  def ignore[A](a: A): Unit = ()
}

abstract class LowPriorityImplicits {
  implicit def genericWrapArray[T] = P.genericWrapArray[T] _
}

trait SKI {
  // NB: Unicode has double-struck and bold versions of the letters, which might
  //     be more appropriate, but the code points are larger than 2 bytes, so
  //     Scala doesn't handle them.

  /** Probably not useful; implemented here mostly because it's amusing. */
  def σ[A, B, C](x: A => B => C, y: A => B, z: A): C = x(z)(y(z))

  /**
   A shorter name for the constant function of 1, 2, 3, or 6 args.
   NB: the argument is eager here, so use `_ => ...` instead if you need it to be thunked.
   */
  def κ[A, B](x: B): A => B                                 = _ => x
  def κ[A, B, C](x: C): (A, B) => C                         = (_, _) => x
  def κ[A, B, C, D](x: D): (A, B, C) => D                   = (_, _, _) => x
  def κ[A, B, C, D, E, F, G](x: G): (A, B, C, D, E, F) => G = (_, _, _, _, _, _) => x

  /** A shorter name for the identity function. */
  def ι[A]: A => A = x => x
}
object SKI extends SKI
