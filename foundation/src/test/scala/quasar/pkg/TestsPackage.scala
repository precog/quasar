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

package quasar.pkg

import slamdata.Predef._

import scala.Predef.$conforms
import scala.collection.mutable.Builder
import scala.collection.Traversable
import scala.language.postfixOps
import scala.{ Byte, Char }

package object tests extends TestsPackage

trait TestsPackage extends ScalacheckSupport with SpecsSupport

trait SpecsSupport {
  import org.specs2._, matcher._

  type ScalaCheck = org.specs2.ScalaCheck
  val SpecsFailure = org.specs2.execute.Failure
  type MatchResult[+A]    = org.specs2.matcher.MatchResult[A]
  type SpecsFailure       = org.specs2.execute.Failure
  type SpecsResult        = org.specs2.execute.Result
  type Specification      = org.specs2.mutable.Specification
  type SpecificationLike  = org.specs2.mutable.SpecificationLike
  type ThrownExpectations = org.specs2.matcher.ThrownExpectations

  trait SpecificationHelp extends SpecificationLike with ThrownMessages {
    def haveAllElementsLike[A](pf: PartialFunction[A, Boolean]): ContainWithResult[A] = contain(like[A]({ case x if pf.isDefinedAt(x) && pf(x) => ok })).forall
  }
}

trait ScalacheckSupport {
  val Arbitrary = org.scalacheck.Arbitrary
  val Gen       = org.scalacheck.Gen
  val Pretty    = org.scalacheck.util.Pretty
  val Prop      = org.scalacheck.Prop
  val Shrink    = org.scalacheck.Shrink

  type Arbitrary[A]            = org.scalacheck.Arbitrary[A]
  type Buildable[Elem, Result] = org.scalacheck.util.Buildable[Elem, Result]
  type Gen[+A]                 = org.scalacheck.Gen[A]
  type Pretty                  = org.scalacheck.util.Pretty
  type Prop                    = org.scalacheck.Prop
  type Shrink[A]               = org.scalacheck.Shrink[A]

  import Gen.{ listOfN, containerOfN, identifier, sized, oneOf, frequency, alphaNumChar }

  def choose(lo: Int, hi: Int): Gen[Int]    = Gen.choose(lo, hi)
  def choose(lo: Long, hi: Long): Gen[Long] = Gen.choose(lo, hi)
  def const[A](value: A): Gen[A]            = Gen.const(value)

  def arbitrary[A](implicit z: Arbitrary[A]): Gen[A] = z.arbitrary

  def maxSequenceLength = 16

  implicit def liftGenerator[A](g: Gen[A]): Arbitrary[A] = Arbitrary(g)

  implicit def buildableVector[A]: Buildable[A, Vector[A]] = new Buildable[A, Vector[A]] {
    def builder: Builder[A, Vector[A]] = Vector.newBuilder[A]
  }

  def genFile: Gen[jFile] = listOfN(3, identifier map (_ take 5)) map (xs => new jFile(xs.mkString("/", "/", ".cooked")))

  def containerOfAtMostN[C[X] <: Traversable[X], A](maxSize: Int, g: Gen[A])(implicit b: Buildable[A, C[A]]): Gen[C[A]] =
    sized(size => for (n <- choose(0, size min maxSize); c <- containerOfN[C, A](n, g)) yield c)

  def arrayOf[A: CTag](gen: Gen[A]): Gen[Array[A]] = vectorOf(gen) ^^ (_.toArray)
  def vectorOf[A](gen: Gen[A]): Gen[Vector[A]]     = containerOfAtMostN[Vector, A](maxSequenceLength, gen)
  def listOf[A](gen: Gen[A]): Gen[List[A]]         = containerOfAtMostN[List, A](maxSequenceLength, gen)
  def setOf[A](gen: Gen[A]): Gen[Set[A]]           = containerOfAtMostN[Set, A](maxSequenceLength, gen)

  def arrayOfN[A: CTag](len: Int, gen: Gen[A]): Gen[Array[A]] = vectorOfN(len, gen) ^^ (_.toArray)
  def vectorOfN[A](len: Int, gen: Gen[A]): Gen[Vector[A]]     = containerOfN[Vector, A](len, gen)
  def setOfN[A](len: Int, gen: Gen[A]): Gen[Set[A]]           = containerOfN[Set, A](len, gen)

  // !!! Should take an Eq[K].
  def mapOfN[K, V](len: Int, k: Gen[K], v: Gen[V]): Gen[sciMap[K, V]] =
    (setOfN(len, k) -> listOfN(len, v)) >> (_ zip _ toMap)

  def genAlphaNumString: Gen[String]   = alphaNumChar.list ^^ (_.mkString)
  def genBigDecimal: Gen[BigDecimal]   = Arbitrary.arbBigDecimal.arbitrary
  def genBigInt: Gen[BigInt]           = Arbitrary.arbBigInt.arbitrary
  def genBool: Gen[Boolean]            = oneOf(true, false)
  def genByte: Gen[Byte]               = choose(Byte.MinValue, Byte.MaxValue) ^^ (_.toByte)
  def genChar: Gen[Char]               = choose(Char.MinValue, Char.MaxValue) ^^ (_.toChar)
  def genDouble: Gen[Double]           = Arbitrary.arbDouble.arbitrary
  def genIdentifier: Gen[String]       = identifier filter (_ != null)
  def genIndex(size: Int): Gen[Int]    = choose(0, size - 1)
  def genInt: Gen[Int]                 = choose(Int.MinValue, Int.MaxValue)
  def genLong: Gen[Long]               = choose(Long.MinValue, Long.MaxValue)
  def genMinus10To10: Gen[Int]         = choose(-10, 10)
  def genOffsetAndLen: Gen[Int -> Int] = (genMinus10To10, 0 upTo 20).zip
  def genPosInt: Gen[Int]              = choose(1, Int.MaxValue)
  def genPosLong: Gen[Long]            = choose(1L, Long.MaxValue)
  def genStartAndEnd: Gen[Int -> Int]  = genMinus10To10 >> (s => (0 upTo 20) ^^ (e => s -> e))
  def genString: Gen[String]           = Arbitrary.arbString.arbitrary

  implicit class ScalacheckIntOps(private val n: Int) {
    def upTo(end: Int): Gen[Int] = choose(n, end)
  }

  implicit class ArbitraryOps[A](arb: Arbitrary[A]) {
    def gen: Gen[A] = arb.arbitrary

    def ^^[B](f: A => B): Arbitrary[B]      = Arbitrary(gen map f)
    def >>[B](f: A => Gen[B]): Arbitrary[B] = Arbitrary(gen flatMap f)

    def list: Gen[List[A]]  = gen.list
    def opt: Gen[Option[A]] = gen.opt
  }
  implicit class ScalacheckGenOps[A](gen: Gen[A]) {
    def ^^[B](f: A => B): Gen[B]      = gen map f
    def >>[B](f: A => Gen[B]): Gen[B] = gen flatMap f

    def take(n: Int): Vec[A]         = Stream continually gen.sample flatMap (x => x) take n toVector
    def *(gn: Gen[Int]): Gen[Seq[A]] = gn >> (this * _)
    def *(n: Int): Gen[Seq[A]]       = vectorOfN(n, gen)
    def list: Gen[List[A]]           = listOf(gen)
    def opt: Gen[Option[A]]          = frequency(
      1  -> None,
      10 -> (gen map (x => Some(x)))
    )
  }
  implicit class ScalacheckGen2Ops[A, B](gen: (Gen[A], Gen[B])) {
    def >>[C](f: (A, B) => C): Gen[C] = for (a <- gen._1; b <- gen._2) yield f(a, b)
    def zip: Gen[A -> B]              = >>(scala.Tuple2(_, _))
  }
  implicit class ScalacheckGen3Ops[A, B, C](gen: (Gen[A], Gen[B], Gen[C])) {
    def >>[D](f: (A, B, C) => D): Gen[D] = for (a <- gen._1; b <- gen._2; c <- gen._3) yield f(a, b, c)
    def zip: Gen[(A, B, C)]              = >>(scala.Tuple3(_, _, _))
  }
}
