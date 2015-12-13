package quasar.fp.numeric

import quasar.Predef._

import scala.Numeric

import org.scalacheck.Gen.Choose
import org.scalacheck.{Arbitrary, Gen}

import shapeless.ops.nat.ToInt
import shapeless.Nat

import eu.timepit.refined.api.RefType
import eu.timepit.refined.numeric.{Greater, Less}
import eu.timepit.refined.boolean.Not

import eu.timepit.refined.scalacheck.arbitraryRefType

package object scalacheck {



  // It is very annoying that Numeric does not have a MaxValue function
//  implicit def greaterArbitrary[F[_, _], T:Choose, N <: Nat](
//     implicit
//     rt: RefType[F],
//     num: Numeric[T]
//  ): Arbitrary[F[T, Greater[N]]] = {
//    val gen = Gen.chooseNum(num.plus(num.fromInt(Nat.toInt[N]), num.one), num.MaxValue)
//    arbitraryRefType(gen)
//  }

  def greaterArbitraryRange[F[_,_], T:Choose, N <: Nat](min: F[T,Greater[N]], max: F[T,Greater[N]])
                                                  (implicit rt: RefType[F], num: Numeric[T]): Arbitrary[F[T,Greater[N]]] = {
    val gen = Gen.chooseNum(num.plus(rt.unwrap(min), num.one), rt.unwrap(max))
    arbitraryRefType(gen)
  }

  def greaterArbitraryMax[F[_,_], T:Choose, N <: Nat:ToInt](max: F[T,Greater[N]])
          (implicit rt: RefType[F], num: Numeric[T]): Arbitrary[F[T,Greater[N]]] = {
    val gen = Gen.chooseNum(num.plus(num.fromInt(Nat.toInt[N]), num.one), rt.unwrap(max))
    arbitraryRefType(gen)
  }

  implicit def greaterArbitraryInt[F[_, _], N <: Nat](implicit rt: RefType[F], toInt:ToInt[N]): Arbitrary[F[Int, Greater[N]]] = {
    val gen = Gen.chooseNum(Nat.toInt[N] + 1, Int.MaxValue)
    arbitraryRefType(gen)
  }

  implicit def greaterArbitraryLong[F[_, _], N <: Nat](implicit rt: RefType[F], toInt:ToInt[N]): Arbitrary[F[Long, Greater[N]]] = {
    val gen = Gen.chooseNum(Nat.toInt[N].toLong + 1, Long.MaxValue)
    arbitraryRefType(gen)
  }
//
  implicit def greaterArbitraryShort[F[_, _], N <: Nat](implicit rt: RefType[F], toInt:ToInt[N]): Arbitrary[F[Short, Greater[N]]] = {
    val num = implicitly[Numeric[Short]]
    val gen = Gen.chooseNum(num.plus(Nat.toInt[N].toShort, num.one), Short.MaxValue)
    arbitraryRefType(gen)
  }

  implicit def lessArbitraryLong[F[_, _]:RefType, N <: Nat:ToInt]: Arbitrary[F[Long, Less[N]]] = {
    val gen = Gen.chooseNum(Long.MinValue, Nat.toInt[N].toLong - 1)
    arbitraryRefType(gen)
  }

  implicit def notLessArbitraryLong[F[_,_]:RefType,N <: Nat:ToInt]: Arbitrary[F[Long,Not[Less[N]]]] = {
    val gen = Gen.chooseNum(Nat.toInt[N].toLong,Long.MaxValue)
    arbitraryRefType(gen)
  }

  implicit def notLessArbitraryInt[F[_,_]:RefType,N <: Nat:ToInt]: Arbitrary[F[Int,Not[Less[N]]]] = {
    val gen = Gen.chooseNum(Nat.toInt[N], Int.MaxValue)
    arbitraryRefType(gen)
  }

  def notLessArbitraryRange[F[_,_], T:Choose:Numeric,N <: Nat:ToInt]
  (min: F[T,Not[Less[N]]], max: F[T,Not[Less[N]]])(implicit rt: RefType[F]):Arbitrary[F[T,Not[Less[N]]]] = {
    val gen = Gen.chooseNum(rt.unwrap(min), rt.unwrap(max))
    arbitraryRefType(gen)
  }

  def notLessArbitraryMax[F[_,_], T:Choose,N <: Nat:ToInt]
  (max: F[T,Not[Less[N]]])(implicit rt: RefType[F], num: Numeric[T]):Arbitrary[F[T,Not[Less[N]]]] = {
    val gen = Gen.chooseNum(num.fromInt(Nat.toInt[N]), rt.unwrap(max))
    arbitraryRefType(gen)
  }
}