package quasar.fp.numeric

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

  // TODO: Consider contributing back to refined

  implicit def greaterArbitraryNat[F[_, _], T:Choose, N <: Nat:ToInt](
     implicit
     rt: RefType[F],
     num: Numeric[T],
     bound: Bounded[T]
  ): Arbitrary[F[T, Greater[N]]] = {
    val gen = Gen.chooseNum(num.plus(num.fromInt(Nat.toInt[N]), num.one), bound.maxValue)
    arbitraryRefType(gen)
  }

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

  implicit def lessArbitraryNat[F[_,_], T:Choose, N <: Nat:ToInt](
    implicit
    rt: RefType[F],
    num: Numeric[T],
    bound: Bounded[T]
  ): Arbitrary[F[T,Less[N]]] = {
    val gen = Gen.chooseNum(bound.minValue,num.minus(num.fromInt(Nat.toInt[N]), num.one))
    arbitraryRefType(gen)
  }

  implicit def notLessArbitraryNat[F[_,_], T:Choose, N <: Nat:ToInt](
     implicit
     rt: RefType[F],
     num: Numeric[T],
     bound: Bounded[T]
   ): Arbitrary[F[T,Not[Less[N]]]] = {
    val gen = Gen.chooseNum(num.fromInt(Nat.toInt[N]),bound.maxValue)
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