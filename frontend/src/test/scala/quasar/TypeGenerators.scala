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

import slamdata.Predef.{Set => _, _}
import quasar.time.{DateTimeInterval, OffsetDate}
import quasar.time.DateGenerators._

import java.time._
import scala.Predef.implicitly

import org.scalacheck._
import org.scalacheck.Gen._

import scalaz.ImmutableArray
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz.syntax.traverse._
import scalaz.std.list._
import scalaz.std.option._

trait TypeGenerators {
  implicit def arbitraryType: Arbitrary[Type] = Arbitrary { Gen.sized(depth => typeGen(depth/25)) }

  def arbitrarySimpleType = Arbitrary { Gen.sized(depth => complexGen(depth/25, simpleGen)) }

  def arbitraryTerminal = Arbitrary { terminalGen }

  def arbitraryConst = Arbitrary { constGen }

  def arbitraryNonnestedType = Arbitrary { Gen.oneOf(Gen.const(Type.Top), Gen.const(Type.Bottom), simpleGen) }

  def arbitrarySubtype(superType: Type) = Arbitrary {
    Arbitrary.arbitrary[Type].suchThat(superType.contains(_))
  }

  /** `arbitrarySubtype` is more general, but throws away too many cases to
    * succeed. This version uses `suchThat` in a much more restricted context.
    */
  val arbitraryNumeric = Arbitrary {
    Gen.oneOf(
      Gen.const(Type.Dec),
      Gen.const(Type.Int),
      constGen.suchThat(Type.Numeric.contains(_)))
  }

  def typeGen(depth: Int): Gen[Type] = {
    // NB: never nests Top or Bottom inside any complex type, because that's mostly nonsensical.
    val gens = Gen.oneOf(Type.Top, Type.Bottom) :: List(terminalGen, constGen, objectGen, arrayGen).map(complexGen(depth, _))

    Gen.oneOf(gens(0), gens(1), gens.drop(2): _*)
  }

  def genSubtypesWithConst(superType: Type, depth: Int): Gen[Type] = {
    Gen.oneOf(Gen.const(superType),
      superType match {
        case Type.Top             => typeGen(depth).filter(x => x != Type.Bottom && x != Type.Const(Data.NA))
        case Type.Bottom          => Gen.const(Type.Bottom)
        case cty@Type.Const(_)    => Gen.const(cty)
        case Type.Null            => Gen.oneOf(Gen.const(Type.Null), Gen.const(Type.Const(Data.Null)))
        case Type.Str             => Gen.oneOf(Gen.const(Type.Str), Arbitrary.arbString.arbitrary.map(s => Type.Const(Data.Str(s))))
        case Type.Int             => Gen.oneOf(Gen.const(Type.Int), Arbitrary.arbBigInt.arbitrary.map(i => Type.Const(Data.Int(i))))
        case Type.Dec             => Gen.oneOf(Gen.const(Type.Dec), Arbitrary.arbBigDecimal.arbitrary.map(d => Type.Const(Data.Dec(d))))
        case Type.Bool            => Gen.oneOf(Gen.const(Type.Bool), Arbitrary.arbBool.arbitrary.map(b => Type.Const(Data.Bool(b))))
        case Type.Binary          => Gen.oneOf(Gen.const(Type.Binary), implicitly[Arbitrary[Array[Byte]]].arbitrary.map(a => Type.Const(Data.Binary(ImmutableArray.fromArray(a)))))
        case Type.LocalDateTime   => Gen.oneOf(Gen.const(Type.LocalDateTime), implicitly[Arbitrary[LocalDateTime]].arbitrary.map(i => Type.Const(Data.LocalDateTime(i))))
        case Type.LocalDate       => Gen.oneOf(Gen.const(Type.LocalDate), implicitly[Arbitrary[LocalDate]].arbitrary.map(i => Type.Const(Data.LocalDate(i))))
        case Type.LocalTime       => Gen.oneOf(Gen.const(Type.LocalTime), implicitly[Arbitrary[LocalTime]].arbitrary.map(i => Type.Const(Data.LocalTime(i))))
        case Type.OffsetDateTime  => Gen.oneOf(Gen.const(Type.OffsetDateTime), implicitly[Arbitrary[OffsetDateTime]].arbitrary.map(i => Type.Const(Data.OffsetDateTime(i))))
        case Type.OffsetDate      => Gen.oneOf(Gen.const(Type.OffsetDate), implicitly[Arbitrary[OffsetDate]].arbitrary.map(i => Type.Const(Data.OffsetDate(i))))
        case Type.OffsetTime      => Gen.oneOf(Gen.const(Type.OffsetTime), implicitly[Arbitrary[OffsetTime]].arbitrary.map(i => Type.Const(Data.OffsetTime(i))))
        case Type.Interval        => Gen.oneOf(Gen.const(Type.Interval), implicitly[Arbitrary[DateTimeInterval]].arbitrary.map(i => Type.Const(Data.Interval(i))))
        case Type.Id              => Gen.oneOf(Gen.const(Type.Id), Arbitrary.arbString.arbitrary.map(i => Type.Const(Data.Id(i))))
        case Type.Arr(tys)        => tys.traverse(genSubtypes(_, depth)).map(Type.Arr)
        case Type.Coproduct(x, y) => Gen.oneOf(genSubtypes(x, depth - 1), genSubtypes(y, depth - 1))
        case Type.Obj(map, hk)    =>
          for {
            mapS <- map.toList.traverse { case (k, v) => genSubtypes(v, depth - 1).map(v => (k, v)) }.map(_.toMap)
            hks <- hk.traverse(genSubtypes(_, depth - 1))
          } yield Type.Obj(mapS, hks)
        case Type.FlexArr(min, max, ty) => genSubtypes(ty, depth - 1).map(Type.FlexArr(min, max, _))
      })
  }

  def genSubtypes(superType: Type, depth: Int): Gen[Type] =
    genSubtypesWithConst(superType, depth).map(_.widenConst)

  def complexGen(depth: Int, gen: Gen[Type]): Gen[Type] =
    if (depth > 1) coproductGen(depth, gen)
    else gen

  def coproductGen(depth: Int, gen: Gen[Type]): Gen[Type] = for {
    left <- complexGen(depth-1, gen)
    right <- complexGen(depth-1, gen)
  } yield left ⨿ right

  def simpleGen: Gen[Type] = Gen.oneOf(terminalGen, simpleConstGen)

  def terminalGen: Gen[Type] = Gen.oneOf(
    Type.Null, Type.Str, Type.Int, Type.Dec, Type.Bool, Type.Binary,
    Type.OffsetDateTime, Type.OffsetDate, Type.OffsetTime,
    Type.LocalDateTime, Type.LocalDate, Type.LocalTime, Type.Interval)

  def simpleConstGen: Gen[Type] = DataGenerators.simpleData.map(Type.Const(_))
  def constGen: Gen[Type] = DataGenerators.data.map(Type.Const(_))

  def fieldGen: Gen[(String, Type)] = for {
    c <- Gen.alphaChar
    t <- Gen.oneOf(terminalGen, constGen)
  } yield (c.toString(), t)

  def objectGen: Gen[Type] = for {
    t <- Gen.listOf(fieldGen)
    u <- Gen.oneOf[Option[Type]](None, Gen.oneOf(terminalGen, constGen).map(Some(_)))
  } yield Type.Obj(t.toMap, u)

  def arrGen: Gen[Type] = for {
    t <- Gen.listOf(Gen.oneOf(terminalGen, constGen))
  } yield Type.Arr(t)

  def flexArrayGen: Gen[Type] = for {
    i <- Gen.chooseNum(0, 10)
    n <- Gen.oneOf[Option[Int]](None, Gen.chooseNum(i, 20).map(Some(_)))
    t <- Gen.oneOf(terminalGen, constGen)
  } yield Type.FlexArr(i, n, t)

  def arrayGen: Gen[Type] = Gen.oneOf(arrGen, flexArrayGen)
}

object TypeGenerators extends TypeGenerators
