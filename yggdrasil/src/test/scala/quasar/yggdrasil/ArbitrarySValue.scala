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

package quasar.yggdrasil

import quasar.blueeyes._
import quasar.precog.common._
import quasar.blueeyes.json._
import quasar.RCValueGenerators
import scalaz._, Scalaz._
import quasar.precog.TestSupport._, Gen._

object SJValueGenerators {
  type JSchema = Seq[(JPath, CType)]

  def inferSchema(data: Seq[JValue]): JSchema = {
    if (data.isEmpty) {
      Seq.empty
    } else {
      val current = data.head.flattenWithPath flatMap {
        case (path, jv) =>
          CType.forJValue(jv) map { ct => (path, ct) }
      }

      (current ++ inferSchema(data.tail)).distinct
    }
  }
}

trait SJValueGenerators extends ArbitraryBigDecimal with RCValueGenerators {
  import SJValueGenerators._

  def schema(depth: Int): Gen[JSchema] = {
    if (depth <= 0) leafSchema
    else oneOf(1, 2, 3) flatMap {
      case 1 => objectSchema(depth, choose(1, 3))
      case 2 => arraySchema(depth, choose(1, 5))
      case 3 => leafSchema
    }
  }

  def objectSchema(depth: Int, sizeGen: Gen[Int]): Gen[JSchema] = {
    for {
      size <- sizeGen
      names <- containerOfN[Set, String](size, identifier)
      subschemas <- listOfN(size, schema(depth - 1))
    } yield {
      for {
        (name, subschema) <- names.toList zip subschemas
        (jpath, ctype)    <- subschema
      } yield {
        (JPathField(name) \ jpath, ctype)
      }
    }
  }

  def arraySchema(depth: Int, sizeGen: Gen[Int]): Gen[JSchema] = {
    for {
      size <- sizeGen
      subschemas <- listOfN(size, schema(depth - 1))
    } yield {
      for {
        (idx, subschema) <- (0 until size) zip subschemas
        (jpath, ctype)   <- subschema
      } yield {
        (JPathIndex(idx) \ jpath, ctype)
      }
    }
  }

  def leafSchema: Gen[JSchema] = ctype map { t => (NoJPath -> t) :: Nil }

  def ctype: Gen[CType] = oneOf(
    CString,
    CBoolean,
    CLong,
    CDouble,
    CNum,
    CNull,
    CEmptyObject,
    CEmptyArray,
    CInterval,
    COffsetDateTime,
    COffsetTime,
    COffsetDate,
    CLocalDateTime,
    CLocalTime,
    CLocalDate
  )

  def jvalue(ctype: CType): Gen[JValue] = genCValueForType(ctype).map(_.toJValue)

  def jvalue(schema: Seq[(JPath, CType)]): Gen[JValue] = {
    schema.foldLeft(Gen.const[JValue](JUndefined)) {
      case (gen, (jpath, ctype)) =>
        for {
          acc <- gen
          jv  <- jvalue(ctype)
        } yield {
          acc.unsafeInsert(jpath, jv)
        }
    }
  }

  def genEventColumns(jschema: JSchema): Gen[(Int, Stream[(Identities, Seq[(JPath, JValue)])])] =
    for {
      idCount  <- choose(1, 3)
      dataSize <- choose(0, 20)
      ids      <- setOfN[List[Long]](dataSize, listOfN[Long](idCount, genPosLong))
      values   <- listOfN[Seq[(JPath, JValue)]](dataSize, Gen.sequence(jschema map { case (k, v) => jvalue(v) map (k -> _) }))
      falseDepth  <- choose(1, 3)
      falseSchema <- schema(falseDepth)
      falseSize   <- choose(0, 5)
      falseIds    <- setOfN[List[Long]](falseSize, listOfN(idCount, genPosLong))
      falseValues <- listOfN[Seq[(JPath, JValue)]](falseSize, Gen.sequence(falseSchema map { case (k, v) => jvalue(v).map(k -> _) }))

      falseIds2 = falseIds -- ids     // distinct ids
    } yield {
      (idCount, (ids.map(_.toArray) zip values).toStream ++ (falseIds2.map(_.toArray) zip falseValues).toStream)
    }

  def assemble(parts: Seq[(JPath, JValue)]): JValue = {
    val result = parts.foldLeft[JValue](JUndefined) {
      case (acc, (selector, jv)) => acc.unsafeInsert(selector, jv)
    }

    if (result != JUndefined || parts.isEmpty) result else sys.error("Cannot build object from " + parts)
  }
}

trait SValueGenerators extends ArbitraryBigDecimal {
  def svalue(depth: Int): Gen[SValue] = {
    if (depth <= 0) sleaf
    else oneOf(1, 2, 3) flatMap { //it's much faster to lazily compute the subtrees
      case 1 => sobject(depth)
      case 2 => sarray(depth)
      case 3 => sleaf
    }
  }

  def sobject(depth: Int): Gen[SValue] = {
    for {
      size <- choose(0, 3)
      names <- containerOfN[Set, String](size, identifier)
      values <- listOfN(size, svalue(depth - 1))
    } yield {
      SObject((names zip values).toMap)
    }
  }

  def sarray(depth: Int): Gen[SValue] = {
    for {
      size <- choose(0, 3)
      l <- listOfN(size, svalue(depth - 1))
    } yield SArray(Vector(l: _*))
  }


  def sleaf: Gen[SValue] = oneOf[SValue](
    alphaStr map (SString(_: String)),
    arbitrary[Boolean] map (SBoolean(_: Boolean)),
    arbitrary[Long]    map (l => SDecimal(BigDecimal(l))),
    arbitrary[Double]  map (d => SDecimal(BigDecimal(d))),
    arbitrary[BigDecimal] map { bd => SDecimal(bd) }, //scalacheck's BigDecimal gen will overflow at random
    const(SNull)
  )

  def sevent(idCount: Int, vdepth: Int): Gen[SEvent] = {
    for {
      ids <- containerOfN[Set, Long](idCount, posNum[Long])
      value <- svalue(vdepth)
    } yield (ids.toArray, value)
  }

  def chunk(size: Int, idCount: Int, vdepth: Int): Gen[Vector[SEvent]] =
    listOfN(size, sevent(idCount, vdepth)) map { l => Vector(l: _*) }
}

case class LimitList[A](values: List[A])

object LimitList {
  def genLimitList[A: Gen](size: Int): Gen[LimitList[A]] = for {
    i <- choose(0, size)
    l <- listOfN(i, implicitly[Gen[A]])
  } yield LimitList(l)
}

trait ArbitrarySValue extends SValueGenerators {
  def genChunks(size: Int): Gen[LimitList[Vector[SEvent]]] = LimitList.genLimitList[Vector[SEvent]](size)

  implicit val listLongOrder = scalaz.std.list.listOrder[Long]

  implicit val SEventIdentityOrder: scalaz.Order[SEvent] = scalaz.Order[List[Long]].contramap((_: SEvent)._1.toList)
  implicit val SEventOrdering = SEventIdentityOrder.toScalaOrdering

  implicit val SEventChunkGen: Gen[Vector[SEvent]] = chunk(3, 3, 2)
  implicit val ArbitraryChunks = Arbitrary(genChunks(5))
}

trait ArbitraryBigDecimal {
  val MAX_EXPONENT = 50000
  // BigDecimal *isn't* arbitrary precision!  AWESOME!!!
  implicit def arbBigDecimal: Arbitrary[BigDecimal] = Arbitrary(for {
    mantissa <- arbitrary[Long]
    exponent <- Gen.chooseNum(-MAX_EXPONENT, MAX_EXPONENT)

    adjusted = if (exponent.toLong + mantissa.toString.length >= Int.MaxValue.toLong)
      exponent - mantissa.toString.length
    else if (exponent.toLong - mantissa.toString.length <= Int.MinValue.toLong)
      exponent + mantissa.toString.length
    else
      exponent
  } yield BigDecimal(mantissa, adjusted, java.math.MathContext.UNLIMITED))
}
