package slamdata.engine

import org.scalacheck._
import Gen._
import org.threeten.bp.{Instant, LocalDate, LocalTime, Duration}

import Type._

trait TypeGen {
  implicit def arbitraryType: Arbitrary[Type] = Arbitrary { Gen.sized(depth => typeGen(depth/25)) }

  def arbitrarySimpleType = Arbitrary { Gen.sized(depth => complexGen(depth/25, simpleGen)) }

  def arbitraryTerminal = Arbitrary { terminalGen }

  def arbitraryConst = Arbitrary { constGen }

  def arbitraryNonnestedType = Arbitrary { Gen.oneOf(Gen.const(Top), Gen.const(Bottom), simpleGen, objectGen, arrayGen) }

  def typeGen(depth: Int): Gen[Type] = {
    // NB: never nests Top or Bottom inside any complex type, because that's mostly nonsensical.
    val gens = Gen.oneOf(Top, Bottom) :: List(terminalGen, constGen, objectGen, arrayGen).map(complexGen(depth, _))

    Gen.oneOf(gens(0), gens(1), gens.drop(2): _*)
  }

  def complexGen(depth: Int, gen: Gen[Type]): Gen[Type] =
    if (depth > 1) Gen.oneOf(productGen(depth, gen), coproductGen(depth, gen))
    else gen

  def productGen(depth: Int, gen: Gen[Type]): Gen[Type] = for {
    left <- complexGen(depth-1, gen)
    right <- complexGen(depth-1, gen)
  } yield left & right

  def coproductGen(depth: Int, gen: Gen[Type]): Gen[Type] = for {
    left <- complexGen(depth-1, gen)
    right <- complexGen(depth-1, gen)
  } yield left | right

  def simpleGen: Gen[Type] = Gen.oneOf(terminalGen, constGen, setGen)

  def terminalGen: Gen[Type] = Gen.oneOf(Null, Str, Int, Dec, Bool, Binary, Timestamp, Date, Time, Interval)

  def constGen: Gen[Type] =
    Gen.oneOf(Const(Data.Null), Const(Data.Str("a")), Const(Data.Int(1)),
              Const(Data.Dec(1.0)), Const(Data.True), Const(Data.Binary(Array(1))),
              Const(Data.Timestamp(Instant.now())),
              Const(Data.Date(LocalDate.now())),
              Const(Data.Time(LocalTime.now())),
              Const(Data.Interval(Duration.ofSeconds(1))))

  // TODO: can a Set contain constants? objects? arrays?
  def setGen: Gen[Type] = for {
    t <- terminalGen
  } yield Set(t)

  def objectGen: Gen[Type] = for {
    c <- Gen.alphaChar
    t <- Gen.oneOf(terminalGen, constGen)
  } yield NamedField(c.toString(), t)
  // TODO: AnonField

  def arrayGen: Gen[Type] = for {
    i <- Gen.chooseNum(0, 10)
    t <- Gen.oneOf(terminalGen, constGen)
  } yield IndexedElem(i, t)
  // TODO: AnonElem
}

object TypeGen extends TypeGen
