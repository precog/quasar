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

package quasar.std

import slamdata.Predef._
import quasar._
import quasar.fp.ski.κ

import scala.Predef.locally

import org.scalacheck._
import org.scalacheck.Arbitrary
import org.specs2.execute.Result
import org.specs2.specification.core.Fragment
import scalaz.syntax.traverse._
import scalaz.std.list._
import shapeless.{Nat, Sized}

class StdLibTypeCoherenceSpec extends quasar.Qspec with quasar.TypeArbitrary {
  def testInputCoherence[N <: Nat](func: GenericFunc[N])(input: Func.Input[Type, N]): Result = {
    func.tpe(input).toEither.right.map { o =>
      val unconst = o match {
        case Type.Const(d) => d.dataType
        case _ => o
      }
      if (o == Type.Const(Data.NA)) {
        // typers cannot produce Data.NA without causing bugs for now because of the simplifier not
        // folding array indices and object keys with values of undefined
        failure
      } else if (unconst == Type.Bottom) {
        // untypers do not need to deal with `Type.Bottom` as an expected type,
        // nor constant types
        success
      } else {
        val iv = func.untpe(unconst)

        iv.toEither match {
          case Left(_) =>
            failure
          case Right(is) =>
            input.zip(is.unsized).foldMap { case (exp, res) =>
              Type.typecheck(res, exp).toEither.fold[Result](κ(failure), κ(success))
            }
        }
      }
    }.right.getOrElse(success)
  }

  def testOutputCoherence[N <: Nat](func: GenericFunc[N])(output: Type): Result = {
    func.untpe(output).toEither.right.map { i =>
      val ov = func.tpe(i)
      ov.toEither match {
        case Left(_) => failure
        case Right(oresult) =>
          if (oresult == Type.Const(Data.NA)) {
            // typers cannot produce Data.NA without causing bugs for now because of the simplifier not
            // folding array indices and object keys with values of undefined
            failure
          } else {
            Type.typecheck(oresult, output).toEither.fold[Result](_ => failure, _ => success)
          }
      }
    }.right.getOrElse(success)
  }

  def testCoherence[N <: Nat](name: String, func: GenericFunc[N]): Fragment =
    s"$name coherence" >> {
      locally {
        implicit val arbitraryTypes: Arbitrary[Func.Domain[N]] =
          Arbitrary(func.domain.unsized.traverse(k => Gen.sized(s => TypeArbitrary.genSubtypesWithConst(k, s/25))).map(Sized.wrap[List[Type], N]))
        "input coherence" in Prop.forAll { (is: Func.Domain[N]) =>
          testInputCoherence[N](func)(is)
        }
      }
      locally {
        implicit val arbitraryType: Arbitrary[Type] =
          Arbitrary(Gen.sized(s => TypeArbitrary.genSubtypes(func.codomain, s/25)))
        "output coherence" in Prop.forAll { (t: Type) =>
          testOutputCoherence[N](func)(t)
        }
      }
    }

  "AggLib" >> {
    testCoherence("Count", AggLib.Count)

    testCoherence("Sum", AggLib.Sum)

    testCoherence("Min", AggLib.Min)

    testCoherence("Max", AggLib.Max)

    testCoherence("First", AggLib.First)

    testCoherence("Last", AggLib.Last)

    testCoherence("Avg", AggLib.Avg)

    testCoherence("Arbitrary", AggLib.Arbitrary)
  }

  "ArrayLib" >> {
    testCoherence("ArrayLength", ArrayLib.ArrayLength)
  }

  "IdentityLib" >> {
    testCoherence("Squash", IdentityLib.Squash)

    testCoherence("ToId", IdentityLib.ToId)

    testCoherence("TypeOf", IdentityLib.TypeOf)
  }

  "MathLib" >> {
    testCoherence("Divide", MathLib.Divide)

    testCoherence("Add", MathLib.Add)

    testCoherence("Multiply", MathLib.Multiply)

    testCoherence("Power", MathLib.Power)

    testCoherence("Subtract", MathLib.Subtract)

    testCoherence("Divide", MathLib.Divide)

    testCoherence("Negate", MathLib.Negate)

    testCoherence("Abs", MathLib.Abs)

    testCoherence("Ceil", MathLib.Ceil)

    testCoherence("Floor", MathLib.Floor)

    testCoherence("Trunc", MathLib.Trunc)

    testCoherence("Round", MathLib.Round)

    testCoherence("FloorScale", MathLib.FloorScale)

    testCoherence("CeilScale", MathLib.CeilScale)

    testCoherence("RoundScale", MathLib.RoundScale)

    testCoherence("Modulo", MathLib.Modulo)
  }

  "RelationsLib" >> {
    testCoherence("Eq", RelationsLib.Eq)

    testCoherence("Neq", RelationsLib.Neq)

    testCoherence("Lt", RelationsLib.Lt)

    testCoherence("Lte", RelationsLib.Lte)

    testCoherence("Gt", RelationsLib.Gt)

    testCoherence("Gte", RelationsLib.Gte)

    testCoherence("Between", RelationsLib.Between)

    testCoherence("IfUndefined", RelationsLib.IfUndefined)

    testCoherence("And", RelationsLib.And)

    testCoherence("Or", RelationsLib.Or)

    testCoherence("Not", RelationsLib.Not)

    testCoherence("Cond", RelationsLib.Cond)
  }

  "SetLib" >> {
    testCoherence("Take", SetLib.Take)

    testCoherence("Sample", SetLib.Sample)

    testCoherence("Drop", SetLib.Drop)

    testCoherence("Filter", SetLib.Filter)

    testCoherence("GroupBy", SetLib.GroupBy)

    testCoherence("Union", SetLib.Union)

    testCoherence("Intersect", SetLib.Intersect)

    testCoherence("Except", SetLib.Except)

    testCoherence("In", SetLib.In)

    testCoherence("Within", SetLib.Within)

    testCoherence("Constantly", SetLib.Constantly)
  }

  "StringLib" >> {
    testCoherence("Concat", StringLib.Concat)

    testCoherence("Like", StringLib.Like)

    testCoherence("Search", StringLib.Search)

    testCoherence("Length", StringLib.Length)

    testCoherence("Lower", StringLib.Lower)

    testCoherence("Upper", StringLib.Upper)

    testCoherence("Substring", StringLib.Substring)

    testCoherence("Split", StringLib.Split)

    testCoherence("Boolean", StringLib.Boolean)

    testCoherence("Integer", StringLib.Integer)

    testCoherence("Decimal", StringLib.Decimal)

    testCoherence("Null", StringLib.Null)

    testCoherence("ToString", StringLib.ToString)
  }

  "DateLib" >> {
    testCoherence("ExtractCentury", DateLib.ExtractCentury)
    testCoherence("ExtractDayOfMonth", DateLib.ExtractDayOfMonth)
    testCoherence("ExtractDecade", DateLib.ExtractDecade)
    testCoherence("ExtractDayOfWeek", DateLib.ExtractDayOfWeek)
    testCoherence("ExtractDayOfYear", DateLib.ExtractDayOfYear)
    testCoherence("ExtractEpoch", DateLib.ExtractEpoch)
    testCoherence("ExtractHour", DateLib.ExtractHour)
    testCoherence("ExtractIsoDayOfWeek", DateLib.ExtractIsoDayOfWeek)
    testCoherence("ExtractIsoYear", DateLib.ExtractIsoYear)
    testCoherence("ExtractMicroseconds", DateLib.ExtractMicroseconds)
    testCoherence("ExtractMillennium", DateLib.ExtractMillennium)
    testCoherence("ExtractMilliseconds", DateLib.ExtractMilliseconds)
    testCoherence("ExtractMinute", DateLib.ExtractMinute)
    testCoherence("ExtractMonth", DateLib.ExtractMonth)
    testCoherence("ExtractQuarter", DateLib.ExtractQuarter)
    testCoherence("ExtractSecond", DateLib.ExtractSecond)
    testCoherence("ExtractTimezone", DateLib.ExtractTimezone)
    testCoherence("ExtractTimezoneHour", DateLib.ExtractTimezoneHour)
    testCoherence("ExtractTimezoneMinute", DateLib.ExtractTimezoneMinute)
    testCoherence("ExtractWeek", DateLib.ExtractWeek)
    testCoherence("ExtractYear", DateLib.ExtractYear)

    testCoherence("Date", DateLib.Date)

    testCoherence("Now", DateLib.Now)

    testCoherence("Time", DateLib.Time)

    testCoherence("Timestamp", DateLib.Timestamp)

    testCoherence("Interval", DateLib.Interval)

    testCoherence("StartOfDay", DateLib.StartOfDay)

    testCoherence("TimeOfDay", DateLib.TimeOfDay)

    testCoherence("ToTimestamp", DateLib.ToTimestamp)
  }
}
