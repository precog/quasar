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

package quasar.std

import slamdata.Predef._
import quasar.{Data, Func, UnaryFunc, BinaryFunc, Type, Mapping, SemanticError},
  SemanticError._
import quasar.fp._
import quasar.fp.ski._
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}

import scala.math.BigDecimal.RoundingMode

import matryoshka._
import scalaz._, Scalaz._, Validation.{success, failure}
import shapeless._

trait MathLib extends Library {
  private val MathRel = Type.Numeric ⨿ Type.Interval
  private val MathAbs = Type.Numeric ⨿ Type.Interval ⨿ Type.Temporal

  // TODO[monocle]: Unit unapply needs to do Boolean instead of Option[Unit]
  // val Zero = Prism.partial[Data, Unit] {
  //   case Data.Number(v) if v ≟ 0 => ()
  // } (κ(Data.Int(0)))
  object Zero {
    def apply() = Data.Int(0)
    def unapply(obj: Data): Boolean = obj match {
      case Data.Number(v) if v ≟ 0 => true
      case _                       => false
    }
  }
  object One {
    def apply() = Data.Int(1)
    def unapply(obj: Data): Boolean = obj match {
      case Data.Number(v) if v ≟ 1 => true
      case _                       => false
    }
  }

  object ZeroF {
    def apply() = Constant(Zero())
    def unapply[A](obj: LP[A]): Boolean = obj match {
      case Constant(Zero()) => true
      case _                 => false
    }
  }
  object OneF {
    def apply() = Constant(One())
    def unapply[A](obj: LP[A]): Boolean = obj match {
      case Constant(One()) => true
      case _                => false
    }
  }

  object TZero {
    def apply() = Type.Const(Zero())
    def unapply(obj: Type): Boolean = obj match {
      case Type.Const(Zero()) => true
      case _                  => false
    }
  }
  object TOne {
    def apply() = Type.Const(One())
    def unapply(obj: Type): Boolean = obj match {
      case Type.Const(One()) => true
      case _                 => false
    }
  }

  private val biReflexiveUnapply: Func.Untyper[nat._2] = partialUntyperV[nat._2] {
    case Type.Const(d) => success(Func.Input2(d.dataType, d.dataType))
    case t             => success(Func.Input2(t, t))
  }

  /** Adds two numeric values, promoting to decimal if either operand is
    * decimal.
    */
  val Add = BinaryFunc(
    Mapping,
    "Adds two numeric or temporal values",
    MathAbs,
    Func.Input2(MathAbs, MathRel),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(x), Embed(ZeroF()))) => x.some
          case Invoke(_, Sized(Embed(ZeroF()), Embed(x))) => x.some
          case _                                           => None
        }
    },
    (partialTyper[nat._2] {
      case Sized(Type.Const(Data.Int(v1)), Type.Const(Data.Int(v2)))             => Type.Const(Data.Int(v1 + v2))
      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Number(v2)))       => Type.Const(Data.Dec(v1 + v2))
      case Sized(Type.Const(Data.Timestamp(v1)), Type.Const(Data.Interval(v2)))  => Type.Const(Data.Timestamp(v1.plus(v2)))
      case Sized(Type.Timestamp, Type.Interval)                                  => Type.Timestamp
      case Sized(Type.Const(Data.Timestamp(_)), t2) if t2 contains Type.Interval => Type.Timestamp
      case Sized(Type.Timestamp, Type.Interval)  => Type.Timestamp
      case Sized(Type.Date,      Type.Interval)  => Type.Date
      case Sized(Type.Time,      Type.Interval)  => Type.Time
    }) ||| numericWidening,
    untyper[nat._2](t => Type.typecheck(Type.Timestamp ⨿ Type.Interval, t).fold(
      κ(t match {
        case Type.Const(d) => success(Func.Input2(d.dataType,  d.dataType))
        case Type.Int      => success(Func.Input2(Type.Int, Type.Int))
        case _             => success(Func.Input2(Type.Numeric, Type.Numeric))
      }),
      κ(success(Func.Input2(t, Type.Interval))))))

  /**
   * Multiplies two numeric values, promoting to decimal if either operand is decimal.
   */
  val Multiply = BinaryFunc(
    Mapping,
    "Multiplies two numeric values or one interval and one numeric value",
    MathRel,
    Func.Input2(MathRel, Type.Numeric),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(x), Embed(OneF()))) => x.some
          case Invoke(_, Sized(Embed(OneF()), Embed(x))) => x.some
          case _                                          => None
        }
    },
    (partialTyper[nat._2] {
      case Sized(TZero(), _) => TZero()
      case Sized(_, TZero()) => TZero()

      case Sized(Type.Const(Data.Int(v1)), Type.Const(Data.Int(v2)))       => Type.Const(Data.Int(v1 * v2))
      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Number(v2))) => Type.Const(Data.Dec(v1 * v2))

      // TODO: handle interval multiplied by Dec (not provided by threeten). See SD-582.
      case Sized(Type.Const(Data.Interval(v1)), Type.Const(Data.Int(v2))) => Type.Const(Data.Interval(v1.multipliedBy(v2.longValue)))
      case Sized(Type.Const(Data.Int(v1)), Type.Const(Data.Interval(v2))) => Type.Const(Data.Interval(v2.multipliedBy(v1.longValue)))
      case Sized(Type.Const(Data.Interval(v1)), t) if t contains Type.Int => Type.Interval
    }) ||| numericWidening,
    biReflexiveUnapply)

  val Power = BinaryFunc(
    Mapping,
    "Raises the first argument to the power of the second",
    Type.Numeric,
    Func.Input2(Type.Numeric, Type.Numeric),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(x), Embed(OneF()))) => x.some
          case _                                         => None
        }
    },
    (partialTyper[nat._2] {
      case Sized(_, TZero()) => TOne()
      case Sized(v1, TOne()) => v1
      case Sized(TZero(), _) => TZero()

      case Sized(Type.Const(Data.Int(v1)), Type.Const(Data.Int(v2))) if v2.isValidInt    => Type.Const(Data.Int(v1.pow(v2.toInt)))
      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Int(v2))) if v2.isValidInt => Type.Const(Data.Dec(v1.pow(v2.toInt)))
    }) ||| numericWidening,
    biReflexiveUnapply)

  /** Subtracts one value from another, promoting to decimal if either operand
    * is decimal.
    */
  val Subtract = BinaryFunc(
    Mapping,
    "Subtracts two numeric or temporal values",
    MathAbs,
    Func.Input2(MathAbs, MathAbs),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(x), Embed(ZeroF()))) => x.some
          case Invoke(_, Sized(Embed(ZeroF()), x))        => Negate(x).some
          case _                                           => None
        }
    },
    (partialTyper[nat._2] {
      case Sized(v1, TZero()) => v1

      case Sized(Type.Const(Data.Int(v1)), Type.Const(Data.Int(v2)))       => Type.Const(Data.Int(v1 - v2))
      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Number(v2))) => Type.Const(Data.Dec(v1 - v2))

      case Sized(Type.Timestamp, Type.Timestamp) => Type.Interval
      case Sized(Type.Timestamp, Type.Interval)  => Type.Timestamp
      case Sized(Type.Date,      Type.Date)      => Type.Interval
      case Sized(Type.Date,      Type.Interval)  => Type.Date
      case Sized(Type.Time,      Type.Time)      => Type.Interval
      case Sized(Type.Time,      Type.Interval)  => Type.Time
    }) ||| numericWidening,
    untyper[nat._2](t => Type.typecheck(Type.Temporal, t).fold(
      κ(Type.typecheck(Type.Interval, t).fold(
        κ(t match {
          case Type.Const(d) => success(Func.Input2(d.dataType  , d.dataType  ))
          case Type.Int      => success(Func.Input2(Type.Int    , Type.Int    ))
          case _             => success(Func.Input2(Type.Numeric, Type.Numeric))
        }),
        κ(success(Func.Input2(Type.Temporal ⨿ Type.Interval, Type.Temporal ⨿ Type.Interval))))),
      κ(success(Func.Input2(t, Type.Interval))))))

  /**
   * Divides one value by another, promoting to decimal if either operand is decimal.
   */
  val Divide = BinaryFunc(
    Mapping,
    "Divides one numeric or interval value by another (non-zero) numeric value",
    MathRel,
    Func.Input2(MathAbs, MathRel),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(x), Embed(OneF()))) => x.some
          case _                                         => None
        }
    },
    (partialTyperV[nat._2] {
      case Sized(v1, TOne() ) => success(v1)
      case Sized(v1, TZero()) => failure(NonEmptyList(GenericError("Division by zero")))

      case Sized(Type.Const(Data.Int(v1)), Type.Const(Data.Int(v2)))       => success(Type.Const(Data.Dec(BigDecimal(v1) / BigDecimal(v2))))
      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Number(v2))) => success(Type.Const(Data.Dec(v1 / v2)))

      // TODO: handle interval divided by Dec (not provided by threeten). See SD-582.
      case Sized(Type.Const(Data.Interval(v1)), Type.Const(Data.Int(v2))) => success(Type.Const(Data.Interval(v1.dividedBy(v2.longValue))))
      case Sized(Type.Const(Data.Interval(v1)), t) if t contains Type.Int => success(Type.Interval)
      case Sized(Type.Interval, Type.Interval)                            => success(Type.Dec)
    }) ||| numericWidening,
    untyper[nat._2](t => Type.typecheck(Type.Interval, t).fold(
      κ(t match {
        case Type.Const(d) => success(Func.Input2(d.dataType, d.dataType))
        case Type.Int      => success(Func.Input2(Type.Int, Type.Int))
        case _             => success(Func.Input2(MathRel, MathRel))
      }),
      κ(success(Func.Input2((Type.Temporal ⨿ Type.Interval), MathRel))))))

  /**
   * Aka "unary minus".
   */
  val Negate = UnaryFunc(
    Mapping,
    "Reverses the sign of a numeric or interval value",
    MathRel,
    Func.Input1(MathRel),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Int(v)))      => success(Type.Const(Data.Int(-v)))
      case Sized(Type.Const(Data.Dec(v)))      => success(Type.Const(Data.Dec(-v)))
      case Sized(Type.Const(Data.Interval(v))) => success(Type.Const(Data.Interval(v.negated)))

      case Sized(t) if (Type.Numeric ⨿ Type.Interval) contains t => success(t)
    },
    untyper[nat._1] {
      case Type.Const(d) => success(Func.Input1(d.dataType))
      case t             => success(Func.Input1(t))
    })

  val Abs = UnaryFunc(
    Mapping,
    "Returns the absolute value of a numeric or interval value",
    MathRel,
    Func.Input1(MathRel),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Int(v)))      => success(Type.Const(Data.Int(v.abs)))
      case Sized(Type.Const(Data.Dec(v)))      => success(Type.Const(Data.Dec(v.abs)))
      case Sized(Type.Const(Data.Interval(v))) => success(Type.Const(Data.Interval(v.abs)))

      case Sized(t) if (Type.Numeric ⨿ Type.Interval) contains t => success(t)
    },
    untyper[nat._1] {
      case Type.Const(d) => success(Func.Input1(d.dataType))
      case t             => success(Func.Input1(t))
    })

    val Ceil = UnaryFunc(
      Mapping,
      "Returns the nearest integer greater than or equal to a numeric value",
      Type.Numeric,
      Func.Input1(Type.Numeric),
      noSimplification,
      partialTyperV[nat._1] {
        case Sized(Type.Const(Data.Int(v)))      => success(Type.Const(Data.Int(v)))
        case Sized(Type.Const(Data.Dec(v)))      => success(Type.Const(Data.Dec(v.setScale(0, RoundingMode.CEILING))))
        case Sized(t) if Type.Numeric contains t => success(t)
      },
      untyper[nat._1] {
        case Type.Const(d) => success(Func.Input1(d.dataType))
        case t             => success(Func.Input1(t))
      })

    val Floor = UnaryFunc(
      Mapping,
      "Returns the nearest integer less than or equal to a numeric value",
      Type.Numeric,
      Func.Input1(Type.Numeric),
      noSimplification,
      partialTyperV[nat._1] {
        case Sized(Type.Const(Data.Int(v)))      => success(Type.Const(Data.Int(v)))
        case Sized(Type.Const(Data.Dec(v)))      => success(Type.Const(Data.Dec(v.setScale(0, RoundingMode.FLOOR))))
        case Sized(t) if Type.Numeric contains t => success(t)
      },
      untyper[nat._1] {
        case Type.Const(d) => success(Func.Input1(d.dataType))
        case t             => success(Func.Input1(t))
      })

    val Trunc = UnaryFunc(
      Mapping,
      "Truncates a numeric value towards zero",
      Type.Numeric,
      Func.Input1(Type.Numeric),
      noSimplification,
      partialTyperV[nat._1] {
        case Sized(Type.Const(Data.Int(v)))      => success(Type.Const(Data.Int(v)))
        case Sized(Type.Const(Data.Dec(v)))      => success(Type.Const(Data.Int(v.toBigInt)))
        case Sized(t) if Type.Numeric contains t => success(t)
      },
      untyper[nat._1] {
        case Type.Const(d) => success(Func.Input1(d.dataType))
        case t             => success(Func.Input1(t))
      })

  // Note: there are 2 interpretations of `%` which return different values for negative numbers.
  // Depending on the interpretation `-5.5 % 1` can either be `-0.5` or `0.5`.
  // Generally, the first interpretation seems to be referred to as "remainder" and the 2nd as "modulo".
  // Java/scala and PostgreSQL all use the remainder interpretation, so we use it here too.
  // However, since PostgreSQL uses the function name `mod` as an alias for `%` while using the term
  // remainder in its description we keep the term `Modulo` around.
  val Modulo = BinaryFunc(
    Mapping,
    "Finds the remainder of one number divided by another",
    MathRel,
    Func.Input2(MathRel, Type.Numeric),
    noSimplification,
    (partialTyperV[nat._2] {
      case Sized(v1, TZero()) => failure(NonEmptyList(GenericError("Division by zero")))

      case Sized(v1 @ Type.Const(Data.Int(_)), TOne())                     => success(TZero())
      case Sized(Type.Const(Data.Int(v1)), Type.Const(Data.Int(v2)))       => success(Type.Const(Data.Int(v1 % v2)))
      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Number(v2))) => success(Type.Const(Data.Dec(v1 % v2)))
    }) ||| numericWidening,
    biReflexiveUnapply)
}

object MathLib extends MathLib
