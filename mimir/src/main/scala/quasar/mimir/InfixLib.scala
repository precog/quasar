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

package quasar.mimir

import quasar.precog.util.NumericComparisons
import quasar.yggdrasil.bytecode._
import quasar.yggdrasil.table._
import quasar.time.DateTimeInterval

trait InfixLibModule[M[+ _]] extends ColumnarTableLibModule[M] {
  trait InfixLib extends ColumnarTableLib {
    import StdLib.{ BoolFrom, DoubleFrom, LongFrom, NumFrom, StrFrom, doubleIsDefined }

    object Infix {
      val InfixNamespace = Vector("std", "infix")

      final def longOk(x: Long, y: Long)            = true
      final def doubleOk(x: Double, y: Double)      = true
      final def numOk(x: BigDecimal, y: BigDecimal) = true

      final def longNeZero(x: Long, y: Long)            = y != 0
      final def doubleNeZero(x: Double, y: Double)      = y != 0.0
      final def numNeZero(x: BigDecimal, y: BigDecimal) = y != 0

      class InfixOp2(name: String, longf: (Long, Long) => Long, doublef: (Double, Double) => Double, numf: (BigDecimal, BigDecimal) => BigDecimal)
          extends Op2F2(InfixNamespace, name) {
        val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
        def f2: F2 = CF2P("builtin::infix::op2::" + name) {
          case (c1: LongColumn, c2: LongColumn) =>
            new LongFrom.LL(c1, c2, longOk, longf)

          case (c1: LongColumn, c2: DoubleColumn) =>
            new NumFrom.LD(c1, c2, numOk, numf)

          case (c1: LongColumn, c2: NumColumn) =>
            new NumFrom.LN(c1, c2, numOk, numf)

          case (c1: DoubleColumn, c2: LongColumn) =>
            new NumFrom.DL(c1, c2, numOk, numf)

          case (c1: DoubleColumn, c2: DoubleColumn) =>
            new DoubleFrom.DD(c1, c2, doubleOk, doublef)

          case (c1: DoubleColumn, c2: NumColumn) =>
            new NumFrom.DN(c1, c2, numOk, numf)

          case (c1: NumColumn, c2: LongColumn) =>
            new NumFrom.NL(c1, c2, numOk, numf)

          case (c1: NumColumn, c2: DoubleColumn) =>
            new NumFrom.ND(c1, c2, numOk, numf)

          case (c1: NumColumn, c2: NumColumn) =>
            new NumFrom.NN(c1, c2, numOk, numf)
        }
      }

      val Add = new Op2F2(InfixNamespace, "add") {
        val tpe = BinaryOperationType(JType.JRelativeT, JType.JRelativeT, JType.JRelativeT)
        def f2: F2 = CF2P("builtin::infix::op2::add") {
          case (c1: LongColumn, c2: LongColumn) =>
            new LongFrom.LL(c1, c2, longOk, _ + _)

          case (c1: LongColumn, c2: DoubleColumn) =>
            new NumFrom.LD(c1, c2, numOk, _ + _)

          case (c1: LongColumn, c2: NumColumn) =>
            new NumFrom.LN(c1, c2, numOk, _ + _)

          case (c1: DoubleColumn, c2: LongColumn) =>
            new NumFrom.DL(c1, c2, numOk, _ + _)

          case (c1: DoubleColumn, c2: DoubleColumn) =>
            new DoubleFrom.DD(c1, c2, doubleOk, _ + _)

          case (c1: DoubleColumn, c2: NumColumn) =>
            new NumFrom.DN(c1, c2, numOk, _ + _)

          case (c1: NumColumn, c2: LongColumn) =>
            new NumFrom.NL(c1, c2, numOk, _ + _)

          case (c1: NumColumn, c2: DoubleColumn) =>
            new NumFrom.ND(c1, c2, numOk, _ + _)

          case (c1: NumColumn, c2: NumColumn) =>
            new NumFrom.NN(c1, c2, numOk, _ + _)

          case (c1: OffsetDateTimeColumn, c2: IntervalColumn) =>
            new OffsetDateTimeColumn {
              def apply(row: Int) = c2(row).addToOffsetDateTime(c1(row))
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
            }

          case (c1: OffsetDateColumn, c2: IntervalColumn) =>
            new OffsetDateColumn {
              def apply(row: Int) = DateTimeInterval.addToOffsetDate(c1(row), c2(row).period)
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row).isDateLike
            }

          case (c1: OffsetTimeColumn, c2: IntervalColumn) =>
            new OffsetTimeColumn {
              def apply(row: Int) = DateTimeInterval.addToOffsetTime(c1(row), c2(row).duration)
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row).isTimeLike
            }

          case (c1: LocalDateTimeColumn, c2: IntervalColumn) =>
            new LocalDateTimeColumn {
              def apply(row: Int) = c2(row).addToLocalDateTime(c1(row))
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
            }

          case (c1: LocalDateColumn, c2: IntervalColumn) =>
            new LocalDateColumn {
              def apply(row: Int) = DateTimeInterval.addToLocalDate(c1(row), c2(row).period)
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row).isDateLike
            }

          case (c1: LocalTimeColumn, c2: IntervalColumn) =>
            new LocalTimeColumn {
              def apply(row: Int) = DateTimeInterval.addToLocalTime(c1(row), c2(row).duration)
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row).isTimeLike
            }

          case (c1: IntervalColumn, c2: OffsetDateTimeColumn) =>
            new OffsetDateTimeColumn {
              def apply(row: Int) = c1(row).addToOffsetDateTime(c2(row))
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
            }

          case (c1: IntervalColumn, c2: OffsetDateColumn) =>
            new OffsetDateColumn {
              def apply(row: Int) = DateTimeInterval.addToOffsetDate(c2(row), c1(row).period)
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c1(row).isDateLike
            }

          case (c1: IntervalColumn, c2: OffsetTimeColumn) =>
            new OffsetTimeColumn {
              def apply(row: Int) = DateTimeInterval.addToOffsetTime(c2(row), c1(row).duration)
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c1(row).isTimeLike
            }

          case (c1: IntervalColumn, c2: LocalDateTimeColumn) =>
            new LocalDateTimeColumn {
              def apply(row: Int) = c1(row).addToLocalDateTime(c2(row))
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
            }

          case (c1: IntervalColumn, c2: LocalDateColumn) =>
            new LocalDateColumn {
              def apply(row: Int) = DateTimeInterval.addToLocalDate(c2(row), c1(row).period)
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c1(row).isDateLike
            }

          case (c1: IntervalColumn, c2: LocalTimeColumn) =>
            new LocalTimeColumn {
              def apply(row: Int) = DateTimeInterval.addToLocalTime(c2(row), c1(row).duration)
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c1(row).isTimeLike
            }

          case (c1: IntervalColumn, c2: IntervalColumn) =>
            new IntervalColumn {
              def apply(row: Int) = c1(row).plus(c2(row))
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
            }
        }
      }

      val Sub = new Op2F2(InfixNamespace, "subtract") {
        val tpe = BinaryOperationType(JType.JAbsoluteT, JType.JRelativeT, JType.JAbsoluteT)
        def f2: F2 = CF2P("builtin::infix::op2::subtract") {
          case (c1: LongColumn, c2: LongColumn) =>
            new LongFrom.LL(c1, c2, longOk, _ - _)

          case (c1: LongColumn, c2: DoubleColumn) =>
            new NumFrom.LD(c1, c2, numOk, _ - _)

          case (c1: LongColumn, c2: NumColumn) =>
            new NumFrom.LN(c1, c2, numOk, _ - _)

          case (c1: DoubleColumn, c2: LongColumn) =>
            new NumFrom.DL(c1, c2, numOk, _ - _)

          case (c1: DoubleColumn, c2: DoubleColumn) =>
            new DoubleFrom.DD(c1, c2, doubleOk, _ - _)

          case (c1: DoubleColumn, c2: NumColumn) =>
            new NumFrom.DN(c1, c2, numOk, _ - _)

          case (c1: NumColumn, c2: LongColumn) =>
            new NumFrom.NL(c1, c2, numOk, _ - _)

          case (c1: NumColumn, c2: DoubleColumn) =>
            new NumFrom.ND(c1, c2, numOk, _ - _)

          case (c1: NumColumn, c2: NumColumn) =>
            new NumFrom.NN(c1, c2, numOk, _ - _)

          case (c1: OffsetDateTimeColumn, c2: IntervalColumn) =>
            new OffsetDateTimeColumn {
              def apply(row: Int) = c2(row).subtractFromOffsetDateTime(c1(row))
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
            }

          case (c1: OffsetDateColumn, c2: IntervalColumn) =>
            new OffsetDateColumn {
              def apply(row: Int) = DateTimeInterval.subtractFromOffsetDate(c1(row), c2(row).period)
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row).isDateLike
            }

          case (c1: OffsetTimeColumn, c2: IntervalColumn) =>
            new OffsetTimeColumn {
              def apply(row: Int) = DateTimeInterval.subtractFromOffsetTime(c1(row), c2(row).duration)
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row).isTimeLike
            }

          case (c1: LocalDateTimeColumn, c2: IntervalColumn) =>
            new LocalDateTimeColumn {
              def apply(row: Int) = c2(row).subtractFromLocalDateTime(c1(row))
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
            }

          case (c1: LocalDateColumn, c2: IntervalColumn) =>
            new LocalDateColumn {
              def apply(row: Int) = DateTimeInterval.subtractFromLocalDate(c1(row), c2(row).period)
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row).isDateLike
            }

          case (c1: LocalTimeColumn, c2: IntervalColumn) =>
            new LocalTimeColumn {
              def apply(row: Int) = DateTimeInterval.subtractFromLocalTime(c1(row), c2(row).duration)
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row).isTimeLike
            }

          case (c1: LocalDateTimeColumn, c2: LocalDateTimeColumn) =>
            new IntervalColumn {
              def apply(row: Int) = DateTimeInterval.betweenLocalDateTime(c1(row), c2(row))
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
            }

          case (c1: LocalDateColumn, c2: LocalDateColumn) =>
            new IntervalColumn {
              def apply(row: Int) =
                DateTimeInterval.ofPeriod(DateTimeInterval.betweenLocalDate(c1(row), c2(row)))
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
            }

          case (c1: LocalTimeColumn, c2: LocalTimeColumn) =>
            new IntervalColumn {
              def apply(row: Int) =
                DateTimeInterval.ofDuration(DateTimeInterval.betweenLocalTime(c1(row), c2(row)))
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
            }

          case (c1: OffsetDateTimeColumn, c2: OffsetDateTimeColumn) =>
            new IntervalColumn {
              def apply(row: Int) = DateTimeInterval.betweenOffsetDateTime(c1(row), c2(row))
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
            }

          case (c1: OffsetDateColumn, c2: OffsetDateColumn) =>
            new IntervalColumn {
              def apply(row: Int) =
                DateTimeInterval.ofPeriod(DateTimeInterval.betweenOffsetDate(c1(row), c2(row)))
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
            }

          case (c1: OffsetTimeColumn, c2: OffsetTimeColumn) =>
            new IntervalColumn {
              def apply(row: Int) =
                DateTimeInterval.ofDuration(DateTimeInterval.betweenOffsetTime(c1(row), c2(row)))
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
            }

          case (c1: IntervalColumn, c2: IntervalColumn) =>
            new IntervalColumn {
              def apply(row: Int) = c1(row).minus(c2(row))
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
            }
        }
      }

      val Mul = new Op2F2(InfixNamespace, "multiply") {
        val tpe = BinaryOperationType(JType.JRelativeT, JType.JRelativeT, JType.JRelativeT)
        def f2: F2 = CF2P("builtin::infix::op2::multiply") {
          case (c1: LongColumn, c2: LongColumn) =>
            new LongFrom.LL(c1, c2, longOk, _ * _)

          case (c1: LongColumn, c2: DoubleColumn) =>
            new NumFrom.LD(c1, c2, numOk, _ * _)

          case (c1: LongColumn, c2: NumColumn) =>
            new NumFrom.LN(c1, c2, numOk, _ * _)

          case (c1: DoubleColumn, c2: LongColumn) =>
            new NumFrom.DL(c1, c2, numOk, _ * _)

          case (c1: DoubleColumn, c2: DoubleColumn) =>
            new DoubleFrom.DD(c1, c2, doubleOk, _ * _)

          case (c1: DoubleColumn, c2: NumColumn) =>
            new NumFrom.DN(c1, c2, numOk, _ * _)

          case (c1: NumColumn, c2: LongColumn) =>
            new NumFrom.NL(c1, c2, numOk, _ * _)

          case (c1: NumColumn, c2: DoubleColumn) =>
            new NumFrom.ND(c1, c2, numOk, _ * _)

          case (c1: NumColumn, c2: NumColumn) =>
            new NumFrom.NN(c1, c2, numOk, _ * _)

          case (c1: LongColumn, c2: IntervalColumn) =>
            new IntervalColumn {
              def apply(row: Int) = c2(row).multiply(c1(row).toInt)
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
            }

          case (c1: IntervalColumn, c2: LongColumn) =>
            new IntervalColumn {
              def apply(row: Int) = c1(row).multiply(c2(row).toInt)
              def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
            }
        }
      }

      // div needs to make sure to use Double even for division with longs
      val Div = new Op2F2(InfixNamespace, "divide") {
        def doublef(x: Double, y: Double) = x / y

        val context = java.math.MathContext.DECIMAL128
        def numf(x: BigDecimal, y: BigDecimal) = x(context) / y(context)

        val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
        def f2: F2 = CF2P("builtin::infix::div") {
          case (c1: LongColumn, c2: LongColumn) =>
            new DoubleFrom.LL(c1, c2, doubleNeZero, doublef)

          case (c1: LongColumn, c2: DoubleColumn) =>
            new NumFrom.LD(c1, c2, numNeZero, numf)

          case (c1: LongColumn, c2: NumColumn) =>
            new NumFrom.LN(c1, c2, numNeZero, numf)

          case (c1: DoubleColumn, c2: LongColumn) =>
            new NumFrom.DL(c1, c2, numNeZero, numf)

          case (c1: DoubleColumn, c2: DoubleColumn) =>
            new DoubleFrom.DD(c1, c2, doubleNeZero, doublef)

          case (c1: DoubleColumn, c2: NumColumn) =>
            new NumFrom.DN(c1, c2, numNeZero, numf)

          case (c1: NumColumn, c2: LongColumn) =>
            new NumFrom.NL(c1, c2, numNeZero, numf)

          case (c1: NumColumn, c2: DoubleColumn) =>
            new NumFrom.ND(c1, c2, numNeZero, numf)

          case (c1: NumColumn, c2: NumColumn) =>
            new NumFrom.NN(c1, c2, numNeZero, numf)
        }
      }

      val Mod = new Op2F2(InfixNamespace, "mod") {
        val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)

        def longMod(x: Long, y: Long) = x % y
        def doubleMod(x: Double, y: Double) = x % y
        def numMod(x: BigDecimal, y: BigDecimal) = x % y

        def f2: F2 = CF2P("builtin::infix::mod") {
          case (c1: LongColumn, c2: LongColumn) =>
            new LongFrom.LL(c1, c2, longNeZero, longMod)

          case (c1: LongColumn, c2: DoubleColumn) =>
            new NumFrom.LD(c1, c2, numNeZero, numMod)

          case (c1: LongColumn, c2: NumColumn) =>
            new NumFrom.LN(c1, c2, numNeZero, numMod)

          case (c1: DoubleColumn, c2: LongColumn) =>
            new NumFrom.DL(c1, c2, numNeZero, numMod)

          case (c1: DoubleColumn, c2: DoubleColumn) =>
            new DoubleFrom.DD(c1, c2, doubleNeZero, doubleMod)

          case (c1: DoubleColumn, c2: NumColumn) =>
            new NumFrom.DN(c1, c2, numNeZero, numMod)

          case (c1: NumColumn, c2: LongColumn) =>
            new NumFrom.NL(c1, c2, numNeZero, numMod)

          case (c1: NumColumn, c2: DoubleColumn) =>
            new NumFrom.ND(c1, c2, numNeZero, numMod)

          case (c1: NumColumn, c2: NumColumn) =>
            new NumFrom.NN(c1, c2, numNeZero, numMod)
        }
      }

      // Separate trait for use in MathLib
      trait Power {
        def cf2pName: String

        val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
        def defined(x: Double, y: Double) = doubleIsDefined(x) && doubleIsDefined(y)
        def f2: F2 = CF2P(cf2pName) {
          case (c1: DoubleColumn, c2: DoubleColumn) =>
            new DoubleFrom.DD(c1, c2, defined, scala.math.pow)

          case (c1: DoubleColumn, c2: LongColumn) =>
            new DoubleFrom.DL(c1, c2, defined, scala.math.pow)

          case (c1: DoubleColumn, c2: NumColumn) =>
            new DoubleFrom.DN(c1, c2, defined, scala.math.pow)

          case (c1: LongColumn, c2: DoubleColumn) =>
            new DoubleFrom.LD(c1, c2, defined, scala.math.pow)

          case (c1: NumColumn, c2: DoubleColumn) =>
            new DoubleFrom.ND(c1, c2, defined, scala.math.pow)

          case (c1: LongColumn, c2: LongColumn) =>
            new DoubleFrom.LL(c1, c2, defined, scala.math.pow)

          case (c1: LongColumn, c2: NumColumn) =>
            new DoubleFrom.LN(c1, c2, defined, scala.math.pow)

          case (c1: NumColumn, c2: LongColumn) =>
            new DoubleFrom.NL(c1, c2, defined, scala.math.pow)

          case (c1: NumColumn, c2: NumColumn) =>
            new DoubleFrom.NN(c1, c2, defined, scala.math.pow)
        }
      }

      object Pow extends Op2F2(InfixNamespace, "pow") with Power {
        val cf2pName = "builtin::infix::pow"
      }

      class CompareOp2(name: String, f: Int => Boolean) extends Op2F2(InfixNamespace, name) {
        val tpe = BinaryOperationType(JNumberT, JNumberT, JBooleanT)
        import NumericComparisons.compare
        def f2: F2 = CF2P("builtin::infix::compare") {
          case (c1: LongColumn, c2: LongColumn) =>
            new BoolFrom.LL(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

          case (c1: LongColumn, c2: DoubleColumn) =>
            new BoolFrom.LD(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

          case (c1: LongColumn, c2: NumColumn) =>
            new BoolFrom.LN(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

          case (c1: DoubleColumn, c2: LongColumn) =>
            new BoolFrom.DL(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

          case (c1: DoubleColumn, c2: DoubleColumn) =>
            new BoolFrom.DD(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

          case (c1: DoubleColumn, c2: NumColumn) =>
            new BoolFrom.DN(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

          case (c1: NumColumn, c2: LongColumn) =>
            new BoolFrom.NL(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

          case (c1: NumColumn, c2: DoubleColumn) =>
            new BoolFrom.ND(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

          case (c1: NumColumn, c2: NumColumn) =>
            new BoolFrom.NN(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

          case (c1: StrColumn, c2: StrColumn) =>
            new BoolFrom.SS(c1, c2, (x, y) => true, (x, y) => f(x.compareTo(y)))

          case (c1: OffsetDateTimeColumn, c2: OffsetDateTimeColumn) =>
            new BoolFrom.OdtmOdtm(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

          case (c1: OffsetDateColumn, c2: OffsetDateColumn) =>
            new BoolFrom.OdtOdt(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

          case (c1: OffsetTimeColumn, c2: OffsetTimeColumn) =>
            new BoolFrom.OtmOtm(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

          case (c1: LocalDateTimeColumn, c2: LocalDateTimeColumn) =>
            new BoolFrom.LdtmLdtm(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

          case (c1: LocalDateColumn, c2: LocalDateColumn) =>
            new BoolFrom.LdtLdt(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

          case (c1: LocalTimeColumn, c2: LocalTimeColumn) =>
            new BoolFrom.LtmLtm(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))
        }
      }

      val Lt   = new CompareOp2("lt", _ < 0)
      val LtEq = new CompareOp2("lte", _ <= 0)
      val Gt   = new CompareOp2("gt", _ > 0)
      val GtEq = new CompareOp2("gte", _ >= 0)

      class BoolOp2(name: String, f: (Boolean, Boolean) => Boolean) extends Op2F2(InfixNamespace, name) {
        val tpe = BinaryOperationType(JBooleanT, JBooleanT, JBooleanT)
        def f2: F2 = CF2P("builtin::infix::bool") {
          case (c1: BoolColumn, c2: BoolColumn) => new BoolFrom.BB(c1, c2, f)
        }
      }

      // TODO find the commonalities and abstract these two
      val And = new OpNFN(InfixNamespace, "and") {
        val fn: FN = CFNP("builtin::infix::bool::and") {
          case List(c: BoolColumn) =>
            new BoolColumn {
              def isDefinedAt(row: Int): Boolean = c.isDefinedAt(row) && !c(row)
              def apply(row: Int): Boolean = c(row)
            }

          case List(c1: BoolColumn, c2: BoolColumn) =>
            new BoolColumn {
              def isDefinedAt(row: Int): Boolean = {
                if (c1.isDefinedAt(row) && c2.isDefinedAt(row))
                  true
                else if (c1.isDefinedAt(row) && !c2.isDefinedAt(row))
                  !c1(row)
                else if (!c1.isDefinedAt(row) && c2.isDefinedAt(row))
                  !c2(row)
                else
                  false
              }

              def apply(row: Int): Boolean = {
                if (c1.isDefinedAt(row) && c2.isDefinedAt(row))
                  c1(row) && c2(row)
                else if (c1.isDefinedAt(row) && !c2.isDefinedAt(row))
                  c1(row)
                else if (!c1.isDefinedAt(row) && c2.isDefinedAt(row))
                  c2(row)
                else
                  false
              }
            }
        }
      }

      val Or = new OpNFN(InfixNamespace, "or") {
        val fn: FN = CFNP("builtin::infix::bool::or") {
          case List(c: BoolColumn) =>
            new BoolColumn {
              def isDefinedAt(row: Int): Boolean = c.isDefinedAt(row) && c(row)
              def apply(row: Int): Boolean = c(row)
            }

          case List(c1: BoolColumn, c2: BoolColumn) =>
            new BoolColumn {
              def isDefinedAt(row: Int): Boolean = {
                if (c1.isDefinedAt(row) && c2.isDefinedAt(row))
                  true
                else if (c1.isDefinedAt(row) && !c2.isDefinedAt(row))
                  c1(row)
                else if (!c1.isDefinedAt(row) && c2.isDefinedAt(row))
                  c2(row)
                else
                  false
              }

              def apply(row: Int): Boolean = {
                if (c1.isDefinedAt(row) && c2.isDefinedAt(row))
                  c1(row) || c2(row)
                else if (c1.isDefinedAt(row) && !c2.isDefinedAt(row))
                  c1(row)
                else if (!c1.isDefinedAt(row) && c2.isDefinedAt(row))
                  c2(row)
                else
                  false
              }
            }
        }
      }

      val concatString = new Op2F2(InfixNamespace, "concatString") {
        //@deprecated, see the DEPRECATED comment in StringLib
        val tpe = BinaryOperationType(JTextT, JTextT, JTextT)

        private def build(c1: StrColumn, c2: StrColumn) =
          new StrFrom.SS(c1, c2, _ != null && _ != null, _ + _)

        def f2: F2 = CF2P("builtin::infix:concatString") {
          case (c1: StrColumn, c2: StrColumn) => build(c1, c2)
        }
      }
    }
  }
}
