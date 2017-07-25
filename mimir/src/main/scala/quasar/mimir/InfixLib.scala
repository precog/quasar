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

package quasar.mimir

import quasar.yggdrasil.bytecode._
import quasar.yggdrasil.table._

import quasar.precog.util.NumericComparisons

trait InfixLibModule[M[+ _]] extends ColumnarTableLibModule[M] {
  trait InfixLib extends ColumnarTableLib {
    import StdLib.{ BoolFrom, DoubleFrom, LongFrom, NumFrom, StrFrom, doubleIsDefined, StrAndDateT, dateToStrCol }

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

      val Add = new InfixOp2("add", _ + _, _ + _, _ + _)
      val Sub = new InfixOp2("subtract", _ - _, _ - _, _ - _)
      val Mul = new InfixOp2("multiply", _ * _, _ * _, _ * _)

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

          case (c1: DateColumn, c2: DateColumn) =>
            new BoolFrom.DtDt(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))
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

      val And = new BoolOp2("and", _ && _)
      val Or  = new BoolOp2("or", _ || _)

      val concatString = new Op2F2(InfixNamespace, "concatString") {
        //@deprecated, see the DEPRECATED comment in StringLib
        val tpe = BinaryOperationType(StrAndDateT, StrAndDateT, JTextT)

        private def build(c1: StrColumn, c2: StrColumn) =
          new StrFrom.SS(c1, c2, _ != null && _ != null, _ + _)

        def f2: F2 = CF2P("builtin::infix:concatString") {
          case (c1: StrColumn, c2: StrColumn)   => build(c1, c2)
          case (c1: DateColumn, c2: StrColumn)  => build(dateToStrCol(c1), c2)
          case (c1: StrColumn, c2: DateColumn)  => build(c1, dateToStrCol(c2))
          case (c1: DateColumn, c2: DateColumn) => build(dateToStrCol(c1), dateToStrCol(c2))
        }
      }
    }
  }
}
