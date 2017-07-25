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

import quasar.precog.common._
import quasar.yggdrasil.bytecode._
import quasar.yggdrasil.table._

import java.math.{MathContext, RoundingMode}

trait UnaryLibModule[M[+ _]] extends ColumnarTableLibModule[M] {
  trait UnaryLib extends ColumnarTableLib {
    import trans._
    import StdLib.{ BoolFrom, DoubleFrom, LongFrom, NumFrom, doubleIsDefined }

    object Unary {
      val UnaryNamespace = Vector("std", "unary")

      object Comp extends Op1F1(UnaryNamespace, "comp") {
        val tpe = UnaryOperationType(JBooleanT, JBooleanT)
        def f1: F1 = CF1P("builtin::unary::comp") {
          case c: BoolColumn => new BoolFrom.B(c, !_)
        }

        def spec[A <: SourceType]: TransSpec[A] => TransSpec[A] = { transSpec =>
          trans.Map1(transSpec, f1)
        }
      }

      object Neg extends Op1F1(UnaryNamespace, "neg") {
        val tpe = UnaryOperationType(JNumberT, JNumberT)
        def f1: F1 = CF1P("builtin::unary::neg") {
          case c: DoubleColumn => new DoubleFrom.D(c, doubleIsDefined, -_)
          case c: LongColumn   => new LongFrom.L(c, n => true, -_)
          case c: NumColumn    => new NumFrom.N(c, n => true, -_)
        }

        def spec[A <: SourceType]: TransSpec[A] => TransSpec[A] = { transSpec =>
          trans.Map1(transSpec, f1)
        }
      }

      // use these rather than the ones in MathLib
      // TODO remove the old ones in MathLib
      object Ceil extends Op1F1(UnaryNamespace, "ceil") {
        private val ctx = new MathContext(0, RoundingMode.CEILING)
        val tpe = UnaryOperationType(JNumberT, JNumberT)
        def f1: F1 = CF1P("builtin::unary::ceil") {
          case c: DoubleColumn => new DoubleFrom.D(c, doubleIsDefined, math.ceil)
          case c: LongColumn   => new LongFrom.L(c, n => true, x => x)
          case c: NumColumn    => new NumFrom.N(c, n => true, _.round(ctx))
        }

        def spec[A <: SourceType]: TransSpec[A] => TransSpec[A] = { transSpec =>
          trans.Map1(transSpec, f1)
        }
      }

      object Floor extends Op1F1(UnaryNamespace, "floor") {
        private val ctx = new MathContext(0, RoundingMode.FLOOR)
        val tpe = UnaryOperationType(JNumberT, JNumberT)
        def f1: F1 = CF1P("builtin::unary::floor") {
          case c: DoubleColumn => new DoubleFrom.D(c, doubleIsDefined, math.floor)
          case c: LongColumn   => new LongFrom.L(c, n => true, x => x)
          case c: NumColumn    => new NumFrom.N(c, n => true, _.round(ctx))
        }

        def spec[A <: SourceType]: TransSpec[A] => TransSpec[A] = { transSpec =>
          trans.Map1(transSpec, f1)
        }
      }

      object Trunc extends Op1F1(UnaryNamespace, "trunc") {
        private val ctx = new MathContext(0, RoundingMode.DOWN)
        val tpe = UnaryOperationType(JNumberT, JNumberT)
        def f1: F1 = CF1P("builtin::unary::trunc") {
          case c: DoubleColumn => new DoubleFrom.D(c, doubleIsDefined, { d =>
            val result = math.round(d)
            // the JVM uses half-over rounding semantics by default
            if (result > d) math.floor(d) else result
          })
          case c: LongColumn   => new LongFrom.L(c, n => true, x => x)
          case c: NumColumn    => new NumFrom.N(c, n => true, _.round(ctx))
        }

        def spec[A <: SourceType]: TransSpec[A] => TransSpec[A] = { transSpec =>
          trans.Map1(transSpec, f1)
        }
      }

      // this implementation of abs checks for boundary conditions and up-coerces as necessary
      // for example: Long.MinValue.abs == Long.MinValue, which violates invariants
      // we may want to generalize this pattern a bit, since there are other similar functions
      object Abs extends CMapperS[M] {

        def spec[A <: SourceType](source: TransSpec[A]): TransSpec[A] =
          MapWith(source, this)

        def map(cols: Map[ColumnRef, Column], range: Range): Map[ColumnRef, Column] = {
          // we look for boundary cases and coerce up if we see them
          // this is slightly faster than doing it on a row-by-row basis with a union column
          val targets = cols collect {
            case (ColumnRef(CPath.Identity, CDouble), col: DoubleColumn) =>
              // this will be inefficient on any column that isn't forced
              val atBound = range exists { i =>
                col.isDefinedAt(i) && col(i) == Double.MinValue
              }

              if (atBound)
                new NumFrom.D(col, _ => true, x => x)
              else
                col

            case (ColumnRef(CPath.Identity, CLong), col: LongColumn) =>
              // this will be inefficient on any column that isn't forced
              val atBound = range exists { i =>
                col.isDefinedAt(i) && col(i) == Long.MinValue
              }

              if (atBound)
                new NumFrom.L(col, _ => true, x => x)
              else
                col

            case (ColumnRef(CPath.Identity, CNum), col) => col
          }

          val pairs: Iterable[(ColumnRef, Column)] = targets map {
            case c: DoubleColumn =>
              ColumnRef(CPath.Identity, CDouble) -> new DoubleFrom.D(c, _ => true, _.abs)

            case c: LongColumn =>
              ColumnRef(CPath.Identity, CLong) -> new LongFrom.L(c, _ => true, _.abs)

            case c: NumColumn =>
              ColumnRef(CPath.Identity, CNum) -> new NumFrom.N(c, _ => true, _.abs)

            case _ => sys.error("impossible")
          }

          pairs.groupBy(_._1).map({
            case (ref, cols) =>
              ref -> cols.map(_._2).reduce(cf.util.UnionRight(_, _).get)
          })(collection.breakOut)
        }
      }
    }
  }
}
