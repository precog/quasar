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

import quasar.precog.common._
import quasar.yggdrasil.bytecode._
import quasar.yggdrasil.table._

import BigDecimal.RoundingMode

trait UnaryLibModule extends ColumnarTableLibModule {
  trait UnaryLib extends ColumnarTableLib {
    import trans.{Range => _, _}
    import StdLib.{ BoolFrom, DoubleFrom, LongFrom, NumFrom, doubleIsDefined }

    object Unary {

      object Comp extends Op1F1 {
        val tpe = UnaryOperationType(JBooleanT, JBooleanT)
        def f1: CF1 = CF1P {
          case c: BoolColumn => new BoolFrom.B(c, !_)
        }

        def spec[A <: SourceType]: TransSpec[A] => TransSpec[A] = { transSpec =>
          trans.Map1(transSpec, f1)
        }
      }

      object Neg extends Op1F1 {
        val tpe = UnaryOperationType(JNumberT, JNumberT)
        def f1: CF1 = CF1P {
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
      object Ceil extends Op1F1 {
        val tpe = UnaryOperationType(JNumberT, JNumberT)
        def f1: CF1 = CF1P {
          case c: DoubleColumn => new DoubleFrom.D(c, doubleIsDefined, math.ceil)

          case c: LongColumn   => new LongFrom.L(c, n => true, x => x)
          case c: NumColumn    => new NumFrom.N(c, n => true, _.setScale(0, RoundingMode.CEILING))
        }

        def spec[A <: SourceType]: TransSpec[A] => TransSpec[A] = { transSpec =>
          trans.Map1(transSpec, f1)
        }
      }

      object Floor extends Op1F1 {
        val tpe = UnaryOperationType(JNumberT, JNumberT)
        def f1: CF1 = CF1P {
          case c: DoubleColumn => new DoubleFrom.D(c, doubleIsDefined, math.floor)
          case c: LongColumn   => new LongFrom.L(c, n => true, x => x)
          case c: NumColumn    => new NumFrom.N(c, n => true, _.setScale(0, RoundingMode.FLOOR))
        }

        def spec[A <: SourceType]: TransSpec[A] => TransSpec[A] = { transSpec =>
          trans.Map1(transSpec, f1)
        }
      }

      object Trunc extends Op1F1 {
        val tpe = UnaryOperationType(JNumberT, JNumberT)
        def f1: CF1 = CF1P {
          case c: DoubleColumn =>
            new DoubleFrom.D(c, doubleIsDefined, { d =>
              d - (d % 1.0)
            })

          case c: LongColumn =>
            new LongFrom.L(c, n => true, x => x)

          case c: NumColumn =>
            new NumFrom.N(c, n => true, _.setScale(0, RoundingMode.DOWN))
        }

        def spec[A <: SourceType]: TransSpec[A] => TransSpec[A] = { transSpec =>
          trans.Map1(transSpec, f1)
        }
      }

      object Round extends Op1F1 {
        val tpe = UnaryOperationType(JNumberT, JNumberT)
        def f1: CF1 = CF1P {
          // encoding of half-even rounding
          case c: DoubleColumn => new DoubleFrom.D(c, doubleIsDefined, { d =>
            if (math.abs(d % 1) == 0.5) {
              val candidate = math.ceil(d)

              if (candidate % 2 == 0)
                candidate
              else
                math.floor(d)
            } else {
              math.round(d)
            }
          })
          case c: LongColumn   => new LongFrom.L(c, n => true, x => x)
          case c: NumColumn    => new NumFrom.N(c, n => true, _.setScale(0, RoundingMode.HALF_EVEN))
        }

        def spec[A <: SourceType]: TransSpec[A] => TransSpec[A] = { transSpec =>
          trans.Map1(transSpec, f1)
        }
      }

      // this implementation of abs checks for boundary conditions and up-coerces as necessary
      // for example: Long.MinValue.abs == Long.MinValue, which violates invariants
      // we may want to generalize this pattern a bit, since there are other similar functions
      object Abs extends CMapperS {

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
