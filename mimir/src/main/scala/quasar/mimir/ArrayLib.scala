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

import quasar.yggdrasil.table._

trait ArrayLibModule extends ColumnarTableLibModule {
  trait ArrayLib extends ColumnarTableLib {

    override def _libMorphism1 = super._libMorphism1

    // this isn't really an array function, but I don't know where else to put it
    // this is basically lower <= target <= upper, except with support for strings
    // and dates/times/datetimes
    lazy val between: CFN = CFNP("std::array::between") {
      case List(target: LongColumn, lower: LongColumn, upper: LongColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DoubleColumn, lower: LongColumn, upper: LongColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: NumColumn, lower: LongColumn, upper: LongColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LongColumn, lower: DoubleColumn, upper: LongColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DoubleColumn, lower: DoubleColumn, upper: LongColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: NumColumn, lower: DoubleColumn, upper: LongColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LongColumn, lower: NumColumn, upper: LongColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DoubleColumn, lower: NumColumn, upper: LongColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: NumColumn, lower: NumColumn, upper: LongColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LongColumn, lower: LongColumn, upper: DoubleColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DoubleColumn, lower: LongColumn, upper: DoubleColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: NumColumn, lower: LongColumn, upper: DoubleColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LongColumn, lower: DoubleColumn, upper: DoubleColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DoubleColumn, lower: DoubleColumn, upper: DoubleColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: NumColumn, lower: DoubleColumn, upper: DoubleColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LongColumn, lower: NumColumn, upper: DoubleColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DoubleColumn, lower: NumColumn, upper: DoubleColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: NumColumn, lower: NumColumn, upper: DoubleColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LongColumn, lower: LongColumn, upper: NumColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DoubleColumn, lower: LongColumn, upper: NumColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: NumColumn, lower: LongColumn, upper: NumColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LongColumn, lower: DoubleColumn, upper: NumColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DoubleColumn, lower: DoubleColumn, upper: NumColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: NumColumn, lower: DoubleColumn, upper: NumColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LongColumn, lower: NumColumn, upper: NumColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DoubleColumn, lower: NumColumn, upper: NumColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: NumColumn, lower: NumColumn, upper: NumColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            t >= lower(row) && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: OffsetDateTimeColumn, lower: OffsetDateTimeColumn, upper: OffsetDateTimeColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            lower(row).compareTo(t) <= 0 && t.compareTo(upper(row)) <= 0
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: OffsetDateColumn, lower: OffsetDateColumn, upper: OffsetDateColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            lower(row).compareTo(t) <= 0 && t.compareTo(upper(row)) <= 0
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: OffsetTimeColumn, lower: OffsetTimeColumn, upper: OffsetTimeColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            lower(row).compareTo(t) <= 0 && t.compareTo(upper(row)) <= 0
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LocalDateTimeColumn, lower: LocalDateTimeColumn, upper: LocalDateTimeColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            lower(row).compareTo(t) <= 0 && t.compareTo(upper(row)) <= 0
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LocalDateColumn, lower: LocalDateColumn, upper: LocalDateColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            lower(row).compareTo(t) <= 0 && t.compareTo(upper(row)) <= 0
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LocalTimeColumn, lower: LocalTimeColumn, upper: LocalTimeColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            lower(row).compareTo(t) <= 0 && t.compareTo(upper(row)) <= 0
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: BoolColumn, lower: BoolColumn, upper: BoolColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            val l = lower(row)
            val u = upper(row)
            ((l == t) && (t == u)) || (!l && ((!t && u) || (t && u)))
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: StrColumn, lower: StrColumn, upper: StrColumn) =>
        new BoolColumn {
          def apply(row: Int) = {
            val t = target(row)
            lower(row) <= t && t <= upper(row)
          }
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }
    }
  }
}
