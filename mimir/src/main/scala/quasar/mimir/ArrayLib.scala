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

trait ArrayLibModule[M[+ _]] extends ColumnarTableLibModule[M] {
  trait ArrayLib extends ColumnarTableLib {
    import StdLib.dateToStrCol

    override def _libMorphism1 = super._libMorphism1

    // this isn't really an array function, but I don't know where else to put it
    // this is basically lower <= target <= upper, except with support for strings
    lazy val between: CFN = CFNP("std::array::between") {
      case List(target: LongColumn, lower: LongColumn, upper: LongColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DoubleColumn, lower: LongColumn, upper: LongColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: NumColumn, lower: LongColumn, upper: LongColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LongColumn, lower: DoubleColumn, upper: LongColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DoubleColumn, lower: DoubleColumn, upper: LongColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: NumColumn, lower: DoubleColumn, upper: LongColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LongColumn, lower: NumColumn, upper: LongColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DoubleColumn, lower: NumColumn, upper: LongColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: NumColumn, lower: NumColumn, upper: LongColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LongColumn, lower: LongColumn, upper: DoubleColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DoubleColumn, lower: LongColumn, upper: DoubleColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: NumColumn, lower: LongColumn, upper: DoubleColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LongColumn, lower: DoubleColumn, upper: DoubleColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DoubleColumn, lower: DoubleColumn, upper: DoubleColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: NumColumn, lower: DoubleColumn, upper: DoubleColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LongColumn, lower: NumColumn, upper: DoubleColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DoubleColumn, lower: NumColumn, upper: DoubleColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: NumColumn, lower: NumColumn, upper: DoubleColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LongColumn, lower: LongColumn, upper: NumColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DoubleColumn, lower: LongColumn, upper: NumColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: NumColumn, lower: LongColumn, upper: NumColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LongColumn, lower: DoubleColumn, upper: NumColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DoubleColumn, lower: DoubleColumn, upper: NumColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: NumColumn, lower: DoubleColumn, upper: NumColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: LongColumn, lower: NumColumn, upper: NumColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DoubleColumn, lower: NumColumn, upper: NumColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: NumColumn, lower: NumColumn, upper: NumColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: StrColumn, lower: StrColumn, upper: StrColumn) =>
        new BoolColumn {
          def apply(row: Int) = target(row) >= lower(row) && target(row) <= upper(row)
          def isDefinedAt(row: Int) = target.isDefinedAt(row) && lower.isDefinedAt(row) && upper.isDefinedAt(row)
        }

      case List(target: DateColumn, lower: StrColumn, upper: StrColumn) =>
        between(List(dateToStrCol(target), lower, upper)).get

      case List(target: StrColumn, lower: DateColumn, upper: StrColumn) =>
        between(List(target, dateToStrCol(lower), upper)).get

      case List(target: DateColumn, lower: DateColumn, upper: StrColumn) =>
        between(List(dateToStrCol(target), dateToStrCol(lower), upper)).get

      case List(target: DateColumn, lower: StrColumn, upper: DateColumn) =>
        between(List(dateToStrCol(target), lower, dateToStrCol(upper))).get

      case List(target: StrColumn, lower: DateColumn, upper: DateColumn) =>
        between(List(target, dateToStrCol(lower), dateToStrCol(upper))).get

      case List(target: DateColumn, lower: DateColumn, upper: DateColumn) =>
        between(List(dateToStrCol(target), dateToStrCol(lower), dateToStrCol(upper))).get
    }
  }
}
