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

package quasar.yggdrasil
package table
package cf

object math {
  val Negate = CF1P("builtin::ct::negate") {
    case c: BoolColumn =>
      new Map1Column(c) with BoolColumn {
        def apply(row: Int) = !c(row)
      }
    case c: LongColumn =>
      new Map1Column(c) with LongColumn {
        def apply(row: Int) = -c(row)
      }
    case c: DoubleColumn =>
      new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = -c(row)
      }
    case c: NumColumn =>
      new Map1Column(c) with NumColumn {
        def apply(row: Int) = -c(row)
      }
  }

  val Add = CF2P("builtin::ct::add") {
    case (c1: BoolColumn, c2: BoolColumn) =>
      new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = c1(row) || c2(row)
      }
    case (c1: LongColumn, c2: LongColumn) =>
      new Map2Column(c1, c2) with LongColumn {
        def apply(row: Int) = c1(row) + c2(row)
      }
    case (c1: LongColumn, c2: DoubleColumn) =>
      new Map2Column(c1, c2) with NumColumn {
        def apply(row: Int) = c1(row) + c2(row)
      }
    case (c1: LongColumn, c2: NumColumn) =>
      new Map2Column(c1, c2) with NumColumn {
        def apply(row: Int) = c1(row) + c2(row)
      }
    case (c1: DoubleColumn, c2: LongColumn) =>
      new Map2Column(c1, c2) with NumColumn {
        def apply(row: Int) = c1(row) + c2(row)
      }
    case (c1: DoubleColumn, c2: DoubleColumn) =>
      new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = c1(row) + c2(row)
      }
    case (c1: DoubleColumn, c2: NumColumn) =>
      new Map2Column(c1, c2) with NumColumn {
        def apply(row: Int) = c1(row) + c2(row)
      }
    case (c1: NumColumn, c2: LongColumn) =>
      new Map2Column(c1, c2) with NumColumn {
        def apply(row: Int) = c1(row) + c2(row)
      }
    case (c1: NumColumn, c2: DoubleColumn) =>
      new Map2Column(c1, c2) with NumColumn {
        def apply(row: Int) = c1(row) + c2(row)
      }
    case (c1: NumColumn, c2: NumColumn) =>
      new Map2Column(c1, c2) with NumColumn {
        def apply(row: Int) = c1(row) + c2(row)
      }
  }

  val Mod = CF2P("builtin::ct::mod") {
    case (c1: LongColumn, c2: LongColumn) =>
      new Map2Column(c1, c2) with LongColumn {
        def apply(row: Int) = c1(row) % c2(row)
      }
    case (c1: LongColumn, c2: DoubleColumn) =>
      new Map2Column(c1, c2) with NumColumn {
        def apply(row: Int) = c1(row) % c2(row)
      }
    case (c1: LongColumn, c2: NumColumn) =>
      new Map2Column(c1, c2) with NumColumn {
        def apply(row: Int) = c1(row) % c2(row)
      }
    case (c1: DoubleColumn, c2: LongColumn) =>
      new Map2Column(c1, c2) with NumColumn {
        def apply(row: Int) = c1(row) % c2(row)
      }
    case (c1: DoubleColumn, c2: DoubleColumn) =>
      new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = c1(row) % c2(row)
      }
    case (c1: DoubleColumn, c2: NumColumn) =>
      new Map2Column(c1, c2) with NumColumn {
        def apply(row: Int) = c1(row) % c2(row)
      }
    case (c1: NumColumn, c2: LongColumn) =>
      new Map2Column(c1, c2) with NumColumn {
        def apply(row: Int) = c1(row) % c2(row)
      }
    case (c1: NumColumn, c2: DoubleColumn) =>
      new Map2Column(c1, c2) with NumColumn {
        def apply(row: Int) = c1(row) % c2(row)
      }
    case (c1: NumColumn, c2: NumColumn) =>
      new Map2Column(c1, c2) with NumColumn {
        def apply(row: Int) = c1(row) % c2(row)
      }
  }
}

// type Math
