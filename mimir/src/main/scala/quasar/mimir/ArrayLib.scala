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
import quasar.blueeyes._
import quasar.precog.common._

import quasar.yggdrasil._
import quasar.yggdrasil.table._

trait ArrayLibModule[M[+ _]] extends ColumnarTableLibModule[M] {
  trait ArrayLib extends ColumnarTableLib {
    import StdLib.dateToStrCol

    override def _libMorphism1 = super._libMorphism1 ++ Set(Flatten)

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

    object Flatten extends Morphism1(Vector(), "flatten") {
      import trans._
      import TransSpecModule._

      val tpe = UnaryOperationType(JArrayUnfixedT, JType.JUniverseT)

      override val idPolicy = IdentityPolicy.Product(IdentityPolicy.Retain.Merge, IdentityPolicy.Synthesize)

      def apply(table: Table) = M point {
        val derefed = table transform trans.DerefObjectStatic(Leaf(Source), paths.Value)
        val keys    = table transform trans.DerefObjectStatic(Leaf(Source), paths.Key)

        val flattenedSlices = table.slices map { slice =>
          val keys   = slice.deref(paths.Key)
          val values = slice.deref(paths.Value)

          val indices = values.columns.keys collect {
            case ColumnRef(CPath(CPathIndex(i), _ @_ *), _) => i
          }

          if (indices.isEmpty) {
            Slice.empty
          } else {
            val maxLength = indices.max + 1

            val columnTables = values.columns.foldLeft(Map[ColumnRef, Array[Column]]()) {
              case (acc, (ColumnRef(CPath(CPathIndex(idx), ptail @ _ *), tpe), col)) => {
                // remap around the mod ring w.r.t. max length
                // s.t. f(i) = f'(i * max + arrayI)

                val finalRef = ColumnRef(CPath(ptail: _*), tpe)
                val colTable = acc get finalRef getOrElse (new Array[Column](maxLength))

                colTable(idx) = col

                acc.updated(finalRef, colTable)
              }

              case (acc, _) => acc
            }

            val valueCols = columnTables map {
              case (ref @ ColumnRef(_, CUndefined), _) =>
                ref -> UndefinedColumn.raw

              case (ref @ ColumnRef(_, CBoolean), colTable) => {
                val col = new ModUnionColumn(colTable) with BoolColumn {
                  def apply(i: Int) = col(i).asInstanceOf[BoolColumn](row(i))
                }

                ref -> col
              }

              case (ref @ ColumnRef(_, CString), colTable) => {
                val col = new ModUnionColumn(colTable) with StrColumn {
                  def apply(i: Int) = col(i).asInstanceOf[StrColumn](row(i))
                }

                ref -> col
              }

              case (ref @ ColumnRef(_, CLong), colTable) => {
                val col = new ModUnionColumn(colTable) with LongColumn {
                  def apply(i: Int) = col(i).asInstanceOf[LongColumn](row(i))
                }

                ref -> col
              }

              case (ref @ ColumnRef(_, CDouble), colTable) => {
                val col = new ModUnionColumn(colTable) with DoubleColumn {
                  def apply(i: Int) = col(i).asInstanceOf[DoubleColumn](row(i))
                }

                ref -> col
              }

              case (ref @ ColumnRef(_, CNum), colTable) => {
                val col = new ModUnionColumn(colTable) with NumColumn {
                  def apply(i: Int) = col(i).asInstanceOf[NumColumn](row(i))
                }

                ref -> col
              }

              case (ref @ ColumnRef(_, CEmptyObject), colTable) => {
                val col = new ModUnionColumn(colTable) with EmptyObjectColumn

                ref -> col
              }

              case (ref @ ColumnRef(_, CEmptyArray), colTable) => {
                val col = new ModUnionColumn(colTable) with EmptyArrayColumn

                ref -> col
              }

              case (ref @ ColumnRef(_, CNull), colTable) => {
                val col = new ModUnionColumn(colTable) with NullColumn

                ref -> col
              }

              case (ref @ ColumnRef(_, CDate), colTable) => {
                val col = new ModUnionColumn(colTable) with DateColumn {
                  def apply(i: Int) = col(i).asInstanceOf[DateColumn](row(i))
                }

                ref -> col
              }

              case (ref @ ColumnRef(_, CPeriod), colTable) => {
                val col = new ModUnionColumn(colTable) with PeriodColumn {
                  def apply(i: Int) = col(i).asInstanceOf[PeriodColumn](row(i))
                }

                ref -> col
              }

              case (ref @ ColumnRef(_, arrTpe: CArrayType[a]), colTable) => {
                val col = new ModUnionColumn(colTable) with HomogeneousArrayColumn[a] {
                  val tpe = arrTpe
                  def apply(i: Int) =
                    col(i).asInstanceOf[HomogeneousArrayColumn[a]](row(i)) // primitive arrays are still objects, so the erasure here is not a problem
                }

                ref -> col
              }
            }

            val remap = cf.util.Remap(_ / maxLength)
            val keyCols = for {
              (ref, col) <- keys.columns
              col0 <- remap(col)
            } yield (ref -> col0)

            val sliceSize  = maxLength * slice.size
            val keySlice   = Slice(keyCols, sliceSize).wrap(paths.Key)
            val valueSlice = Slice(valueCols, sliceSize).wrap(paths.Value)
            keySlice zip valueSlice
          }
        }

        val size2          = UnknownSize
        val flattenedTable = Table(flattenedSlices, UnknownSize).compact(TransSpec1.Id)
        val finalTable     = flattenedTable.canonicalize(yggConfig.minIdealSliceSize, Some(yggConfig.maxSliceSize))

        val spec = InnerObjectConcat(
          WrapObject(
            InnerArrayConcat(
              DerefObjectStatic(Leaf(Source), paths.Key),
              WrapArray(Scan(Leaf(Source), freshIdScanner))
            ),
            paths.Key.name),
          WrapObject(DerefObjectStatic(Leaf(Source), paths.Value), paths.Value.name))

        finalTable transform spec
      }
    }
  }
}
