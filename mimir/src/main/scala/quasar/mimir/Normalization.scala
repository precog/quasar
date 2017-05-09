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

import quasar.yggdrasil._
import quasar.yggdrasil.table._

import quasar.blueeyes._
import quasar.yggdrasil.bytecode._
import quasar.precog.common._
import quasar.precog.util._

import scalaz._
import scalaz.std.list._
import scalaz.syntax.monad._

class BigDecimalPrecision(num: BigDecimal) {
  val context = java.math.MathContext.DECIMAL128
  def addContext: BigDecimal = BigDecimal(num.toString, context)
}

trait NormalizationHelperModule[M[+ _]] extends ColumnarTableLibModule[M] with ReductionLibModule[M] {

  trait NormalizationHelperLib extends ColumnarTableLib with ReductionLib {

    trait NormalizationHelper {
      import TransSpecModule._

      val tpe = BinaryOperationType(JType.JUniverseT, JObjectUnfixedT, JType.JUniverseT)

      case class Stats(mean: BigDecimal, stdDev: BigDecimal)
      case class RowValueWithStats(rowValue: BigDecimal, stats: Stats)

      type Summary = Map[CPath, Stats]
      type Result  = List[Summary]

      implicit val monoid = implicitly[Monoid[Result]]

      implicit def makePrecise(num: BigDecimal) = new BigDecimalPrecision(num)

      // Ported over from Switzer's exponential smoothing branch.
      // Need to put in common place!
      def unifyNumColumns(cols: Iterable[Column]): NumColumn = {
        val cols0: Array[NumColumn] = cols.collect({
          case (col: LongColumn) =>
            new Map1Column(col) with NumColumn {
              def apply(row: Int) = BigDecimal(col(row))
            }
          case (col: DoubleColumn) =>
            new Map1Column(col) with NumColumn {
              def apply(row: Int) = BigDecimal(col(row))
            }
          case (col: NumColumn) => col
        })(collection.breakOut)

        new UnionLotsColumn[NumColumn](cols0) with NumColumn {
          def apply(row: Int): BigDecimal = {
            var i = 0
            while (i < cols0.length) {
              if (cols0(i).isDefinedAt(row))
                return cols0(i)(row)
              i += 1
            }
            return null
          }
        }
      }

      def reducer = new CReducer[Result] {
        def reduce(schema: CSchema, range: Range) = {
          val refs: Set[ColumnRef] = schema.columnRefs

          def collectReduction(reduction: Reduction): Set[CPath] = {
            refs collect {
              case ColumnRef(selector, ctype) if selector.hasSuffix(CPathField(reduction.name)) && ctype.isNumeric =>
                selector.take(selector.length - 1) getOrElse CPath.Identity
            }
          }

          val meanPaths   = collectReduction(Mean)
          val stdDevPaths = collectReduction(StdDev)

          val commonPaths = (meanPaths & stdDevPaths).toList

          def getColumns(reduction: Reduction): List[(CPath, NumColumn)] = {
            commonPaths map { path =>
              val augPath = path \ CPathField(reduction.name)
              val jtype   = Schema.mkType(List(ColumnRef(augPath, CNum)))

              val cols       = jtype map { schema.columns } getOrElse Set.empty[Column]
              val unifiedCol = unifyNumColumns(cols.toList)

              (path, unifiedCol)
            }
          }

          val meanCols   = getColumns(Mean)
          val stdDevCols = getColumns(StdDev)

          val totalColumns: List[(CPath, (NumColumn, NumColumn))] = {
            meanCols flatMap {
              case (cpathMean, colMean) =>
                stdDevCols collect {
                  case (cpathStdDev, colStdDev) if cpathMean == cpathStdDev =>
                    (cpathMean, (colMean, colStdDev))
                }
            }
          }

          range.toList map { i =>
            totalColumns.collect {
              case (cpath, (meanCol, stdDevCol)) if meanCol.isDefinedAt(i) && stdDevCol.isDefinedAt(i) =>
                (cpath, Stats(meanCol(i), stdDevCol(i)))
            }.toMap
          }
        }
      }

      def applyMapper(summary: Result, mapper: Summary => CMapper[M], table: Table, ctx: MorphContext): M[Table] = {
        val resultTables = summary map {
          case singleSummary =>
            val spec = liftToValues(trans.MapWith(trans.TransSpec1.Id, mapper(singleSummary)))
            table.transform(spec)
        }

        val result = resultTables reduceOption { _ concat _ } getOrElse Table.empty

        M.point(result)
      }

      def normMapper(f: RowValueWithStats => BigDecimal)(singleSummary: Summary) = new CMapperS[M] {

        def findSuffices(cpath: CPath): Set[CPath] =
          singleSummary.keySet.filter(cpath.hasSuffix)

        def map(cols: Map[ColumnRef, Column], range: Range): Map[ColumnRef, Column] = {
          val numericCols = cols filter {
            case (ColumnRef(cpath, ctype), _) =>
              ctype.isNumeric
          }

          val groupedCols: Map[CPath, Map[ColumnRef, Column]] = numericCols.groupBy { case (ColumnRef(selector, _), _) => selector }

          def continue: Map[ColumnRef, Column] = {
            val unifiedCols: Map[ColumnRef, Column] = {
              groupedCols map {
                case (cpath, refs) =>
                  (ColumnRef(cpath, CNum), unifyNumColumns(refs.values))
              }
            }

            val resultsAll = unifiedCols collect {
              case (ColumnRef(selector, ctype), col: NumColumn) if findSuffices(selector).size == 1 => {
                val suffix = findSuffices(selector).head

                val mean   = singleSummary(suffix).mean
                val stdDev = singleSummary(suffix).stdDev

                val newColumn = new Map1Column(col) with NumColumn {
                  def value(row: Int) =
                    RowValueWithStats(col(row).addContext, Stats(mean.addContext, stdDev.addContext))

                  def apply(row: Int) = f(value(row))
                }

                (ColumnRef(selector, ctype), newColumn)
              }
            }

            val bitsets       = resultsAll.values map { _.definedAt(0, range.end) }
            val definedBitset = bitsets reduceOption { _ & _ } getOrElse BitSetUtil.create()

            def intersectColumn(col: NumColumn): NumColumn = {
              new BitsetColumn(definedBitset) with NumColumn {
                def apply(row: Int) = col.apply(row)
              }
            }

            resultsAll map {
              case (ref, col) =>
                (ref, intersectColumn(col))
            }
          }

          val subsumes = singleSummary forall {
            case (cpath, _) =>
              groupedCols.keySet exists { _.hasSuffix(cpath) }
          }

          if (subsumes)
            continue
          else
            Map.empty[ColumnRef, Column]
        }
      }

      lazy val alignment = MorphismAlignment.Custom(IdentityPolicy.Retain.Cross, alignCustom _)

      def morph1Apply(summary: Result): Morph1Apply

      def alignCustom(t1: Table, t2: Table): M[(Table, Morph1Apply)] = {
        val valueTable = t2.transform(trans.DerefObjectStatic(trans.TransSpec1.Id, paths.Value))
        valueTable.reduce(reducer) map { summary =>
          (t1, morph1Apply(summary))
        }
      }
    }
  }
}

trait NormalizationLibModule[M[+ _]] extends NormalizationHelperModule[M] {
  trait NormalizationLib extends NormalizationHelperLib {

    override def _libMorphism2 = super._libMorphism2 ++ Set(Normalization, Denormalization)

    object Normalization extends Morphism2(Vector("std", "stats"), "normalize") with NormalizationHelper {
      override val idPolicy: IdentityPolicy = IdentityPolicy.Retain.Left

      def morph1Apply(summary: Result) = new Morph1Apply {

        def apply(table: Table, ctx: MorphContext): M[Table] = {
          def makeValue(info: RowValueWithStats): BigDecimal =
            (info.rowValue - info.stats.mean) / info.stats.stdDev

          applyMapper(summary, normMapper(makeValue), table, ctx)
        }
      }
    }

    object Denormalization extends Morphism2(Vector("std", "stats"), "denormalize") with NormalizationHelper {
      override val idPolicy: IdentityPolicy = IdentityPolicy.Retain.Left

      def morph1Apply(summary: Result) = new Morph1Apply {

        def apply(table: Table, ctx: MorphContext): M[Table] = {
          def makeValue(info: RowValueWithStats): BigDecimal =
            (info.rowValue * info.stats.stdDev) + info.stats.mean

          applyMapper(summary, normMapper(makeValue), table, ctx)
        }
      }
    }
  }
}
