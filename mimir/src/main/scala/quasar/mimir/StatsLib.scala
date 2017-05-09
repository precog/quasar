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

import quasar.blueeyes._
import quasar.yggdrasil.bytecode._
import quasar.yggdrasil._, table._
import quasar.precog.common._
import quasar.precog.util._
import scalaz._, Scalaz._
import TableModule.SortAscending

trait StatsLibModule[M[+ _]] extends ColumnarTableLibModule[M] with ReductionLibModule[M] {
  //import library._
  import trans._

  trait StatsLib extends ColumnarTableLib with ReductionLib {
    import BigDecimalOperations._
    import TableModule.paths

    val StatsNamespace = Vector("std", "stats")
    val EmptyNamespace = Vector()

    override def _libMorphism1 = super._libMorphism1 ++ Set(Median, Mode, Rank, DenseRank, IndexedRank, Dummy)
    override def _libMorphism2 =
      super._libMorphism2 ++ Set(
        Covariance,
        LinearCorrelation,
        LinearRegression,
        LogarithmicRegression,
        SimpleExponentialSmoothing,
        DoubleExponentialSmoothing)

    object Median extends Morphism1(EmptyNamespace, "median") {
      val tpe = UnaryOperationType(JNumberT, JNumberT)

      def apply(table: Table, ctx: MorphContext) = { //TODO write tests for the empty table case
        val compactedTable = table.compact(WrapObject(Typed(DerefObjectStatic(Leaf(Source), paths.Value), JNumberT), paths.Value.name))

        val sortKey = DerefObjectStatic(Leaf(Source), paths.Value)

        for {
          sortedTable <- compactedTable.sort(sortKey, SortAscending)
          count <- sortedTable.reduce(Count.reducer(ctx))
          median <- if (count % 2 == 0) {
                     val middleValues     = sortedTable.takeRange((count.toLong / 2) - 1, 2)
                     val transformedTable = middleValues.transform(trans.DerefObjectStatic(Leaf(Source), paths.Value)) //todo make function for this
                     Mean(transformedTable, ctx)
                   } else {
                     val middleValue = M.point(sortedTable.takeRange((count.toLong / 2), 1))
                     middleValue map { _.transform(trans.DerefObjectStatic(Leaf(Source), paths.Value)) }
                   }
        } yield {
          val keyTable   = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))
          val valueTable = median.transform(trans.WrapObject(Leaf(Source), paths.Value.name))

          valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
        }
      }
    }

    object Mode extends Morphism1(EmptyNamespace, "mode") {

      type Result = Set[BigDecimal] //(currentRunValue, curentCount, listOfModes, maxCount)

      val tpe = UnaryOperationType(JNumberT, JNumberT)

      implicit def monoid = new Monoid[BigDecimal] {
        def zero                                           = BigDecimal(0)
        def append(left: BigDecimal, right: => BigDecimal) = left + right
      }

      implicit def setMonoid[A] = new Monoid[Set[A]] { //TODO this is WAY WRONG - it needs to deal with slice boundaries properly!!
        def zero                                   = Set.empty[A]
        def append(left: Set[A], right: => Set[A]) = left ++ right
      }

      def reducer(ctx: MorphContext): Reducer[Result] =
        new Reducer[Result] { //TODO add cases for other column types; get information necessary for dealing with slice boundaries and unsoretd slices in the Iterable[Slice] that's used in table.reduce
          def reduce(schema: CSchema, range: Range): Result = {
            schema.columns(JNumberT) flatMap {
              case col: LongColumn =>
                val mapped = range filter col.isDefinedAt map { x =>
                  col(x)
                }
                if (mapped.isEmpty) {
                  Set.empty[BigDecimal]
                } else {
                  val foldedMapped: (Option[BigDecimal], BigDecimal, Set[BigDecimal], BigDecimal) =
                    mapped.foldLeft(Option.empty[BigDecimal], BigDecimal(0), Set.empty[BigDecimal], BigDecimal(0)) {
                      case ((None, count, modes, maxCount), sv) => ((Some(sv), count + 1, Set(sv), maxCount + 1))
                      case ((Some(currentRun), count, modes, maxCount), sv) => {
                        if (currentRun == sv) {
                          if (count >= maxCount)
                            (Some(sv), count + 1, Set(sv), maxCount + 1)
                          else if (count + 1 == maxCount)
                            (Some(sv), count + 1, modes + BigDecimal(sv), maxCount)
                          else
                            (Some(sv), count + 1, modes, maxCount)
                        } else {
                          if (maxCount == 1)
                            (Some(sv), 1, modes + BigDecimal(sv), maxCount)
                          else
                            (Some(sv), 1, modes, maxCount)
                        }
                      }
                    }

                  val (_, _, result, _) = foldedMapped
                  result
                }

              case _ => Set.empty[BigDecimal]
            }
          }
        }

      def extract(res: Result): Table = Table.constDecimal(res)

      def apply(table: Table, ctx: MorphContext) = {
        val sortKey               = DerefObjectStatic(Leaf(Source), paths.Value)
        val sortedTable: M[Table] = table.sort(sortKey, SortAscending)

        sortedTable.flatMap(_.reduce(reducer(ctx)).map(extract))
      }
    }

    private def unifyNumColumns(cols: Iterable[Column]): NumColumn = {
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

    abstract class Smoothing(name: String) extends Morphism2(StatsNamespace, name) {
      protected val smoother = "by"
      protected val smoothee = "smooth"

      def alignment = MorphismAlignment.Match(M.point(morph1))

      val keySpec   = DerefObjectStatic(TransSpec1.Id, paths.Key)
      val sortSpec  = DerefObjectStatic(DerefArrayStatic(DerefObjectStatic(TransSpec1.Id, paths.Value), CPathIndex(0)), CPathField(smoother))
      val valueSpec = DerefObjectStatic(DerefArrayStatic(DerefObjectStatic(TransSpec1.Id, paths.Value), CPathIndex(0)), CPathField(smoothee))

      def smoothSpec: TransSpec1
      def spec = InnerObjectConcat(WrapObject(keySpec, paths.Key.name), WrapObject(smoothSpec, paths.Value.name))

      private val morph1 = new Morph1Apply {
        def apply(table: Table, ctx: MorphContext): M[Table] =
          for {
            sorted <- table.sort(sortSpec)
            smoothed <- sorted.transform(spec).sort(keySpec)
          } yield smoothed
      }
    }

    object SimpleExponentialSmoothing extends Smoothing("simpleExpSmoothing") {
      private val ValuePath = CPath(CPathIndex(0))
      private val AlphaPath = CPath(CPathIndex(1))

      val tpe = BinaryOperationType(JObjectFixedT(Map(smoother -> JType.JUniverseT, smoothee -> JNumberT)), JNumberT, JNumberT)

      object ExpSmoothingScanner extends CScanner {
        type A = Option[BigDecimal]
        def init = None

        @tailrec def findFirst(col: NumColumn, row: Int, end: Int): Option[BigDecimal] = {
          if (row < end) {
            if (col.isDefinedAt(row)) Some(col(row)) else findFirst(col, row + 1, end)
          } else {
            None
          }
        }

        def scan(init: Option[BigDecimal], cols: Map[ColumnRef, Column], range: Range): (Option[BigDecimal], Map[ColumnRef, Column]) = {
          val values = unifyNumColumns(cols.collect { case (ColumnRef(ValuePath, _), col) => col })
          val alphas = unifyNumColumns(cols.collect { case (ColumnRef(AlphaPath, _), col) => col })

          init orElse findFirst(values, range.start, range.end) map { init0 =>
            val smoothed = new Array[BigDecimal](range.end)
            val defined  = BitSetUtil.create()
            var row = range.start
            var s   = init0
            while (row < smoothed.length) {
              if (values.isDefinedAt(row) && alphas.isDefinedAt(row)) {
                defined.set(row)
                val x = values(row)
                val a = alphas(row)
                s = a * x + (BigDecimal(1) - a) * s
                smoothed(row) = s
              }
              row += 1
            }

            (Some(s), Map(ColumnRef(CPath.Identity, CNum) -> ArrayNumColumn(defined, smoothed)))
          } getOrElse {
            (None, Map.empty)
          }
        }
      }

      val alphaSpec = DerefArrayStatic(DerefObjectStatic(TransSpec1.Id, paths.Value), CPathIndex(1))
      def smoothSpec = Scan(InnerArrayConcat(WrapArray(valueSpec), WrapArray(alphaSpec)), ExpSmoothingScanner)
    }

    object DoubleExponentialSmoothing extends Smoothing("doubleExpSmoothing") {
      private val ValuePath = CPath(CPathIndex(0))
      private val AlphaPath = CPath(CPathIndex(1))
      private val BetaPath  = CPath(CPathIndex(2))
      private val alpha     = "alpha"
      private val beta      = "beta"

      val tpe = BinaryOperationType(
        JObjectFixedT(Map(smoother -> JType.JUniverseT, smoothee -> JNumberT)),
        JObjectFixedT(Map(alpha    -> JNumberT, beta             -> JNumberT)),
        JNumberT)

      private sealed trait ScannerState
      private case object FindFirst extends ScannerState
      private case class FindSecond(x0: BigDecimal)                                       extends ScannerState
      private case class Continue(s0: BigDecimal, b0: BigDecimal, first: Boolean = false) extends ScannerState

      private object DoubleExpSmoothingScanner extends CScanner {
        type A = ScannerState
        def init = FindFirst

        @tailrec def findFirst(col: NumColumn, row: Int, end: Int): Option[(Int, BigDecimal)] = {
          if (row < end) {
            if (col.isDefinedAt(row)) Some(row -> col(row)) else findFirst(col, row + 1, end)
          } else {
            None
          }
        }

        def scan(state0: ScannerState, cols: Map[ColumnRef, Column], range: Range): (ScannerState, Map[ColumnRef, Column]) = {
          val values = unifyNumColumns(cols.collect { case (ColumnRef(ValuePath, _), col) => col })
          val alphas = unifyNumColumns(cols.collect { case (ColumnRef(AlphaPath, _), col) => col })
          val betas  = unifyNumColumns(cols.collect { case (ColumnRef(BetaPath, _), col)  => col })

          // This is a bit messy, but it's because we need the first 2 values
          // in order to initialize s_1 and b_1. Since a slice may have 0 or
          // only 1 value, we need to account for those states. Moreover, the
          // single value case is really funky, as we still need to output the
          // smoothed value in that case, continue looking for the second value
          // in the next slice, but make sure we don't output the first value
          // in any subsequent slices.

          @tailrec def loop(state: ScannerState, rowM: Option[Int]): (ScannerState, Map[ColumnRef, Column]) = state match {
            case FindFirst =>
              findFirst(values, range.start, range.end) match {
                case Some((row, x0)) => loop(FindSecond(x0), Some(row + 1))
                case None            => (FindFirst, Map.empty)
              }

            case FindSecond(x0) =>
              findFirst(values, rowM.getOrElse(range.start), range.end) match {
                case Some((_, x1)) => loop(Continue(x0, x1 - x0, true), None)
                case None =>
                  rowM map { row =>
                    // In this case, we found a single value in this slice, but
                    // failed to find a second one. So, we still need to output
                    // this value in this slice, due to the contract of Scanner.

                    val defined  = BitSetUtil.create()
                    val smoothed = new Array[BigDecimal](range.end)
                    defined.set(row)
                    smoothed(row) = x0
                    (FindSecond(x0), Map(ColumnRef(CPath.Identity, CNum) -> ArrayNumColumn(defined, smoothed)))
                  } getOrElse {
                    (FindSecond(x0), Map.empty)
                  }
              }

            case Continue(s00, b00, first0) =>
              val smoothed = new Array[BigDecimal](range.end)
              val defined  = BitSetUtil.create()
              var row   = range.start
              var s0    = s00
              var b0    = b00
              var first = first0

              while (row < smoothed.length) {
                if (values.isDefinedAt(row) && alphas.isDefinedAt(row) && betas.isDefinedAt(row)) {
                  defined.set(row)
                  if (first) {
                    // I hope we can do better here eventually.
                    smoothed(row) = s0
                    first = false
                  } else {
                    val x  = values(row)
                    val a  = alphas(row)
                    val b  = betas(row)
                    val s1 = a * x + (BigDecimal(1) - a) * (s0 + b0)
                    b0 = b * (s1 - s0) + (BigDecimal(1) - b) * b0
                    s0 = s1
                    smoothed(row) = s0
                  }
                }
                row += 1
              }
              (Continue(s0, b0), Map(ColumnRef(CPath.Identity, CNum) -> ArrayNumColumn(defined, smoothed)))
          }

          loop(state0, None)
        }
      }

      val alphaSpec = DerefObjectStatic(DerefArrayStatic(DerefObjectStatic(TransSpec1.Id, paths.Value), CPathIndex(1)), CPathField(alpha))
      val betaSpec  = DerefObjectStatic(DerefArrayStatic(DerefObjectStatic(TransSpec1.Id, paths.Value), CPathIndex(1)), CPathField(beta))
      def smoothSpec = Scan(InnerArrayConcat(WrapArray(valueSpec), WrapArray(alphaSpec), WrapArray(betaSpec)), DoubleExpSmoothingScanner)
    }

    object LinearCorrelation extends Morphism2(StatsNamespace, "corr") {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)

      lazy val alignment = MorphismAlignment.Match(M.point(morph1))

      type InitialResult = (BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal) // (count, sum1, sum2, sumsq1, sumsq2, productSum)
      type Result        = Option[(BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal)]

      implicit def monoid = implicitly[Monoid[Result]]

      def reducer(ctx: MorphContext): Reducer[Result] = new Reducer[Result] {
        def reduce(schema: CSchema, range: Range): Result = {
          val left  = schema.columns(JArrayFixedT(Map(0 -> JNumberT)))
          val right = schema.columns(JArrayFixedT(Map(1 -> JNumberT)))

          val cross = for (l <- left; r <- right) yield (l, r)

          val result: Set[Result] = cross map {
            case (c1: LongColumn, c2: LongColumn) =>
              val mapped = range filter { r =>
                c1.isDefinedAt(r) && c2.isDefinedAt(r)
              } map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) =>
                    (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 * v2), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: NumColumn) =>
              val mapped = range filter { r =>
                c1.isDefinedAt(r) && c2.isDefinedAt(r)
              } map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) =>
                    (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 * v2), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: DoubleColumn) =>
              val mapped = range filter { r =>
                c1.isDefinedAt(r) && c2.isDefinedAt(r)
              } map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) =>
                    (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 * v2), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: LongColumn, c2: NumColumn) =>
              val mapped = range filter { r =>
                c1.isDefinedAt(r) && c2.isDefinedAt(r)
              } map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) =>
                    (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 * v2), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: LongColumn, c2: DoubleColumn) =>
              val mapped = range filter { r =>
                c1.isDefinedAt(r) && c2.isDefinedAt(r)
              } map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) =>
                    (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 * v2), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: LongColumn) =>
              val mapped = range filter { r =>
                c1.isDefinedAt(r) && c2.isDefinedAt(r)
              } map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) =>
                    (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 * v2), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: DoubleColumn) =>
              val mapped = range filter { r =>
                c1.isDefinedAt(r) && c2.isDefinedAt(r)
              } map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) =>
                    (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 * v2), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: LongColumn) =>
              val mapped = range filter { r =>
                c1.isDefinedAt(r) && c2.isDefinedAt(r)
              } map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) =>
                    (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 * v2), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: NumColumn) =>
              val mapped = range filter { r =>
                c1.isDefinedAt(r) && c2.isDefinedAt(r)
              } map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) =>
                    (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 * v2), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case _ => None
          }

          if (result.isEmpty) None
          else result.suml(monoid)
        }
      }

      def extract(res: Result): Table = {
        res filter (_._1 > 0) map {
          case (count, sum1, sum2, sumsq1, sumsq2, productSum) =>
            val unscaledVar1 = count * sumsq1 - sum1 * sum1
            val unscaledVar2 = count * sumsq2 - sum2 * sum2
            if (unscaledVar1 != 0 && unscaledVar2 != 0) {
              val cov         = (productSum - ((sum1 * sum2) / count)) / count
              val stdDev1     = sqrt(unscaledVar1) / count
              val stdDev2     = sqrt(unscaledVar2) / count
              val correlation = cov / (stdDev1 * stdDev2)

              val resultTable = Table.constDecimal(Set(correlation)) //TODO the following lines are used throughout. refactor!
              val valueTable  = resultTable.transform(trans.WrapObject(Leaf(Source), paths.Value.name))
              val keyTable    = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))

              valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
            } else {
              Table.empty
            }
        } getOrElse Table.empty
      }

      private val morph1 = new Morph1Apply {
        def apply(table: Table, ctx: MorphContext) = {
          val valueSpec = DerefObjectStatic(TransSpec1.Id, paths.Value)
          table.transform(valueSpec).reduce(reducer(ctx)) map extract
        }
      }
    }

    object Covariance extends Morphism2(StatsNamespace, "cov") {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)

      lazy val alignment = MorphismAlignment.Match(M.point(morph1))

      type InitialResult = (BigDecimal, BigDecimal, BigDecimal, BigDecimal) // (count, sum1, sum2, productSum)
      type Result        = Option[(BigDecimal, BigDecimal, BigDecimal, BigDecimal)]

      implicit def monoid = implicitly[Monoid[Result]]

      def reducer(ctx: MorphContext): Reducer[Result] = new Reducer[Result] {
        def reduce(schema: CSchema, range: Range): Result = {

          val left  = schema.columns(JArrayFixedT(Map(0 -> JNumberT)))
          val right = schema.columns(JArrayFixedT(Map(1 -> JNumberT)))

          val cross = for (l <- left; r <- right) yield (l, r)

          val result = cross map {
            case (c1: LongColumn, c2: LongColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: NumColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: DoubleColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: LongColumn, c2: DoubleColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: LongColumn, c2: NumColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: LongColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: DoubleColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: LongColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: NumColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }

            case _ => None
          }

          if (result.isEmpty) None
          else result.suml(monoid)
        }
      }

      def extract(res: Result): Table = {
        val res2 = res filter {
          case (count, _, _, _) => count != 0
        }

        res2 map {
          case (count, sum1, sum2, productSum) => {
            val cov = (productSum - ((sum1 * sum2) / count)) / count

            val resultTable = Table.constDecimal(Set(cov))
            val valueTable  = resultTable.transform(trans.WrapObject(Leaf(Source), paths.Value.name))
            val keyTable    = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))

            valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
          }
        } getOrElse Table.empty
      }

      private val morph1 = new Morph1Apply {
        def apply(table: Table, ctx: MorphContext) = {
          val valueSpec = DerefObjectStatic(TransSpec1.Id, paths.Value)
          table.transform(valueSpec).reduce(reducer(ctx)) map extract
        }
      }
    }

    object LinearRegression extends Morphism2(StatsNamespace, "linReg") {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)

      lazy val alignment = MorphismAlignment.Match(M.point(morph1))

      type InitialResult = (BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal) // (count, sum1, sum2, sumsq1, productSum)
      type Result        = Option[(BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal)]

      implicit def monoid = implicitly[Monoid[Result]]

      def reducer(ctx: MorphContext): Reducer[Result] = new Reducer[Result] {
        def reduce(schema: CSchema, range: Range): Result = {

          val left  = schema.columns(JArrayFixedT(Map(0 -> JNumberT)))
          val right = schema.columns(JArrayFixedT(Map(1 -> JNumberT)))

          val cross = for (l <- left; r <- right) yield (l, r)

          val result = cross map {
            case (c1: LongColumn, c2: LongColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: NumColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: DoubleColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: LongColumn, c2: DoubleColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: LongColumn, c2: NumColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: LongColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: NumColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: LongColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: DoubleColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }

            case _ => None
          }

          if (result.isEmpty) None
          else result.suml(monoid)
        }
      }

      def extract(res: Result): Table = {
        val res2 = res filter {
          case (count, _, _, _, _) => count != 0
        }

        res2 map {
          case (count, sum1, sum2, sumsq1, productSum) => {
            val cov  = (productSum - ((sum1 * sum2) / count)) / count
            val vari = (sumsq1 - (sum1 * (sum1 / count))) / count

            val slope = cov / vari
            val yint  = (sum2 / count) - (slope * (sum1 / count))

            val constSlope     = Table.constDecimal(Set(slope))
            val constIntercept = Table.constDecimal(Set(yint))

            val slopeSpec  = trans.WrapObject(Leaf(SourceLeft), "slope")
            val yintSpec   = trans.WrapObject(Leaf(SourceRight), "intercept")
            val concatSpec = trans.InnerObjectConcat(slopeSpec, yintSpec)

            val valueTable = constSlope.cross(constIntercept)(trans.WrapObject(concatSpec, paths.Value.name))
            val keyTable   = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))

            valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
          }
        } getOrElse Table.empty
      }

      private val morph1 = new Morph1Apply {
        def apply(table: Table, ctx: MorphContext) = {
          val valueSpec = DerefObjectStatic(TransSpec1.Id, paths.Value)
          table.transform(valueSpec).reduce(reducer(ctx)) map extract
        }
      }
    }

    object LogarithmicRegression extends Morphism2(StatsNamespace, "logReg") {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)

      lazy val alignment = MorphismAlignment.Match(M.point(morph1))

      type InitialResult = (BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal) // (count, sum1, sum2, sumsq1, productSum)
      type Result        = Option[(BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal)]

      implicit def monoid = implicitly[Monoid[Result]]

      def reducer(ctx: MorphContext): Reducer[Result] = new Reducer[Result] {
        def reduce(schema: CSchema, range: Range): Result = {

          val left  = schema.columns(JArrayFixedT(Map(0 -> JNumberT)))
          val right = schema.columns(JArrayFixedT(Map(1 -> JNumberT)))

          val cross = for (l <- left; r <- right) yield (l, r)

          val result = cross map {
            case (c1: LongColumn, c2: LongColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                    if (v1 > 0) {
                      (count + 1,
                       sum1 + math.log(v1.toDouble),
                       sum2 + v2,
                       sumsq1 + (math.log(v1.toDouble) * math.log(v1.toDouble)),
                       productSum + (math.log(v1.toDouble) * v2))
                    } else {
                      (count, sum1, sum2, sumsq1, productSum)
                    }
                  }
                }
                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: NumColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                    if (v1 > 0) {
                      (count + 1,
                       sum1 + math.log(v1.toDouble),
                       sum2 + v2,
                       sumsq1 + (math.log(v1.toDouble) * math.log(v1.toDouble)),
                       productSum + (math.log(v1.toDouble) * v2))
                    } else {
                      (count, sum1, sum2, sumsq1, productSum)
                    }
                  }
                }
                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: DoubleColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                    if (v1 > 0) {
                      (count + 1,
                       sum1 + math.log(v1.toDouble),
                       sum2 + v2,
                       sumsq1 + (math.log(v1.toDouble) * math.log(v1.toDouble)),
                       productSum + (math.log(v1.toDouble) * v2))
                    } else {
                      (count, sum1, sum2, sumsq1, productSum)
                    }
                  }
                }
                Some(foldedMapped)
              }
            case (c1: LongColumn, c2: DoubleColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                    if (v1 > 0) {
                      (count + 1,
                       sum1 + math.log(v1.toDouble),
                       sum2 + v2,
                       sumsq1 + (math.log(v1.toDouble) * math.log(v1.toDouble)),
                       productSum + (math.log(v1.toDouble) * v2))
                    } else {
                      (count, sum1, sum2, sumsq1, productSum)
                    }
                  }
                }
                Some(foldedMapped)
              }
            case (c1: LongColumn, c2: NumColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                    if (v1 > 0) {
                      (count + 1,
                       sum1 + math.log(v1.toDouble),
                       sum2 + v2,
                       sumsq1 + (math.log(v1.toDouble) * math.log(v1.toDouble)),
                       productSum + (math.log(v1.toDouble) * v2))
                    } else {
                      (count, sum1, sum2, sumsq1, productSum)
                    }
                  }
                }
                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: LongColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                    if (v1 > 0) {
                      (count + 1,
                       sum1 + math.log(v1.toDouble),
                       sum2 + v2,
                       sumsq1 + (math.log(v1.toDouble) * math.log(v1.toDouble)),
                       productSum + (math.log(v1.toDouble) * v2))
                    } else {
                      (count, sum1, sum2, sumsq1, productSum)
                    }
                  }
                }
                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: NumColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                    if (v1 > 0) {
                      (count + 1,
                       sum1 + math.log(v1.toDouble),
                       sum2 + v2,
                       sumsq1 + (math.log(v1.toDouble) * math.log(v1.toDouble)),
                       productSum + (math.log(v1.toDouble) * v2))
                    } else {
                      (count, sum1, sum2, sumsq1, productSum)
                    }
                  }
                }
                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: LongColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                    if (v1 > 0) {
                      (count + 1,
                       sum1 + math.log(v1.toDouble),
                       sum2 + v2,
                       sumsq1 + (math.log(v1.toDouble) * math.log(v1.toDouble)),
                       productSum + (math.log(v1.toDouble) * v2))
                    } else {
                      (count, sum1, sum2, sumsq1, productSum)
                    }
                  }
                }
                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: DoubleColumn) =>
              val mapped = range filter (r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i =>
                (c1(i), c2(i))
              }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                    if (v1 > 0) {
                      (count + 1,
                       sum1 + math.log(v1.toDouble),
                       sum2 + v2,
                       sumsq1 + (math.log(v1.toDouble) * math.log(v1.toDouble)),
                       productSum + (math.log(v1.toDouble) * v2))
                    } else {
                      (count, sum1, sum2, sumsq1, productSum)
                    }
                  }
                }
                Some(foldedMapped)
              }

            case _ => None
          }

          if (result.isEmpty) None
          else result.suml(monoid)
        }
      }

      def extract(res: Result): Table = {
        val res2 = res filter {
          case (count, _, _, _, _) => count != 0
        }

        res2 map {
          case (count, sum1, sum2, sumsq1, productSum) => {
            val cov  = (productSum - ((sum1 * sum2) / count)) / count
            val vari = (sumsq1 - (sum1 * (sum1 / count))) / count

            val slope = cov / vari
            val yint  = (sum2 / count) - (slope * (sum1 / count))

            val constSlope     = Table.constDecimal(Set(slope))
            val constIntercept = Table.constDecimal(Set(yint))

            val slopeSpec  = trans.WrapObject(Leaf(SourceLeft), "slope")
            val yintSpec   = trans.WrapObject(Leaf(SourceRight), "intercept")
            val concatSpec = trans.InnerObjectConcat(slopeSpec, yintSpec)

            val valueTable = constSlope.cross(constIntercept)(trans.WrapObject(concatSpec, paths.Value.name))
            val keyTable   = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))

            valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
          }
        } getOrElse Table.empty
      }

      private val morph1 = new Morph1Apply {
        def apply(table: Table, ctx: MorphContext) = {
          val valueSpec = DerefObjectStatic(TransSpec1.Id, paths.Value)
          table.transform(valueSpec).reduce(reducer(ctx)) map extract
        }
      }
    }

    /**
      * Base trait for all Rank-scanners.
      *
      * This provides scaffolding that is useful in all cases.
      */
    trait BaseRankScanner extends CScanner {
      import scala.collection.mutable

      // collapses number columns into one decimal column.
      //
      // TODO: it would be nice to avoid doing this, but its not clear that
      // decimals are going to be a significant performance problem for rank
      // (compared to sorting) and this simplifies the algorithm a lot.
      protected def decimalize(m: Map[ColumnRef, Column], r: Range): Map[ColumnRef, Column] = {
        val m2   = scmMap[ColumnRef, Column]()
        val nums = scmMap[CPath, List[Column]]()

        m.foreach {
          case (ref @ ColumnRef(path, ctype), col) =>
            if (ctype == CLong || ctype == CDouble || ctype == CNum) {
              nums(path) = col :: nums.getOrElse(path, Nil)
            } else {
              m2(ref) = col
            }
        }

        val start = r.start
        val end   = r.end
        val len   = r.size

        nums.foreach {
          case (path, cols) =>
            val bs  = new BitSet()
            val arr = new Array[BigDecimal](len)
            cols.foreach {
              case col: LongColumn =>
                val bs2 = col.definedAt(start, end)
                Loop.range(0, len)(j => if (bs2.get(j)) arr(j) = BigDecimal(col(j + start)))
                bs.or(bs2)
              case col: DoubleColumn =>
                val bs2 = col.definedAt(start, end)
                Loop.range(0, len)(j => if (bs2.get(j)) arr(j) = BigDecimal(col(j + start)))
                bs.or(bs2)
              case col: NumColumn =>
                val bs2 = col.definedAt(start, end)
                Loop.range(0, r.size)(j => if (bs2.get(j)) arr(j) = col(j + start))
                bs.or(bs2)
              case col =>
                sys.error("unexpected column found: %s" format col)
            }

            m2(ColumnRef(path, CNum)) = shiftColumn(ArrayNumColumn(bs, arr), start)
        }
        m2.toMap
      }

      /**
        * Represents the state of the scanner at the end of a slice.
        *
        * The n is the last rank number used (-1 means we haven't started).
        * The iterable items are the refs/cvalues defined by that row (which
        * will only be empty when we haven't started).
        */
      case class RankContext(curr: Long, next: Long, items: Iterable[(ColumnRef, CValue)])

      type A = RankContext

      def init = RankContext(-1L, 0L, Nil)

      /**
        * Builds a bitset for each column we were given. Each bitset contains
        * (end-start) boolean values.
        */
      def initDefinedCols(cols: Array[Column], r: Range): Array[BitSet] = {
        val start = r.start
        val end   = r.end
        val ncols = cols.length
        val arr   = new Array[BitSet](ncols)
        var i = 0
        while (i < ncols) { arr(i) = cols(i).definedAt(start, end); i += 1 }
        arr
      }

      /**
        * Builds a bitset with a boolean value for each row. This value for a row
        * will be true if at least one column is defined for that row and false
        * otherwise.
        */
      def initDefined(definedCols: Array[BitSet]): BitSet = {
        val ncols = definedCols.length
        if (ncols == 0) return new BitSet()
        val arr = definedCols(0).copy()
        var i = 1
        while (i < ncols) { arr.or(definedCols(i)); i += 1 }
        arr
      }

      def findFirstDefined(defined: BitSet, r: Range): Int = {
        var row = r.start
        val end = r.end
        while (row < end && !defined.get(row)) row += 1
        row
      }

      def buildRankContext(m: Map[ColumnRef, Column], lastRow: Int, curr: Long, next: Long): RankContext = {
        val items = m.filter {
          case (k, v) => v.isDefinedAt(lastRow)
        }.map {
          case (k, v) => (k, v.cValue(lastRow))
        }
        RankContext(curr, next, items)
      }

      // TODO: seems like shifting shouldn't need to return an Option.
      def shiftColumn(col: Column, start: Int): Column =
        if (start == 0) col else (col |> cf.util.Shift(start)).get
    }

    /**
      * This class works for the indexed rank case (where we are not worried
      * about row uniqueness). It is much faster than the traits based on
      * UniqueRankScanner.
      */
    class IndexedRankScanner extends BaseRankScanner {
      def buildRankArrayIndexed(defined: BitSet, r: Range, ctxt: RankContext): (Array[Long], Long, Int) = {
        var curr = ctxt.next

        val start = r.start
        val end   = r.end
        val len   = end - start
        var i = 0
        val values = new Array[Long](len)

        var lastRow = -1
        while (i < len) {
          if (defined.get(i)) {
            lastRow = i
            values(i) = curr
            curr += 1L
          }
          i += 1
        }

        (values, curr, lastRow)
      }

      /**
        *
        */
      def scan(ctxt: RankContext, _m: Map[ColumnRef, Column], range: Range): (RankContext, Map[ColumnRef, Column]) = {

        val m = decimalize(_m, range)

        val start = range.start
        val end   = range.end
        val len   = end - start

        val cols = m.values.toArray

        // for each column, store its definedness bitset for later use
        val definedCols = initDefinedCols(cols, range)

        // find the union of column-definedness. any row in defined that is zero
        // after this is a row that is totally undefined.
        val defined = initDefined(definedCols)

        // find the first defined row
        val row = findFirstDefined(defined, range)

        // if none of our rows are defined let's short-circuit out of here!
        val back = if (row == end) {
          (ctxt, Map.empty[ColumnRef, Column])
        } else {
          // build the actual rank array
          val (values, curr, lastRow) = buildRankArrayIndexed(defined, range, ctxt)

          // build the context to be used for the next slice
          val ctxt2 = buildRankContext(m, lastRow, curr, curr + 1L)

          // construct the column ref and column to return
          val col2: Column = shiftColumn(ArrayLongColumn(defined, values), start)
          val data         = Map(ColumnRef(CPath.Identity, CLong) -> col2)

          (ctxt2, data)
        }

        back
      }
    }

    /**
      * This trait works for cases where the rank should be shared for adjacent
      * rows that are identical.
      */
    trait UniqueRankScanner extends BaseRankScanner {
      import scala.collection.mutable

      /**
        * Determines whether row is a duplicate of lastRow.
        * The method returns true if the two rows differ, e.g.:
        *
        * 1. There is a column that is defined for one row but not another.
        * 2. There is a column that is defined for both but has different values.
        *
        * If we make it through all the columns without either of those being
        * true then we can return false, since the rows are not duplicates.
        */
      @tailrec
      private def isDuplicate(cols: Array[Column], definedCols: Array[BitSet], lastRow: Int, row: Int, index: Int): Boolean = {
        if (index >= cols.length) {
          true
        } else {
          val dc         = definedCols(index)
          val wasDefined = dc.get(lastRow)
          if (wasDefined != dc.get(row)) {
            false
          } else if (!wasDefined || cols(index).rowEq(lastRow, row)) {
            isDuplicate(cols, definedCols, lastRow, row, index + 1)
          } else {
            false
          }
        }
      }

      def findDuplicates(defined: BitSet, definedCols: Array[BitSet], cols: Array[Column], r: Range, _row: Int): (BitSet, Int) = {
        val start = r.start
        val end   = r.end
        val len   = end - start
        var row = _row

        // start assuming all rows are dupes until we find out otherwise
        val duplicateRows = new BitSet()
        duplicateRows.setBits(Array.fill(((len - 1) >> 6) + 1)(-1L))

        // FIXME: for now, assume first row is valid
        duplicateRows.clear(row - start)
        var lastRow = row
        row += 1

        // compare each subsequent row against the last valid row
        while (row < end) {
          if (defined.get(row) && !isDuplicate(cols, definedCols, lastRow, row, 0)) {
            duplicateRows.clear(row - start)
            lastRow = row
          }
          row += 1
        }

        (duplicateRows, lastRow)
      }

      def buildRankArrayUnique(defined: BitSet, duplicateRows: BitSet, r: Range, ctxt: RankContext): (Array[Long], Long, Long)

      /**
        *
        */
      def scan(ctxt: RankContext, _m: Map[ColumnRef, Column], range: Range): (RankContext, Map[ColumnRef, Column]) = {
        val m = decimalize(_m, range)

        val start = range.start
        val end   = range.end
        val len   = end - start

        val cols = m.values.toArray

        // for each column, store its definedness bitset for later use
        val definedCols = initDefinedCols(cols, range)

        // find the union of column-definedness. any row in defined that is zero
        // after this is a row that is totally undefined.
        val defined = initDefined(definedCols)

        // find the first defined row
        val row = findFirstDefined(defined, range)

        // if none of our rows are defined let's short-circuit out of here!
        val back = if (row == end) {
          (ctxt, Map.empty[ColumnRef, Column])
        } else {
          // find a bitset of duplicate rows and the last defined row
          val (duplicateRows, lastRow) = findDuplicates(defined, definedCols, cols, range, row)

          // build the actual rank array
          val (values, curr, next) = buildRankArrayUnique(defined, duplicateRows, range, ctxt)

          // build the context to be used for the next slice
          val ctxt2 = buildRankContext(m, lastRow, curr, next)

          // construct the column ref and column to return
          val col2 = shiftColumn(ArrayLongColumn(defined, values), start)
          val data = Map(ColumnRef(CPath.Identity, CLong) -> col2)

          (ctxt2, data)
        }

        back
      }
    }

    class SparseRankScaner extends UniqueRankScanner {
      def buildRankArrayUnique(defined: BitSet, duplicateRows: BitSet, r: Range, ctxt: RankContext): (Array[Long], Long, Long) = {
        var curr = ctxt.curr
        var next = ctxt.next

        val start = r.start
        val end   = r.end
        val len   = end - start
        var i = 0
        val values = new Array[Long](len)

        while (i < len) {
          if (defined.get(i)) {
            if (!duplicateRows.get(i)) curr = next
            values(i) = curr
            next += 1L
          }
          i += 1
        }

        (values, curr, next)
      }
    }

    class DenseRankScaner extends UniqueRankScanner {
      def buildRankArrayUnique(defined: BitSet, duplicateRows: BitSet, r: Range, ctxt: RankContext): (Array[Long], Long, Long) = {
        var curr = ctxt.curr

        val start = r.start
        val end   = r.end
        val len   = end - start
        var i = 0
        val values = new Array[Long](len)

        while (i < len) {
          if (defined.get(i)) {
            if (!duplicateRows.get(i)) curr += 1L
            values(i) = curr
          }
          i += 1
        }

        (values, curr, curr + 1L)
      }
    }

    final class DummyScanner(length: Long) extends CScanner {
      type A = Long
      def init: A = 0L

      def scan(a: A, cols: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column]) = {
        val defined = cols.values.foldLeft(BitSetUtil.create()) { (bitset, col) =>
          bitset.or(col.definedAt(range.start, range.end))
          bitset
        }

        val indices = new Array[Long](range.size)
        var i       = 0
        var n: Long = a
        while (i < indices.length) {
          if (defined(i)) {
            indices(i) = n
            n += 1
          } else {
            indices(i) = -1
          }
          i += 1
        }

        def makeColumn(idx: Int) = new LongColumn {
          def isDefinedAt(row: Int) = defined(row)
          def apply(row: Int)       = if (indices(row) == idx) 1L else 0L
        }

        val cols0: Map[ColumnRef, Column] = (0 until length.toInt).map({ idx =>
          ColumnRef(CPath(CPathIndex(idx)), CLong) -> makeColumn(idx)
        })(collection.breakOut)

        (n, cols0)
      }
    }

    abstract class BaseRank(name: String, retType: JType = JNumberT) extends Morphism1(StatsNamespace, name) {
      val tpe               = UnaryOperationType(JType.JUniverseT, retType)
      override val idPolicy = IdentityPolicy.Retain.Merge

      def rankScanner(size: Long): CScanner

      private val sortByValue = DerefObjectStatic(Leaf(Source), paths.Value)
      private val sortByKey   = DerefObjectStatic(Leaf(Source), paths.Key)

      // TODO: sorting the data twice is kind of awful.
      //
      // it would be better to let our consumers worry about whether the table is
      // sorted on paths.SortKey.

      def apply(table: Table, ctx: MorphContext): M[Table] = {
        def count(tbl: Table): M[Long] = tbl.size match {
          case ExactSize(n) => M.point(n)
          case _            => table.reduce(Count.reducer(ctx))
        }

        for {
          valueSortedTable <- table.sort(sortByValue, SortAscending)
          size <- count(valueSortedTable)
          rankedTable = {
            val m    = Map(paths.Value -> Scan(Leaf(Source), rankScanner(size)))
            val scan = makeTableTrans(m)
            valueSortedTable.transform(ObjectDelete(scan, Set(paths.SortKey)))
          }
          keySortedTable <- rankedTable.sort(sortByKey, SortAscending)
        } yield keySortedTable
      }
    }

    object DenseRank extends BaseRank("denseRank") {
      def rankScanner(size: Long) = new DenseRankScaner
    }

    object Rank extends BaseRank("rank") {
      def rankScanner(size: Long) = new SparseRankScaner
    }

    object IndexedRank extends BaseRank("indexedRank") {
      def rankScanner(size: Long) = new IndexedRankScanner
    }

    object Dummy extends BaseRank("dummy", JArrayUnfixedT) {
      def rankScanner(size: Long) = new DummyScanner(size)
    }
  }
}
