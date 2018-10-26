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

package quasar.yggdrasil

import quasar.ParseInstruction
import quasar.common.CPath
import quasar.precog.common._
import quasar.yggdrasil.vfs.ResourceError
import quasar.yggdrasil.bytecode.JType

import qdata.QDataDecode
import qdata.time.{DateTimeInterval, OffsetDate}

import java.nio.CharBuffer
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
import scala.collection.immutable.Set
import scala.concurrent.ExecutionContext

import cats.effect.{ContextShift, IO, LiftIO}
import scalaz._

// TODO: define better upper/lower bound methods, better comparisons,
// better names, better everything!
// TODO: investigate adding columns to TableSize

sealed trait TableSize {
  def maxSize: Long
  def +(other: TableSize): TableSize
  def *(other: TableSize): TableSize
}

object TableSize {
  def apply(size: Long): TableSize = ExactSize(size)
  def apply(minSize: Long, maxSize: Long): TableSize =
    if (minSize != maxSize) EstimateSize(minSize, maxSize) else ExactSize(minSize)
}

case class ExactSize(minSize: Long) extends TableSize {
  val maxSize = minSize

  def +(other: TableSize) = other match {
    case ExactSize(n)         => ExactSize(minSize + n)
    case EstimateSize(n1, n2) => EstimateSize(minSize + n1, minSize + n2)
    case UnknownSize          => UnknownSize
    case InfiniteSize         => InfiniteSize
  }

  def *(other: TableSize) = other match {
    case ExactSize(n)         => ExactSize(minSize * n)
    case EstimateSize(n1, n2) => EstimateSize(minSize * n1, minSize * n2)
    case UnknownSize          => UnknownSize
    case InfiniteSize         => InfiniteSize
  }
}

case class EstimateSize(minSize: Long, maxSize: Long) extends TableSize {
  def +(other: TableSize) = other match {
    case ExactSize(n)         => EstimateSize(minSize + n, maxSize + n)
    case EstimateSize(n1, n2) => EstimateSize(minSize + n1, maxSize + n2)
    case UnknownSize          => UnknownSize
    case InfiniteSize         => InfiniteSize
  }

  def *(other: TableSize) = other match {
    case ExactSize(n)         => EstimateSize(minSize * n, maxSize * n)
    case EstimateSize(n1, n2) => EstimateSize(minSize * n1, maxSize * n2)
    case UnknownSize          => UnknownSize
    case InfiniteSize         => InfiniteSize
  }
}

case object UnknownSize extends TableSize {
  val maxSize = Long.MaxValue
  def +(other: TableSize) = UnknownSize
  def *(other: TableSize) = UnknownSize
}

case object InfiniteSize extends TableSize {
  val maxSize = Long.MaxValue
  def +(other: TableSize) = InfiniteSize
  def *(other: TableSize) = InfiniteSize
}

object TableModule {
  val paths = TransSpecModule.paths

  sealed trait SortOrder
  sealed trait DesiredSortOrder extends SortOrder {
    def isAscending: Boolean
  }

  case object SortAscending  extends DesiredSortOrder { val isAscending = true }
  case object SortDescending extends DesiredSortOrder { val isAscending = false }

  sealed trait JoinOrder
  object JoinOrder {
    case object LeftOrder  extends JoinOrder
    case object RightOrder extends JoinOrder
    case object KeyOrder   extends JoinOrder
  }

  sealed trait CrossOrder
  object CrossOrder {
    case object CrossLeft      extends CrossOrder
    case object CrossRight     extends CrossOrder
    case object CrossLeftRight extends CrossOrder
    case object CrossRightLeft extends CrossOrder
  }
}

trait TableModule extends TransSpecModule {
  import TableModule._

  type Reducer[α]

  type Table <: TableLike
  type TableCompanion <: TableCompanionLike

  val Table: TableCompanion

  trait TableCompanionLike {
    import trans._
    def empty: Table

    def constString(v: Set[String]): Table
    def constLong(v: Set[Long]): Table
    def constDouble(v: Set[Double]): Table
    def constDecimal(v: Set[BigDecimal]): Table
    def constOffsetDateTime(v: Set[OffsetDateTime]): Table
    def constOffsetTime(v: Set[OffsetTime]): Table
    def constOffsetDate(v: Set[OffsetDate]): Table
    def constLocalDateTime(v: Set[LocalDateTime]): Table
    def constLocalTime(v: Set[LocalTime]): Table
    def constLocalDate(v: Set[LocalDate]): Table
    def constInterval(v: Set[DateTimeInterval]): Table
    def constBoolean(v: Set[Boolean]): Table
    def constNull: Table

    def constEmptyObject: Table
    def constEmptyArray: Table

    def fromRValues(values: Stream[RValue], maxSliceRows: Option[Int] = None): Table

    def fromQDataStream[M[_]: Monad: MonadFinalizers[?[_], IO]: LiftIO, A: QDataDecode](values: fs2.Stream[IO, A])(implicit cs: ContextShift[IO], ec: ExecutionContext): M[Table]

    def parseJson[M[_]: Monad: MonadFinalizers[?[_], IO]: LiftIO](
        bytes: fs2.Stream[IO, Byte],
        instructions: List[ParseInstruction],
        precise: Boolean = false)(
        implicit
        cs: ContextShift[IO],
        ec: ExecutionContext)
        : M[Table]

    def align(sourceLeft: Table, alignOnL: TransSpec1, sourceRight: Table, alignOnR: TransSpec1): IO[(Table, Table)]

    /**
      * Joins `left` and `right` together using their left/right key specs. The
      * final order of the resulting table is dependent on the implementation,
      * but must be a valid `JoinOrder`. This method should not assume any
      * particular order of the tables, unlike `cogroup`.
      */
    def join(left: Table, right: Table, orderHint: Option[JoinOrder] = None)(leftKeySpec: TransSpec1,
                                                                             rightKeySpec: TransSpec1,
                                                                             joinSpec: TransSpec2): IO[(JoinOrder, Table)]

    /**
      * Performs a back-end specific cross. Unlike Table#cross, this does not
      * guarantee a specific implementation (much like Table.join does not).
      * Hints can be provided on how we'd prefer the table to be crossed, but
      * the actual cross order is returned as part of the result.
      */
    def cross(left: Table, right: Table, orderHint: Option[CrossOrder] = None)(spec: TransSpec2): IO[(CrossOrder, Table)]
  }

  trait TableLike {
    this: Table =>
    import trans._
    import TransSpecModule._

    /**
      * Return an indication of table size, if known
      */
    def size: TableSize

    /**
      * For each distinct path in the table, load all columns identified by the specified
      * jtype and concatenate the resulting slices into a new table.
      */
    def load(tpe: JType): EitherT[IO, ResourceError, Table]

    /**
      * Folds over the table to produce a single value (stored in a singleton table).
      */
    def reduce[A: Monoid](reducer: Reducer[A]): IO[A]

    /**
      * Removes all rows in the table for which definedness is satisfied
      * Remaps the indicies.
      */
    def compact(spec: TransSpec1, definedness: Definedness = AnyDefined): Table

    /**
      * Performs a one-pass transformation of the keys and values in the table.
      * If the key transform is not identity, the resulting table will have
      * unknown sort order.
      */
    def transform(spec: TransSpec1): Table

    /**
      * Cogroups this table with another table, using equality on the specified
      * transformation on rows of the table.
      */
    def cogroup(leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(left: TransSpec1, right: TransSpec1, both: TransSpec2): Table

    /**
      * Performs a full cartesian cross on this table with the specified table,
      * applying the specified transformation to merge the two tables into
      * a single table.
      */
    def cross(that: Table)(spec: TransSpec2): Table

    /**
      * Performs a dimensional pivot on any array or object values at the given
      * focus.  We can view the focus as a form of lens: the structure at the
      * focus is pivoted, while everything *around* the focus is left untouched.
      * Usually, this results in data being duplicated, since the resulting
      * number of rows will be greater-than or equal-to the input number of rows,
      * provided that the focus does indeed refer to arrays/objects and those
      * structures are non-empty.
      */
    def leftShift(focus: CPath, emitOnUndef: Boolean): Table

    /**
      * Force the table to a backing store, and provice a restartable table
      * over the results.
      */
    def force: IO[Table]

    def paged(limit: Int): Table

    /**
      * Sorts the KV table by ascending or descending order of a transformation
      * applied to the rows.
      *
      * @param sortKey The transspec to use to obtain the values to sort on
      * @param sortOrder Whether to sort ascending or descending
      * @param unique If true, the same key values will sort into a single row, otherwise
      * we assign a unique row ID as part of the key so that multiple equal values are
      * preserved
      */
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder = SortAscending, unique: Boolean = false): IO[Table]

    def distinct(spec: TransSpec1): Table

    def concat(t2: Table): Table

    def zip(t2: Table): IO[Table]

    def toArray[A](implicit tpe: CValueType[A]): Table

    /**
      * Sorts the KV table by ascending or descending order based on a seq of transformations
      * applied to the rows.
      *
      * @param groupKeys The transspecs to use to obtain the values to sort on
      * @param valueSpec The transspec to use to obtain the non-sorting values
      * @param sortOrder Whether to sort ascending or descending
      * @param unique If true, the same key values will sort into a single row, otherwise
      * we assign a unique row ID as part of the key so that multiple equal values are
      * preserved
      */
    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder = SortAscending, unique: Boolean = false): IO[Seq[Table]]

    def partitionMerge(partitionBy: TransSpec1, keepKey: Boolean = false)(f: Table => IO[Table]): IO[Table]

    // @deprecated("use drop/take directly")
    def takeRange(startIndex: Long, numberToTake: Long): Table = {
      if (startIndex < 0 || numberToTake < 0)   // it's defined this way, for... reasons
        Table.empty
      else if (startIndex == 0)
        take(numberToTake)
      else if (numberToTake == Long.MaxValue)
        drop(startIndex)
      else
        drop(startIndex).take(numberToTake)
    }

    def drop(count: Long): Table

    def take(count: Long): Table

    def canonicalize(length: Int, maxLength0: Option[Int] = None): Table

    def schemas: IO[Set[JType]]

    def renderJson(prefix: String = "", delimiter: String = "\n", suffix: String = "", precise: Boolean = false): StreamT[IO, CharBuffer]

    def renderCsv(assumeHomogeneous: Boolean): StreamT[IO, CharBuffer]

    // for debugging only!!
    def toJson: IO[Iterable[RValue]]

    def printer(prelude: String = "", flag: String = ""): Table

    def metrics: TableMetrics
  }
}
