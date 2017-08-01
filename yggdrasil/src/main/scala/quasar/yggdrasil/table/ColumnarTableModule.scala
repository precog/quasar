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

import quasar.blueeyes._, json._
import quasar.precog.{BitSet, MimeType, MimeTypes}
import quasar.precog.common._
import quasar.precog.common.ingest.FileContent
import quasar.precog.util.RawBitSet
import quasar.yggdrasil.bytecode._
import quasar.yggdrasil.util._
import quasar.yggdrasil.table.cf.util.{ Remap, Empty }

import TransSpecModule._
import org.slf4j.Logger
import org.slf4s.Logging
import quasar.precog.util.IOUtils
import scalaz._, Scalaz._, Ordering._

import java.io.File
import java.nio.CharBuffer
import java.time.ZonedDateTime

import scala.annotation.tailrec
import scala.collection.mutable

trait ColumnarTableTypes[M[+ _]] {
  type F1         = CF1
  type F2         = CF2
  type FN         = CFN
  type Scanner    = CScanner
  type Mapper     = CMapper[M]
  type Reducer[α] = CReducer[α]
  type RowId      = Int
}

trait ColumnarTableModuleConfig {
  def maxSliceSize: Int

  // This is a slice size that we'd like our slices to be at least as large as.
  def minIdealSliceSize: Int = maxSliceSize / 4

  // This is what we consider a "small" slice. This may affect points where
  // we take proactive measures to prevent problems caused by small slices.
  def smallSliceSize: Int

  def maxSaneCrossSize: Long = 2400000000L // 2.4 billion
}

object ColumnarTableModule extends Logging {
  def renderJson[M[+ _]](slices: StreamT[M, Slice], prefix: String, delimiter: String, suffix: String)(implicit M: Monad[M]): StreamT[M, CharBuffer] = {
    def wrap(stream: StreamT[M, CharBuffer]) = {
      if (prefix == "" && suffix == "") stream
      else if (suffix == "") CharBuffer.wrap(prefix) :: stream
      else if (prefix == "") stream ++ (CharBuffer.wrap(suffix) :: StreamT.empty[M, CharBuffer])
      else CharBuffer.wrap(prefix) :: (stream ++ (CharBuffer.wrap(suffix) :: StreamT.empty[M, CharBuffer]))
    }

    def foldFlatMap(slices: StreamT[M, Slice], rendered: Boolean): StreamT[M, CharBuffer] = {
      StreamT[M, CharBuffer](slices.step map {
        case StreamT.Yield(slice, tail) =>
          val (stream, rendered2) = slice.renderJson[M](delimiter)
          val stream2             = if (rendered && rendered2) CharBuffer.wrap(delimiter) :: stream else stream

          StreamT.Skip(stream2 ++ foldFlatMap(tail(), rendered || rendered2))

        case StreamT.Skip(tail) =>
          StreamT.Skip(foldFlatMap(tail(), rendered))

        case StreamT.Done =>
          StreamT.Done
      })
    }

    wrap(foldFlatMap(slices, false))
  }

  /**
    * This method renders the entire table into a single string,
    * encoded as CSV.
    *
    * In the future we may want something Stream-based, but for now
    * the method seems to be "fast enough" for our purposes.
    *
    * The column headers are currently stringified CPaths. These are
    * introduced introduced slice-by-slice in alphabetical order. So
    * if there is one slice, the headers will be totally
    * alphabetical. If two slices, the alphabetized headers from the
    * first slice are first, and then the other headers (also
    * alphabetized). And so on.
    *
    * The escaping here should match Microsoft's:
    *
    * If a value contains commas, double-quotes, or CR/LF, it will be
    * escaped. To escape a value, it is wrapped in double quotes. Any
    * double-quotes in the value are themselves doubled. So:
    *
    * the fox said: "hello, my name is fred."
    *
    * becomes:
    *
    * "the fox said: ""hello, my name is fred."""
    */
  def renderCsv[M[+ _]](slices: StreamT[M, Slice])(implicit M: Monad[M]): StreamT[M, CharBuffer] = {
    import scala.collection.{ Map => GenMap }

    /**
      * Represents the column headers we have. We track three things:
      *
      *  1. n: the number of headers so far.
      *  2. m: a map from path strings to header position
      *  3. a: an array of path strings used.
      *
      * The class is immutable so as we find new headers we'll create
      * new instances. If this proves to be a problem we could easily
      * make a mutable version.
      */
    class Indices(n: Int, m: GenMap[String, Int], a: Array[String]) {
      def size                        = n
      def getPaths: Array[String]     = a
      def columnForPath(path: String) = m(path)
      def combine(that: Indices): Indices = {
        val buf = new mutable.ArrayBuffer[String](a.length)
        buf ++= a
        that.getPaths.foreach(p => if (!m.contains(p)) buf.append(p))
        Indices.fromPaths(buf.toArray)
      }
      override def equals(that: Any): Boolean = that match {
        case that: Indices =>
          val len = n
          if (len != that.size) return false
          var i = 0
          val paths = that.getPaths
          while (i < len) {
            if (a(i) != paths(i)) return false
            i += 1
          }
          true
        case _ =>
          false
      }
      def writeToBuilder(sb: StringBuilder): Unit = {
        if (n == 0) return ()
        sb.append(a(0))
        var i = 1
        val len = n
        while (i < len) { sb.append(','); sb.append(a(i)); i += 1 }
        sb.append("\r\n")
      }
    }

    object Indices {
      def empty: Indices = new Indices(0, Map.empty[String, Int], new Array[String](0))

      def fromPaths(ps: Array[String]): Indices = {
        val paths = ps.sorted
        val m = mutable.Map[String, Int]()
        var i = 0
        val len = paths.length
        while (i < len) { m(paths(i)) = i; i += 1 }
        new Indices(len, m, paths)
      }
    }

    // these methods will quote CSV values for us
    // they could probably be a bit faster but are OK so far.
    def quoteIfNeeded(s: String): String = if (needsQuoting(s)) quote(s) else s
    def quote(s: String): String         = "\"" + s.replace("\"", "\"\"") + "\""
    def needsQuoting(s: String): Boolean = {
      var i = 0
      while (i < s.length) {
        val c = s.charAt(i)
        if (c == ',' || c == '"' || c == '\r' || c == '\n') return true
        i += 1
      }
      false
    }

    /**
      * Render a particular column of a slice into an array of
      * Strings, handling any escaping that is needed.
      */
    def renderColumn(col: Column, rows: Int): Array[String] = {
      val arr = new Array[String](rows)
      var row = 0
      while (row < rows) {
        arr(row) =
          if (col.isDefinedAt(row))
            quoteIfNeeded(col.strValue(row))
          else
            ""
        row += 1
      }
      arr
    }

    /**
      * Generate indices for this slice.
      */
    def indicesForSlice(slice: Slice): Indices =
      Indices.fromPaths(slice.columns.keys.map(_.selector.toString).toArray)

    /**
      * Renders a slice into an array of lines, as well as updating
      * our Indices with any previous unseen paths.
      *
      * Since slice's underlying data is column-oriented, we evaluate
      * each column individually, building an array of values. Then
      * we stride across these arrays building our rows (Line
      * objects).
      *
      * Since we know in advance how many rows we have, we can return
      * an array of lines.
      */
    def renderSlice(pastIndices: Option[Indices], slice: Slice): (Indices, CharBuffer) = {

      val indices = indicesForSlice(slice)
      val height  = slice.size
      val width   = indices.size

      if (width == 0) return (indices, CharBuffer.allocate(0))

      val items = slice.columns.toArray
      val ncols = items.length

      // load each column into strings
      val columns = items.map {
        case (_, col) =>
          renderColumn(col, height)
      }
      val positions = items.map {
        case (ColumnRef(path, _), _) =>
          indices.columnForPath(path.toString)
      }

      val sb = new StringBuilder()

      pastIndices match {
        case None => indices.writeToBuilder(sb)
        case Some(ind) =>
          if (ind != indices) {
            sb.append("\r\n")
            indices.writeToBuilder(sb)
          }
      }

      var row = 0
      while (row < height) {
        // fill in all the buckets for this particular row
        val buckets = Array.fill(width)("")
        var i = 0
        while (i < ncols) {
          val s = columns(i)(row)
          if (s != "") buckets(positions(i)) = s
          i += 1
        }

        // having filled the buckets, add them to the string builder
        sb.append(buckets(0))
        i = 1
        while (i < width) {
          sb.append(',')
          sb.append(buckets(i))
          i += 1
        }
        sb.append("\r\n")

        row += 1
      }
      (indices, CharBuffer.wrap(sb))
    }

    StreamT.unfoldM(slices -> none[Indices]) {
      case (stream, pastIndices) =>
        stream.uncons.map {
          case Some((slice, tail)) =>
            val (indices, cb) = renderSlice(pastIndices, slice)
            some(cb -> (tail -> some(indices)))
          case None =>
            none
        }
    }
  }

  def toCharBuffers[N[+ _]: Monad](output: MimeType, slices: StreamT[N, Slice]): StreamT[N, CharBuffer] = {
    import FileContent._
    import MimeTypes._
    val AnyMimeType = anymaintype / anysubtype

    output match {
      case ApplicationJson | AnyMimeType => ColumnarTableModule.renderJson(slices, "[", ",", "]")
      case XJsonStream                   => ColumnarTableModule.renderJson(slices, "", "\n", "")
      case TextCSV                       => ColumnarTableModule.renderCsv(slices)
      case other                         =>
        log.warn("Unrecognized output type requested for conversion of slice stream to char buffers: %s".format(output))
        StreamT.empty[N, CharBuffer]
    }
  }
}

trait ColumnarTableModule[M[+ _]]
    extends TableModule[M]
    with ColumnarTableTypes[M]
    with IdSourceScannerModule
    with SliceTransforms[M]
    with SamplableColumnarTableModule[M]
    with IndicesModule[M] {

  import TableModule._
  import trans._

  type Table <: ColumnarTable
  type TableCompanion <: ColumnarTableCompanion
  case class TableMetrics(startCount: Int, sliceTraversedCount: Int)

  implicit def M: Monad[M]

  def newScratchDir(): File    = IOUtils.createTmpDir("ctmscratch").unsafePerformIO
  def jdbmCommitInterval: Long = 200000l

  implicit def liftF1(f: F1) = new F1Like {
    def compose(f1: F1) = f compose f1
    def andThen(f1: F1) = f andThen f1
  }

  implicit def liftF2(f: F2) = new F2Like {
    def applyl(cv: CValue) = CF1("builtin::liftF2::applyl") { f(Column.const(cv), _) }
    def applyr(cv: CValue) = CF1("builtin::liftF2::applyl") { f(_, Column.const(cv)) }

    def andThen(f1: F1) = CF2("builtin::liftF2::andThen") { (c1, c2) =>
      f(c1, c2) flatMap f1.apply
    }
  }

  trait ColumnarTableCompanion extends TableCompanionLike {
    def apply(slices: StreamT[M, Slice], size: TableSize): Table

    def singleton(slice: Slice): Table

    implicit def groupIdShow: Show[GroupId] = Show.showFromToString[GroupId]

    def empty: Table = Table(StreamT.empty[M, Slice], ExactSize(0))

    def uniformDistribution(init: MmixPrng): Table = {
      val gen: StreamT[M, Slice] = StreamT.unfoldM[M, Slice, MmixPrng](init) { prng =>
        val (column, nextGen) = Column.uniformDistribution(prng)
        Some((Slice(Map(ColumnRef(CPath.Identity, CDouble) -> column), yggConfig.maxSliceSize), nextGen)).point[M]
      }

      Table(gen, InfiniteSize)
    }

    def constBoolean(v: collection.Set[Boolean]): Table = {
      val column = ArrayBoolColumn(v.toArray)
      Table(Slice(Map(ColumnRef(CPath.Identity, CBoolean) -> column), v.size) :: StreamT.empty[M, Slice], ExactSize(v.size))
    }

    def constLong(v: collection.Set[Long]): Table = {
      val column = ArrayLongColumn(v.toArray)
      Table(Slice(Map(ColumnRef(CPath.Identity, CLong) -> column), v.size) :: StreamT.empty[M, Slice], ExactSize(v.size))
    }

    def constDouble(v: collection.Set[Double]): Table = {
      val column = ArrayDoubleColumn(v.toArray)
      Table(Slice(Map(ColumnRef(CPath.Identity, CDouble) -> column), v.size) :: StreamT.empty[M, Slice], ExactSize(v.size))
    }

    def constDecimal(v: collection.Set[BigDecimal]): Table = {
      val column = ArrayNumColumn(v.toArray)
      Table(Slice(Map(ColumnRef(CPath.Identity, CNum) -> column), v.size) :: StreamT.empty[M, Slice], ExactSize(v.size))
    }

    def constString(v: collection.Set[String]): Table = {
      val column = ArrayStrColumn(v.toArray)
      Table(Slice(Map(ColumnRef(CPath.Identity, CString) -> column), v.size) :: StreamT.empty[M, Slice], ExactSize(v.size))
    }

    def constDate(v: collection.Set[ZonedDateTime]): Table = {
      val column = ArrayDateColumn(v.toArray)
      Table(Slice(Map(ColumnRef(CPath.Identity, CDate) -> column), v.size) :: StreamT.empty[M, Slice], ExactSize(v.size))
    }

    def constNull: Table =
      Table(Slice(Map(ColumnRef(CPath.Identity, CNull) -> new InfiniteColumn with NullColumn), 1) :: StreamT.empty[M, Slice], ExactSize(1))

    def constEmptyObject: Table =
      Table(Slice(Map(ColumnRef(CPath.Identity, CEmptyObject) -> new InfiniteColumn with EmptyObjectColumn), 1) :: StreamT.empty[M, Slice], ExactSize(1))

    def constEmptyArray: Table =
      Table(Slice(Map(ColumnRef(CPath.Identity, CEmptyArray) -> new InfiniteColumn with EmptyArrayColumn), 1) :: StreamT.empty[M, Slice], ExactSize(1))

    def transformStream[A](sliceTransform: SliceTransform1[A], slices: StreamT[M, Slice]): StreamT[M, Slice] = {
      def stream(state: A, slices: StreamT[M, Slice]): StreamT[M, Slice] = StreamT(
        for {
          head <- slices.uncons

          back <- {
            head map {
              case (s, sx) => {
                sliceTransform.f(state, s) map {
                  case (nextState, s0) =>
                    StreamT.Yield(s0, stream(nextState, sx))
                }
              }
            } getOrElse {
              M.point(StreamT.Done)
            }
          }
        } yield back
      )

      stream(sliceTransform.initial, slices)
    }

    /**
      * Merge controls the iteration over the table of group key values.
      */
    def merge[N[+ _]](grouping: GroupingSpec)(body: (RValue, GroupId => M[Table]) => N[Table])(implicit nt: N ~> M): M[Table] = {
      import GroupKeySpec.{ dnf, toVector }

      type Key       = Seq[RValue]
      type KeySchema = Seq[CPathField]

      def sources(spec: GroupKeySpec): Seq[GroupKeySpecSource] = (spec: @unchecked) match {
        case GroupKeySpecAnd(left, right) => sources(left) ++ sources(right)
        case src: GroupKeySpecSource      => Vector(src)
      }

      def mkProjections(spec: GroupKeySpec) =
        toVector(dnf(spec)).map(sources(_).map { s =>
          (s.key, s.spec)
        })

      case class IndexedSource(groupId: GroupId, index: TableIndex, keySchema: KeySchema)

      (for {
        source <- grouping.sources
        groupKeyProjections <- mkProjections(source.groupKeySpec)
        disjunctGroupKeyTransSpecs = groupKeyProjections.map { case (key, spec) => spec }
      } yield {
        TableIndex.createFromTable(source.table, disjunctGroupKeyTransSpecs, source.targetTrans.getOrElse(TransSpec1.Id)).map { index =>
          IndexedSource(source.groupId, index, groupKeyProjections.map(_._1))
        }
      }).sequence.flatMap { sourceKeys =>
        val fullSchema = sourceKeys.flatMap(_.keySchema).distinct

        val indicesGroupedBySource = sourceKeys.groupBy(_.groupId).mapValues(_.map(y => (y.index, y.keySchema)).toSeq).values.toSeq

        def unionOfIntersections(indicesGroupedBySource: Seq[Seq[(TableIndex, KeySchema)]]): Set[Key] = {
          def allSourceDNF[T](l: Seq[Seq[T]]): Seq[Seq[T]] = {
            l match {
              case Seq(hd) => hd.map(Seq(_))
              case Seq(hd, tl @ _ *) => {
                for {
                  disjunctHd <- hd
                  disjunctTl <- allSourceDNF(tl)
                } yield disjunctHd +: disjunctTl
              }
              case empty => empty
            }
          }

          def normalizedKeys(index: TableIndex, keySchema: KeySchema): collection.Set[Key] = {
            val schemaMap = for (k <- fullSchema) yield keySchema.indexOf(k)
            for (key <- index.getUniqueKeys)
              yield for (k <- schemaMap) yield if (k == -1) CUndefined else key(k)
          }

          def intersect(keys0: collection.Set[Key], keys1: collection.Set[Key]): collection.Set[Key] = {
            def consistent(key0: Key, key1: Key): Boolean =
              (key0 zip key1).forall {
                case (k0, k1) => k0 == k1 || k0 == CUndefined || k1 == CUndefined
              }

            def merge(key0: Key, key1: Key): Key =
              (key0 zip key1).map {
                case (k0, CUndefined) => k0
                case (_, k1)          => k1
              }

            // TODO: This "mini-cross" is much better than the
            // previous mega-cross. However in many situations we
            // could do even less work. Consider further optimization
            // (e.g. when one key schema is a subset of the other).

            // Relatedly it might make sense to topologically sort the
            // Indices by their keyschemas so that we end up intersecting
            // key with their subset.
            keys0.flatMap { key0 =>
              keys1.flatMap(key1 => if (consistent(key0, key1)) Some(merge(key0, key1)) else None)
            }
          }

          allSourceDNF(indicesGroupedBySource).foldLeft(Set.empty[Key]) {
            case (acc, intersection) =>
              val hd = normalizedKeys(intersection.head._1, intersection.head._2)
              acc | intersection.tail.foldLeft(hd) {
                case (keys0, (index1, schema1)) =>
                  val keys1 = normalizedKeys(index1, schema1)
                  intersect(keys0, keys1)
              }
          }
        }

        def jValueFromGroupKey(key: Seq[RValue], cpaths: Seq[CPathField]): RValue = {
          val items = (cpaths zip key).map(t => (t._1.name, t._2))
          RObject(items.toMap)
        }

        val groupKeys: Set[Key] = unionOfIntersections(indicesGroupedBySource)

        // given a groupKey, return an M[Table] which represents running
        // the evaluator on that subgroup.
        def evaluateGroupKey(groupKey: Key): M[Table] = {
          val groupKeyTable = jValueFromGroupKey(groupKey, fullSchema)

          def map(gid: GroupId): M[Table] = {
            val subTableProjections = (sourceKeys
              .filter(_.groupId == gid)
              .map { indexedSource =>
                val keySchema           = indexedSource.keySchema
                val projectedKeyIndices = for (k <- fullSchema) yield keySchema.indexOf(k)
                (indexedSource.index, projectedKeyIndices, groupKey)
              })
              .toList

            M.point(TableIndex.joinSubTables(subTableProjections).normalize) // TODO: normalize necessary?
          }

          nt(body(groupKeyTable, map))
        }

        // TODO: this can probably be done as one step, but for now
        // it's probably fine.
        val tables: StreamT[M, Table] = StreamT.unfoldM(groupKeys.toList) {
          case k :: ks =>
            evaluateGroupKey(k).map(t => Some((t, ks)))
          case Nil =>
            M.point(None)
        }

        val slices: StreamT[M, Slice] = tables.flatMap(_.slices)

        M.point(Table(slices, UnknownSize))
      }
    }

    /// Utility Methods ///

    /**
      * Reduce the specified table to obtain the in-memory set of strings representing the vfs paths
      * to be loaded.
      */
    protected def pathsM(table: Table) = {
      table reduce {
        new CReducer[Set[Path]] {
          def reduce(schema: CSchema, range: Range): Set[Path] = {
            schema.columns(JTextT) flatMap {
              case s: StrColumn => range.filter(s.isDefinedAt).map(i => Path(s(i)))
              case _            => Set()
            }
          }
        }
      }
    }

    def fromRValues(values: Stream[RValue], maxSliceSize: Option[Int] = None): Table = {
      val sliceSize = maxSliceSize.getOrElse(yggConfig.maxSliceSize)

      def makeSlice(data: Stream[RValue]): (Slice, Stream[RValue]) = {
        val (prefix, suffix) = data.splitAt(sliceSize)

        (Slice.fromRValues(prefix), suffix)
      }

      Table(
        StreamT.unfoldM(values) { events =>
          M.point {
            (!events.isEmpty) option {
              makeSlice(events.toStream)
            }
          }
        },
        ExactSize(values.length)
      )
    }

    def join(left: Table, right: Table, orderHint: Option[JoinOrder] = None)(leftKeySpec: TransSpec1,
                                                                             rightKeySpec: TransSpec1,
                                                                             joinSpec: TransSpec2): M[(JoinOrder, Table)] = {
      val emptySpec = trans.ConstLiteral(CEmptyArray, Leaf(Source))
      for {
        left0 <- left.sort(leftKeySpec)
        right0 <- right.sort(rightKeySpec)
        cogrouped = left0.cogroup(leftKeySpec, rightKeySpec, right0)(emptySpec, emptySpec, trans.WrapArray(joinSpec))
      } yield {
        JoinOrder.KeyOrder -> cogrouped.transform(trans.DerefArrayStatic(Leaf(Source), CPathIndex(0)))
      }
    }

    def cross(left: Table, right: Table, orderHint: Option[CrossOrder] = None)(spec: TransSpec2): M[(CrossOrder, Table)] = {
      import CrossOrder._
      M.point(orderHint match {
        case Some(CrossRight | CrossRightLeft) =>
          CrossRight -> right.cross(left)(TransSpec2.flip(spec))
        case _ =>
          CrossLeft -> left.cross(right)(spec)
      })
    }
  }

  abstract class ColumnarTable(slices0: StreamT[M, Slice], val size: TableSize) extends TableLike with SamplableColumnarTable { self: Table =>
    import SliceTransform._

    private final val readStarts = new java.util.concurrent.atomic.AtomicInteger
    private final val blockReads = new java.util.concurrent.atomic.AtomicInteger

    val slices = StreamT(
      StreamT
        .Skip({
          readStarts.getAndIncrement
          slices0.map(s => { blockReads.getAndIncrement; s })
        })
        .point[M]
    )

    /**
      * Folds over the table to produce a single value (stored in a singleton table).
      */
    def reduce[A](reducer: Reducer[A])(implicit monoid: Monoid[A]): M[A] = {
      def rec(stream: StreamT[M, A], acc: A): M[A] = {
        stream.uncons flatMap {
          case Some((head, tail)) => rec(tail, head |+| acc)
          case None               => M.point(acc)
        }
      }

      rec(
        slices map { s =>
          val schema = new CSchema {
            def columnRefs = s.columns.keySet
            def columnMap(jtpe: JType) =
              s.columns collect {
                case (ref @ ColumnRef(cpath, ctype), col) if Schema.includes(jtpe, cpath, ctype) =>
                  ref -> col
              }
          }

          reducer.reduce(schema, 0 until s.size)
        },
        monoid.zero
      )
    }

    def compact(spec: TransSpec1, definedness: Definedness = AnyDefined): Table = {
      val specTransform = SliceTransform.composeSliceTransform(spec)
      val compactTransform = {
        SliceTransform.composeSliceTransform(Leaf(Source)).zip(specTransform) { (s1, s2) =>
          s1.compact(s2, definedness)
        }
      }
      Table(Table.transformStream(compactTransform, slices), size).normalize
    }

    /**
      * Performs a one-pass transformation of the keys and values in the table.
      * If the key transform is not identity, the resulting table will have
      * unknown sort order.
      */
    def transform(spec: TransSpec1): Table = {
      Table(Table.transformStream(composeSliceTransform(spec), slices), this.size)
    }

    def force: M[Table] = {
      def loop(slices: StreamT[M, Slice], acc: List[Slice], size: Long): M[(List[Slice], Long)] = slices.uncons flatMap {
        case Some((slice, tail)) if slice.size > 0 =>
          loop(tail, slice.materialized :: acc, size + slice.size)
        case Some((_, tail)) =>
          loop(tail, acc, size)
        case None =>
          M.point((acc.reverse, size))
      }
      val former = new (Id.Id ~> M) { def apply[A](a: Id.Id[A]): M[A] = M.point(a) }
      loop(slices, Nil, 0L).map {
        case (stream, size) =>
          Table(StreamT.fromIterable(stream).trans(former), ExactSize(size))
      }
    }

    def paged(limit: Int): Table = {
      val slices2 = slices flatMap { slice =>
        StreamT.unfoldM(0) { idx =>
          val back =
            if (idx >= slice.size)
              None
            else
              Some((slice.takeRange(idx, limit), idx + limit))

          M.point(back)
        }
      }

      Table(slices2, size)
    }

    def concat(t2: Table): Table = {
      val resultSize   = TableSize(size.maxSize + t2.size.maxSize)
      val resultSlices = slices ++ t2.slices
      Table(resultSlices, resultSize)
    }

    /**
      * Zips two tables together in their current sorted order.
      * If the tables are not normalized first and thus have different slices sizes,
      * then since the zipping is done per slice, this can produce a result that is
      * different than if the tables were normalized.
      */
    def zip(t2: Table): M[Table] = {
      def rec(slices1: StreamT[M, Slice], slices2: StreamT[M, Slice]): StreamT[M, Slice] = {
        StreamT(slices1.uncons flatMap {
          case Some((head1, tail1)) =>
            slices2.uncons map {
              case Some((head2, tail2)) =>
                StreamT.Yield(head1 zip head2, rec(tail1, tail2))
              case None =>
                StreamT.Done
            }

          case None =>
            M point StreamT.Done
        })
      }

      val resultSize = EstimateSize(0, size.maxSize min t2.size.maxSize)
      M point Table(rec(slices, t2.slices), resultSize)

      // todo investigate why the code below makes all of RandomLibSpecs explode
      // val resultSlices = Apply[({ type l[a] = StreamT[M, a] })#l].zip.zip(slices, t2.slices) map { case (s1, s2) => s1.zip(s2) }
      // Table(resultSlices, resultSize)
    }

    def toArray[A](implicit tpe: CValueType[A]): Table = {
      val slices2: StreamT[M, Slice] = slices map { _.toArray[A] }
      Table(slices2, size)
    }

    /**
      * Returns a table where each slice (except maybe the last) has slice size `length`.
      * Also removes slices of size zero. If an optional `maxLength0` size is provided,
      * then the slices need only land in the range between `length` and `maxLength0`.
      * For slices being loaded from ingest, it is often the case that we are missing a
      * few rows at the end, so we shouldn't be too strict.
      */
    def canonicalize(length: Int, maxLength0: Option[Int] = None): Table = {
      val minLength = length
      val maxLength = maxLength0 getOrElse length

      require(maxLength > 0 && minLength >= 0 && maxLength >= minLength, "length bounds must be positive and ordered")

      def concat(rslices: List[Slice]): Slice = rslices.reverse match {
        case Nil          => Slice(Map.empty, 0)
        case slice :: Nil => slice
        case slices =>
          val slice = Slice.concat(slices)
          if (slices.size > (slice.size / yggConfig.smallSliceSize)) {
            slice.materialized // Deal w/ lots of small slices by materializing them.
          } else {
            slice
          }
      }

      def step(sliceSize: Int, acc: List[Slice], stream: StreamT[M, Slice]): M[StreamT.Step[Slice, StreamT[M, Slice]]] = {
        stream.uncons flatMap {
          case Some((head, tail)) =>
            if (head.size == 0) {
              // Skip empty slices.
              step(sliceSize, acc, tail)

            } else if (sliceSize + head.size >= minLength) {
              // We emit a slice, but the last slice added may fall on a stream boundary.
              val splitAt = math.min(head.size, maxLength - sliceSize)
              if (splitAt < head.size) {
                val (prefix, suffix) = head.split(splitAt)
                val slice            = concat(prefix :: acc)
                M.point(StreamT.Yield(slice, StreamT(step(0, Nil, suffix :: tail))))
              } else {
                val slice = concat(head :: acc)
                M.point(StreamT.Yield(slice, StreamT(step(0, Nil, tail))))
              }

            } else {
              // Just keep swimming (aka accumulating).
              step(sliceSize + head.size, head :: acc, tail)
            }

          case None =>
            if (sliceSize > 0) {
              M.point(StreamT.Yield(concat(acc), StreamT.empty[M, Slice]))
            } else {
              M.point(StreamT.Done)
            }
        }
      }

      Table(StreamT(step(0, Nil, slices)), size)
    }

    /**
      * Cogroups this table with another table, using equality on the specified
      * transformation on rows of the table.
      */
    def cogroup(leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(leftResultTrans: TransSpec1,
                                                                        rightResultTrans: TransSpec1,
                                                                        bothResultTrans: TransSpec2): Table = {

      // println("Cogrouping with respect to\nleftKey: " + leftKey + "\nrightKey: " + rightKey)
      class IndexBuffers(lInitialSize: Int, rInitialSize: Int) {
        val lbuf   = new ArrayIntList(lInitialSize)
        val rbuf   = new ArrayIntList(rInitialSize)
        val leqbuf = new ArrayIntList(lInitialSize max rInitialSize)
        val reqbuf = new ArrayIntList(lInitialSize max rInitialSize)

        @inline def advanceLeft(lpos: Int): Unit = {
          lbuf.add(lpos)
          rbuf.add(-1)
          leqbuf.add(-1)
          reqbuf.add(-1)
        }

        @inline def advanceRight(rpos: Int): Unit = {
          lbuf.add(-1)
          rbuf.add(rpos)
          leqbuf.add(-1)
          reqbuf.add(-1)
        }

        @inline def advanceBoth(lpos: Int, rpos: Int): Unit = {
          // println("advanceBoth: lpos = %d, rpos = %d" format (lpos, rpos))
          lbuf.add(-1)
          rbuf.add(-1)
          leqbuf.add(lpos)
          reqbuf.add(rpos)
        }

        def cogrouped[LR, RR, BR](lslice: Slice,
                                  rslice: Slice,
                                  leftTransform: SliceTransform1[LR],
                                  rightTransform: SliceTransform1[RR],
                                  bothTransform: SliceTransform2[BR]): M[(Slice, LR, RR, BR)] = {

          val remappedLeft  = lslice.remap(lbuf)
          val remappedRight = rslice.remap(rbuf)

          val remappedLeq = lslice.remap(leqbuf)
          val remappedReq = rslice.remap(reqbuf)

          for {
            pairL <- leftTransform(remappedLeft)
            (ls0, lx) = pairL

            pairR <- rightTransform(remappedRight)
            (rs0, rx) = pairR

            pairB <- bothTransform(remappedLeq, remappedReq)
            (bs0, bx) = pairB
          } yield {
            assert(lx.size == rx.size && rx.size == bx.size)
            val resultSlice = lx zip rx zip bx

            (resultSlice, ls0, rs0, bs0)
          }
        }

        override def toString = {
          "left: " + lbuf.toArray.mkString("[", ",", "]") + "\n" +
            "right: " + rbuf.toArray.mkString("[", ",", "]") + "\n" +
            "both: " + (leqbuf.toArray zip reqbuf.toArray).mkString("[", ",", "]")
        }
      }

      final case class SliceId(id: Int) {
        def +(n: Int): SliceId = SliceId(id + n)
      }

      case class SlicePosition[K](sliceId: SliceId,
                                  /** The position in the current slice. This will only be nonzero when the slice has been appended
                                    * to as a result of a cartesian crossing the slice boundary */
                                  pos: Int,
                                  /** Present if not in a final right or left run. A pair of a key slice that is parallel to the
                                    * current data slice, and the value that is needed as input to sltk or srtk to produce the next key. */
                                  keyState: K,
                                  key: Slice,
                                  /** The current slice to be operated upon. */
                                  data: Slice,
                                  /** The remainder of the stream to be operated upon. */
                                  tail: StreamT[M, Slice])

      sealed trait NextStep[A, B]
      case class SplitLeft[A, B](lpos: Int)  extends NextStep[A, B]
      case class SplitRight[A, B](rpos: Int) extends NextStep[A, B]
      case class NextCartesianLeft[A, B](left: SlicePosition[A],
                                         right: SlicePosition[B],
                                         rightStart: Option[SlicePosition[B]],
                                         rightEnd: Option[SlicePosition[B]])
          extends NextStep[A, B]
      case class NextCartesianRight[A, B](left: SlicePosition[A],
                                          right: SlicePosition[B],
                                          rightStart: Option[SlicePosition[B]],
                                          rightEnd: Option[SlicePosition[B]])
          extends NextStep[A, B]
      case class SkipRight[A, B](left: SlicePosition[A], rightEnd: SlicePosition[B])                                  extends NextStep[A, B]
      case class RestartRight[A, B](left: SlicePosition[A], rightStart: SlicePosition[B], rightEnd: SlicePosition[B]) extends NextStep[A, B]
      def cogroup0[LK, RK, LR, RR, BR](stlk: SliceTransform1[LK],
                                       strk: SliceTransform1[RK],
                                       stlr: SliceTransform1[LR],
                                       strr: SliceTransform1[RR],
                                       stbr: SliceTransform2[BR]) = {

        sealed trait CogroupState
        case class EndLeft(lr: LR, lhead: Slice, ltail: StreamT[M, Slice]) extends CogroupState
        case class Cogroup(lr: LR,
                           rr: RR,
                           br: BR,
                           left: SlicePosition[LK],
                           right: SlicePosition[RK],
                           rightStart: Option[SlicePosition[RK]],
                           rightEnd: Option[SlicePosition[RK]])
            extends CogroupState
        case class EndRight(rr: RR, rhead: Slice, rtail: StreamT[M, Slice]) extends CogroupState
        case object CogroupDone extends CogroupState

        // step is the continuation function fed to uncons. It is called once for each emitted slice
        def step(state: CogroupState): M[Option[(Slice, CogroupState)]] = {

          // step0 is the inner monadic recursion needed to cross slice boundaries within the emission of a slice
          def step0(lr: LR,
                    rr: RR,
                    br: BR,
                    leftPosition: SlicePosition[LK],
                    rightPosition: SlicePosition[RK],
                    rightStart0: Option[SlicePosition[RK]],
                    rightEnd0: Option[SlicePosition[RK]])(
              ibufs: IndexBuffers = new IndexBuffers(leftPosition.key.size, rightPosition.key.size)): M[Option[(Slice, CogroupState)]] = {

            val SlicePosition(lSliceId, lpos0, lkstate, lkey, lhead, ltail) = leftPosition
            val SlicePosition(rSliceId, rpos0, rkstate, rkey, rhead, rtail) = rightPosition

            val comparator = Slice.rowComparatorFor(lkey, rkey) {
              // since we've used the key transforms, and since transforms are contracturally
              // forbidden from changing slice size, we can just use all
              _.columns.keys map (_.selector)
            }

            // the inner tight loop; this will recur while we're within the bounds of
            // a pair of slices. Any operation that must cross slice boundaries
            // must exit this inner loop and recur through the outer monadic loop
            // xrstart is an int with sentinel value for effieiency, but is Option at the slice level.
            @inline
            @tailrec
            def buildRemappings(lpos: Int,
                                rpos: Int,
                                rightStart: Option[SlicePosition[RK]],
                                rightEnd: Option[SlicePosition[RK]],
                                endRight: Boolean): NextStep[LK, RK] = {
              // println("lpos = %d, rpos = %d, rightStart = %s, rightEnd = %s, endRight = %s" format (lpos, rpos, rightStart, rightEnd, endRight))
              // println("Left key: " + lkey.toJson(lpos))
              // println("Right key: " + rkey.toJson(rpos))
              // println("Left data: " + lhead.toJson(lpos))
              // println("Right data: " + rhead.toJson(rpos))

              rightStart match {
                case Some(resetMarker @ SlicePosition(rightStartSliceId, rightStartPos, _, rightStartSlice, _, _)) =>
                  // We're currently in a cartesian.
                  if (lpos < lhead.size && rpos < rhead.size) {
                    comparator.compare(lpos, rpos) match {
                      case LT if rightStartSliceId == rSliceId =>
                        buildRemappings(lpos + 1, rightStartPos, rightStart, Some(rightPosition.copy(pos = rpos)), endRight)
                      case LT =>
                        // Transition to emit the current slice and reset the right side, carry rightPosition through
                        RestartRight(leftPosition.copy(pos = lpos + 1), resetMarker, rightPosition.copy(pos = rpos))
                      case GT =>
                        // catch input-out-of-order errors early
                        rightEnd match {
                          case None =>
                            // println("lhead\n" + lkey.toJsonString())
                            // println("rhead\n" + rkey.toJsonString())
                            sys.error(
                              "Inputs are not sorted; value on the left exceeded value on the right at the end of equal span. lpos = %d, rpos = %d"
                                .format(lpos, rpos))

                          case Some(SlicePosition(endSliceId, endPos, _, endSlice, _, _)) if endSliceId == rSliceId =>
                            buildRemappings(lpos, endPos, None, None, endRight)

                          case Some(rend @ SlicePosition(endSliceId, _, _, _, _, _)) =>
                            // Step out of buildRemappings so that we can restart with the current rightEnd
                            SkipRight(leftPosition.copy(pos = lpos), rend)
                        }
                      case EQ =>
                        ibufs.advanceBoth(lpos, rpos)
                        buildRemappings(lpos, rpos + 1, rightStart, rightEnd, endRight)
                    }
                  } else if (lpos < lhead.size) {
                    if (endRight) {
                      // println(s"Restarting right: lpos = ${lpos + 1}; rpos = $rpos")
                      RestartRight(leftPosition.copy(pos = lpos + 1), resetMarker, rightPosition.copy(pos = rpos))
                    } else {
                      // right slice is exhausted, so we need to emit that slice from the right tail
                      // then continue in the cartesian
                      NextCartesianRight(leftPosition.copy(pos = lpos), rightPosition.copy(pos = rpos), rightStart, rightEnd)
                    }
                  } else if (rpos < rhead.size) {
                    // left slice is exhausted, so we need to emit that slice from the left tail
                    // then continue in the cartesian
                    NextCartesianLeft(leftPosition, rightPosition.copy(pos = rpos), rightStart, rightEnd)
                  } else {
                    sys.error("This state should be unreachable, since we only increment one side at a time.")
                  }

                case None =>
                  // not currently in a cartesian, hence we can simply proceed.
                  if (lpos < lhead.size && rpos < rhead.size) {
                    comparator.compare(lpos, rpos) match {
                      case LT =>
                        ibufs.advanceLeft(lpos)
                        buildRemappings(lpos + 1, rpos, None, None, endRight)
                      case GT =>
                        ibufs.advanceRight(rpos)
                        buildRemappings(lpos, rpos + 1, None, None, endRight)
                      case EQ =>
                        ibufs.advanceBoth(lpos, rpos)
                        buildRemappings(lpos, rpos + 1, Some(rightPosition.copy(pos = rpos)), None, endRight)
                    }
                  } else if (lpos < lhead.size) {
                    // right side is exhausted, so we should just split the left and emit
                    SplitLeft(lpos)
                  } else if (rpos < rhead.size) {
                    // left side is exhausted, so we should just split the right and emit
                    SplitRight(rpos)
                  } else {
                    sys.error("This state should be unreachable, since we only increment one side at a time.")
                  }
              }
            }

            def continue(nextStep: NextStep[LK, RK]): M[Option[(Slice, CogroupState)]] = nextStep match {
              case SplitLeft(lpos) =>
                val (lpref, lsuf) = lhead.split(lpos)
                val (_, lksuf)    = lkey.split(lpos)
                ibufs.cogrouped(lpref, rhead, SliceTransform1[LR](lr, stlr.f), SliceTransform1[RR](rr, strr.f), SliceTransform2[BR](br, stbr.f)) flatMap {
                  case (completeSlice, lr0, rr0, br0) => {
                    rtail.uncons flatMap {
                      case Some((nextRightHead, nextRightTail)) =>
                        strk.f(rkstate, nextRightHead) map {
                          case (rkstate0, rkey0) => {
                            val nextState = Cogroup(
                              lr0,
                              rr0,
                              br0,
                              SlicePosition(lSliceId, 0, lkstate, lksuf, lsuf, ltail),
                              SlicePosition(rSliceId + 1, 0, rkstate0, rkey0, nextRightHead, nextRightTail),
                              None,
                              None)

                            Some(completeSlice -> nextState)
                          }
                        }

                      case None =>
                        val nextState = EndLeft(lr0, lsuf, ltail)
                        M.point(Some(completeSlice -> nextState))
                    }
                  }
                }

              case SplitRight(rpos) =>
                val (rpref, rsuf) = rhead.split(rpos)
                val (_, rksuf)    = rkey.split(rpos)

                ibufs.cogrouped(lhead, rpref, SliceTransform1[LR](lr, stlr.f), SliceTransform1[RR](rr, strr.f), SliceTransform2[BR](br, stbr.f)) flatMap {
                  case (completeSlice, lr0, rr0, br0) => {
                    ltail.uncons flatMap {
                      case Some((nextLeftHead, nextLeftTail)) =>
                        stlk.f(lkstate, nextLeftHead) map {
                          case (lkstate0, lkey0) => {
                            val nextState = Cogroup(
                              lr0,
                              rr0,
                              br0,
                              SlicePosition(lSliceId + 1, 0, lkstate0, lkey0, nextLeftHead, nextLeftTail),
                              SlicePosition(rSliceId, 0, rkstate, rksuf, rsuf, rtail),
                              None,
                              None)

                            Some(completeSlice -> nextState)
                          }
                        }

                      case None =>
                        val nextState = EndRight(rr0, rsuf, rtail)
                        M.point(Some(completeSlice -> nextState))
                    }
                  }
                }

              case NextCartesianLeft(left, right, rightStart, rightEnd) =>
                left.tail.uncons flatMap {
                  case Some((nextLeftHead, nextLeftTail)) =>
                    ibufs
                      .cogrouped(left.data, right.data, SliceTransform1[LR](lr, stlr.f), SliceTransform1[RR](rr, strr.f), SliceTransform2[BR](br, stbr.f)) flatMap {
                      case (completeSlice, lr0, rr0, br0) => {
                        stlk.f(lkstate, nextLeftHead) map {
                          case (lkstate0, lkey0) => {
                            val nextState =
                              Cogroup(lr0, rr0, br0, SlicePosition(lSliceId + 1, 0, lkstate0, lkey0, nextLeftHead, nextLeftTail), right, rightStart, rightEnd)

                            Some(completeSlice -> nextState)
                          }
                        }
                      }
                    }

                  case None =>
                    (rightStart, rightEnd) match {
                      case (Some(_), Some(end)) =>
                        val (rpref, rsuf) = end.data.split(end.pos)

                        ibufs
                          .cogrouped(left.data, rpref, SliceTransform1[LR](lr, stlr.f), SliceTransform1[RR](rr, strr.f), SliceTransform2[BR](br, stbr.f)) map {
                          case (completeSlice, lr0, rr0, br0) => {
                            val nextState = EndRight(rr0, rsuf, end.tail)
                            Some(completeSlice -> nextState)
                          }
                        }

                      case _ =>
                        ibufs.cogrouped(
                          left.data,
                          right.data,
                          SliceTransform1[LR](lr, stlr.f),
                          SliceTransform1[RR](rr, strr.f),
                          SliceTransform2[BR](br, stbr.f)) map {
                          case (completeSlice, lr0, rr0, br0) =>
                            Some(completeSlice -> CogroupDone)
                        }
                    }
                }

              case NextCartesianRight(left, right, rightStart, rightEnd) =>
                right.tail.uncons flatMap {
                  case Some((nextRightHead, nextRightTail)) =>
                    ibufs
                      .cogrouped(left.data, right.data, SliceTransform1[LR](lr, stlr.f), SliceTransform1[RR](rr, strr.f), SliceTransform2[BR](br, stbr.f)) flatMap {
                      case (completeSlice, lr0, rr0, br0) => {
                        strk.f(rkstate, nextRightHead) map {
                          case (rkstate0, rkey0) => {
                            val nextState =
                              Cogroup(lr0, rr0, br0, left, SlicePosition(rSliceId + 1, 0, rkstate0, rkey0, nextRightHead, nextRightTail), rightStart, rightEnd)

                            Some(completeSlice -> nextState)
                          }
                        }
                      }
                    }

                  case None =>
                    continue(buildRemappings(left.pos, right.pos, rightStart, rightEnd, true))
                }

              case SkipRight(left, rightEnd) =>
                step0(lr, rr, br, left, rightEnd, None, None)()

              case RestartRight(left, rightStart, rightEnd) =>
                ibufs.cogrouped(
                  left.data,
                  rightPosition.data,
                  SliceTransform1[LR](lr, stlr.f),
                  SliceTransform1[RR](rr, strr.f),
                  SliceTransform2[BR](br, stbr.f)) map {
                  case (completeSlice, lr0, rr0, br0) => {
                    val nextState = Cogroup(lr0, rr0, br0, left, rightStart, Some(rightStart), Some(rightEnd))

                    // println(s"Computing restart state as $nextState")

                    Some(completeSlice -> nextState)
                  }
                }
            }

            continue(buildRemappings(lpos0, rpos0, rightStart0, rightEnd0, false))
          } // end of step0

          state match {
            case EndLeft(lr, data, tail) =>
              stlr.f(lr, data) flatMap {
                case (lr0, leftResult) => {
                  tail.uncons map { unconsed =>
                    Some(leftResult -> (unconsed map { case (nhead, ntail) => EndLeft(lr0, nhead, ntail) } getOrElse CogroupDone))
                  }
                }
              }

            case Cogroup(lr, rr, br, left, right, rightReset, rightEnd) =>
              step0(lr, rr, br, left, right, rightReset, rightEnd)()

            case EndRight(rr, data, tail) =>
              strr.f(rr, data) flatMap {
                case (rr0, rightResult) => {
                  tail.uncons map { unconsed =>
                    Some(rightResult -> (unconsed map { case (nhead, ntail) => EndRight(rr0, nhead, ntail) } getOrElse CogroupDone))
                  }
                }
              }

            case CogroupDone => M.point(None)
          }
        } // end of step

        val initialState = for {
          // We have to compact both sides to avoid any rows for which the key is completely undefined
          leftUnconsed <- self.compact(leftKey).slices.uncons
          rightUnconsed <- that.compact(rightKey).slices.uncons

          back <- {
            val cogroup = for {
              (leftHead, leftTail) <- leftUnconsed
              (rightHead, rightTail) <- rightUnconsed
            } yield {
              for {
                pairL <- stlk(leftHead)
                (lkstate, lkey) = pairL

                pairR <- strk(rightHead)
                (rkstate, rkey) = pairR
              } yield {
                Cogroup(
                  stlr.initial,
                  strr.initial,
                  stbr.initial,
                  SlicePosition(SliceId(0), 0, lkstate, lkey, leftHead, leftTail),
                  SlicePosition(SliceId(0), 0, rkstate, rkey, rightHead, rightTail),
                  None,
                  None)
              }
            }

            val optM = cogroup orElse {
              leftUnconsed map {
                case (head, tail) => EndLeft(stlr.initial, head, tail)
              } map { M point _ }
            } orElse {
              rightUnconsed map {
                case (head, tail) => EndRight(strr.initial, head, tail)
              } map { M point _ }
            }

            optM map { m =>
              m map { Some(_) }
            } getOrElse {
              M.point(None)
            }
          }
        } yield back

        Table(StreamT.wrapEffect(initialState map { state =>
          StreamT.unfoldM[M, Slice, CogroupState](state getOrElse CogroupDone)(step)
        }), UnknownSize)
      }

      cogroup0(
        composeSliceTransform(leftKey),
        composeSliceTransform(rightKey),
        composeSliceTransform(leftResultTrans),
        composeSliceTransform(rightResultTrans),
        composeSliceTransform2(bothResultTrans))
    }

    /**
      * Performs a full cartesian cross on this table with the specified table,
      * applying the specified transformation to merge the two tables into
      * a single table.
      */
    def cross(that: Table)(spec: TransSpec2): Table = {
      def cross0[A](transform: SliceTransform2[A]): M[StreamT[M, Slice]] = {
        case class CrossState(a: A, position: Int, tail: StreamT[M, Slice])

        def crossBothSingle(lhead: Slice, rhead: Slice)(a0: A): M[(A, StreamT[M, Slice])] = {
          // We try to fill out the slices as much as possible, so we work with
          // several rows from the left at a time.

          val lrowsPerSlice = math.max(1, yggConfig.maxSliceSize / rhead.size)
          val sliceSize     = lrowsPerSlice * rhead.size

          // Note that this is still memory efficient, as the columns are re-used
          // between all slices.

          val results = (0 until lhead.size by lrowsPerSlice).foldLeft(M.point((a0, List.empty[Slice]))) {
            case (accM, offset) =>
              accM flatMap {
                case (a, acc) =>
                  val rows = math.min(sliceSize, (lhead.size - offset) * rhead.size)

                  val lslice = new Slice {
                    val size = rows
                    val columns = lhead.columns.lazyMapValues(Remap({ i =>
                      offset + (i / rhead.size)
                    })(_).get)
                  }

                  val rslice = new Slice {
                    val size = rows
                    val columns =
                      if (rhead.size == 0)
                        rhead.columns.lazyMapValues(Empty(_).get)
                      else
                        rhead.columns.lazyMapValues(Remap(_ % rhead.size)(_).get)
                  }

                  transform.f(a, lslice, rslice) map {
                    case (b, resultSlice) =>
                      (b, resultSlice :: acc)
                  }
              }
          }

          results map {
            case (a1, slices) =>
              val sliceStream = slices.reverse.toStream
              (a1, StreamT.fromStream(M.point(sliceStream)))
          }
        }

        def crossLeftSingle(lhead: Slice, right: StreamT[M, Slice])(a0: A): StreamT[M, Slice] = {
          def step(state: CrossState): M[Option[(Slice, CrossState)]] = {
            if (state.position < lhead.size) {
              state.tail.uncons flatMap {
                case Some((rhead, rtail0)) =>
                  val lslice = new Slice {
                    val size    = rhead.size
                    val columns = lhead.columns.lazyMapValues(Remap(i => state.position)(_).get)
                  }

                  transform.f(state.a, lslice, rhead) map {
                    case (a0, resultSlice) =>
                      Some((resultSlice, CrossState(a0, state.position, rtail0)))
                  }

                case None =>
                  step(CrossState(state.a, state.position + 1, right))
              }
            } else {
              M.point(None)
            }
          }

          StreamT.unfoldM(CrossState(a0, 0, right))(step _)
        }

        def crossRightSingle(left: StreamT[M, Slice], rhead: Slice)(a0: A): StreamT[M, Slice] = {
          StreamT(left.uncons flatMap {
            case Some((lhead, ltail0)) =>
              crossBothSingle(lhead, rhead)(a0) map {
                case (a1, prefix) =>
                  StreamT.Skip(prefix ++ crossRightSingle(ltail0, rhead)(a1))
              }

            case None =>
              M.point(StreamT.Done)
          })
        }

        def crossBoth(ltail: StreamT[M, Slice], rtail: StreamT[M, Slice]): StreamT[M, Slice] = {
          // This doesn't carry the Transform's state around, so, I think it is broken.
          ltail.flatMap(crossLeftSingle(_, rtail)(transform.initial))
        }

        // We canonicalize the tables so that no slices are too small.
        val left  = this.canonicalize(yggConfig.minIdealSliceSize, Some(yggConfig.maxSliceSize))
        val right = that.canonicalize(yggConfig.minIdealSliceSize, Some(yggConfig.maxSliceSize))

        left.slices.uncons flatMap {
          case Some((lhead, ltail)) =>
            right.slices.uncons flatMap {
              case Some((rhead, rtail)) =>
                for {
                  lempty <- ltail.isEmpty //TODO: Scalaz result here is negated from what it should be!
                  rempty <- rtail.isEmpty

                  back <- {
                    // println(s"checking cross: lempty = $lempty; rempty = $rempty")

                    if (lempty && rempty) {
                      // both are small sets, so find the cross in memory
                      crossBothSingle(lhead, rhead)(transform.initial) map { _._2 }
                    } else if (lempty) {
                      // left side is a small set, so restart it in memory
                      M.point(crossLeftSingle(lhead, rhead :: rtail)(transform.initial))
                    } else if (rempty) {
                      // right side is a small set, so restart it in memory
                      M.point(crossRightSingle(lhead :: ltail, rhead)(transform.initial))
                    } else {
                      // both large sets, so just walk the left restarting the right.
                      M.point(crossBoth(lhead :: ltail, rhead :: rtail))
                    }
                  }
                } yield back

              case None => M.point(StreamT.empty[M, Slice])
            }

          case None => M.point(StreamT.empty[M, Slice])
        }
      }

      // TODO: We should be able to fully compute the size of the result above.
      val newSize = (size, that.size) match {
        case (ExactSize(l), ExactSize(r))         => TableSize(l max r, l * r)
        case (EstimateSize(ln, lx), ExactSize(r)) => TableSize(ln max r, lx * r)
        case (ExactSize(l), EstimateSize(rn, rx)) => TableSize(l max rn, l * rx)
        case _                                    => UnknownSize // Bail on anything else for now (see above TODO)
      }

      val newSizeM = newSize match {
        case ExactSize(s)       => Some(s)
        case EstimateSize(_, s) => Some(s)
        case _                  => None
      }

      val sizeCheck = for (resultSize <- newSizeM) yield resultSize < yggConfig.maxSaneCrossSize && resultSize >= 0

      if (sizeCheck getOrElse true) {
        Table(StreamT(cross0(composeSliceTransform2(spec)) map { tail =>
          StreamT.Skip(tail)
        }), newSize)
      } else {
        throw EnormousCartesianException(this.size, that.size)
      }
    }

    def leftShift(focus: CPath): Table = {
      def lens(columns: Map[ColumnRef, Column]): (Map[ColumnRef, Column], Map[ColumnRef, Column]) = {
        val (focused, unfocused) = columns.partition(_._1.selector.hasPrefix(focus))

        // discard scalar values at focus
        val typed = focused filter {
          case (ColumnRef(`focus`, CArrayType(_)), _) => true
          case (ColumnRef(`focus`, _), _) => false
          case _ => true
        }

        // remap focused to be at "root"
        val remapped = typed map {
          case (ColumnRef(path, tpe), col) =>
            (ColumnRef(path.dropPrefix(focus).get, tpe), col)
        }

        (remapped, unfocused)
      }

      // eagerly force the slices, since we'll be backtracking within each
      val slices2 = slices.map(_.materialized) flatMap { slice =>
        val (focused, unfocused) = lens(slice.columns)

        val innerHeads = {
          val unsorted = focused.keys.toVector flatMap {
            case ColumnRef(path, _) => path.head.toVector
          }

          // sort the index lexicographically except in the case of indices
          unsorted sortWith {
            case (CPathIndex(i1), CPathIndex(i2)) => i1 < i2
            case (p1, p2) => p1.toString < p2.toString
          }
        }

        val innerIndex = Map(innerHeads.zipWithIndex: _*)

        val primitiveWaterMarks = focused.toList collect {
          case (ColumnRef(CPath.Identity, CArrayType(_)), col: HomogeneousArrayColumn[a]) =>
            (0 until slice.size).map(col(_).length).max
        }

        // .max doesn't work, because Scala doesn't understand monoids
        val primitiveMax = primitiveWaterMarks.fold(0)(math.max)

        // TODO doesn't handle the case where we have a sparse array with a missing column!
        // this value may be 0 if we're looking at CEmptyObject | CEmptyArray
        val highWaterMark = math.max(innerHeads.length, primitiveMax)

        val resplit = if (slice.size * highWaterMark > yggConfig.maxSliceSize) {
          val numSplits =
            math.ceil((slice.size * highWaterMark).toDouble / yggConfig.maxSliceSize).toInt

          // this has accumulating rounding errors which can cause slices to exceed max by a small amount in rare cases
          val targetIdx = slice.size / numSplits

          val (acc, last) = (0 until numSplits).foldLeft((Vector.empty[Slice], slice)) {
            case ((acc, cur), _) =>
              val (left, right) = cur.split(targetIdx)
              (acc :+ left, right)
          }

          (acc :+ last).filterNot(_.isEmpty)
        } else {
          Vector(slice)
        }

        // shadow the outer `slice`
        StreamT.fromIterable(resplit).trans(λ[Id.Id ~> M](M.point(_))) map { slice =>
          // ...and the outer `focused` and `unfocused`
          val (focused, unfocused) = lens(slice.columns)

          // a CF1 for inflating column sizes to account for shifting
          val expansion = cf.util.Remap(_ / highWaterMark)

          // expand all of the unfocused columns, then mostly leave them alone
          val unfocusedExpanded = unfocused map {
            case (ref, col) => ref -> expansion(col).get
          }

          val remapped: List[(ColumnRef, Column)] = focused.toList map {
            case (ColumnRef(CPath.Identity, CArrayType(tpe)), col: HomogeneousArrayColumn[a]) =>
              ???

            // because of how we have defined things, path is guaranteed NOT to be Identity now
            case (ColumnRef(path, tpe), col) =>
              val head = path.head.get
              val locus = innerIndex(head)

              // explode column and then sparsen by mod ring
              val expanded =
                expansion.andThen(cf.util.filterBy(_ % highWaterMark == locus))(col).get

              ColumnRef(path.dropPrefix(head).get, tpe) -> expanded
          }

          // put together all the same-ref columns which now are mapped to the same path
          val merged: Map[ColumnRef, Column] = remapped.groupBy(_._1).map({
            // the key here is the column ref; the value is the list of same-type pairs
            case (ref, toMerge) =>
              ref -> toMerge.map(_._2).reduce(cf.util.UnionRight(_, _).get)
          })(collection.breakOut)

          // figure out the definedness of the exploded, filtered result
          // this is necessary so we can implement inner-concat semantics
          val definedness: BitSet =
            merged.values.map(_.definedAt(0, slice.size * highWaterMark)).reduceOption(_ | _).getOrElse(new BitSet)

          // move all of our results into second index of an array
          val indexed = merged map {
            case (ColumnRef(path, tpe), col) =>
              ColumnRef(1 \: path, tpe) -> col
          }

          val refinedHeads = innerHeads collect {
            case CPathField(field) => -\/(field)
            case CPathIndex(idx) => \/-(idx)
          }

          val hasFields = refinedHeads.exists(_.isLeft)
          val hasIndices = refinedHeads.exists(_.isRight)

          // generate the field names column
          val fieldsCol = if (hasFields) {
            val loci = refinedHeads.zipWithIndex collect {
              case (-\/(_), i) => i
            } toSet

            val col = new StrColumn {
              def apply(row: Int) = {
                val -\/(back) = refinedHeads(row % refinedHeads.length)
                back
              }

              def isDefinedAt(row: Int) =
                loci(row % refinedHeads.length) && definedness(row)
            }

            Some(col)
          } else {
            None
          }

          // generate the array indices column
          val indicesCol = if (hasIndices) {
            val loci = refinedHeads.zipWithIndex collect {
              case (\/-(_), i) => i
            } toSet

            val col = new LongColumn {
              def apply(row: Int) = {
                val \/-(back) = refinedHeads(row % refinedHeads.length)
                back
              }

              def isDefinedAt(row: Int) =
                loci(row % refinedHeads.length) && definedness(row)
            }

            Some(col)
          } else {
            None
          }

          // put the fields and index columns into the same path, in the first index of the array
          val fassigned = fieldsCol.map(col => ColumnRef(CPathIndex(0), CString) -> col).toList
          val iassigned = indicesCol.map(col => ColumnRef(CPathIndex(0), CLong) -> col).toList

          // merge them together to produce the heterogeneous output
          val idCols = Map(fassigned ++ iassigned: _*)

          // put the focus prefix BACK on the results and ids (which are now in an array together)
          val focusedTransformed = (indexed ++ idCols) map {
            case (ColumnRef(path, tpe), col) =>
              ColumnRef(focus \ path, tpe) -> col
          }

          // we need to go back to our original columns and filter them by results
          // if we don't do this, the data will be highly sparse (like an outer join)
          val unfocusedTransformed = unfocusedExpanded map {
            case (ref, col) =>
              ref -> cf.util.filter(0, slice.size * highWaterMark, definedness)(col).get
          }

          // glue everything back together with the unfocused and compute the new size
          Slice(focusedTransformed ++ unfocusedTransformed, slice.size * highWaterMark)
        }
      }

      // without peaking into the effects, we don't know exactly what has happened
      // it's possible that there were no vector values, or those vectors were all
      // cardinality 0, in which case the table will have shrunk (perhaps to empty!)
      Table(slices2, UnknownSize)
    }

    /**
      * Yields a new table with distinct rows. Assumes this table is sorted.
      */
    def distinct(spec: TransSpec1): Table = {
      def distinct0[T](id: SliceTransform1[Option[Slice]], filter: SliceTransform1[T]): Table = {
        def stream(state: (Option[Slice], T), slices: StreamT[M, Slice]): StreamT[M, Slice] = StreamT(
          for {
            head <- slices.uncons

            back <- {
              head map {
                case (s, sx) => {
                  for {
                    pairPrev <- id.f(state._1, s)
                    (prevFilter, cur) = pairPrev

                    // TODO use an Applicative
                    pairNext <- filter.f(state._2, s)
                    (nextT, curFilter) = pairNext
                  } yield {
                    val next = cur.distinct(prevFilter, curFilter)

                    StreamT.Yield(next, stream((if (next.size > 0) Some(curFilter) else prevFilter, nextT), sx))
                  }
                }
              } getOrElse {
                M.point(StreamT.Done)
              }
            }
          } yield back
        )

        val slices0 = StreamT.wrapEffect(this.sort(spec) map { sorted =>
          stream((id.initial, filter.initial), sorted.slices)
        })

        Table(slices0, EstimateSize(0L, size.maxSize))
      }

      distinct0(SliceTransform.identity(None: Option[Slice]), composeSliceTransform(spec))
    }

    def takeRange(startIndex: Long, numberToTake: Long): Table = {
      def loop(stream: StreamT[M, Slice], readSoFar: Long): M[StreamT[M, Slice]] = stream.uncons flatMap {
        // Prior to first needed slice, so skip
        case Some((head, tail)) if (readSoFar + head.size) < (startIndex + 1) => loop(tail, readSoFar + head.size)
        // Somewhere in between, need to transition to splitting/reading
        case Some(_) if readSoFar < (startIndex + 1) => inner(stream, 0, (startIndex - readSoFar).toInt)
        // Read off the end (we took nothing)
        case _ => M.point(StreamT.empty[M, Slice])
      }

      def inner(stream: StreamT[M, Slice], takenSoFar: Long, sliceStartIndex: Int): M[StreamT[M, Slice]] = stream.uncons flatMap {
        case Some((head, tail)) if takenSoFar < numberToTake => {
          val needed = head.takeRange(sliceStartIndex, (numberToTake - takenSoFar).toInt)
          inner(tail, takenSoFar + (head.size - (sliceStartIndex)), 0).map(needed :: _)
        }
        case _ => M.point(StreamT.empty[M, Slice])
      }

      def calcNewSize(current: Long): Long = ((current - startIndex) max 0) min numberToTake

      val newSize = size match {
        case ExactSize(sz)            => ExactSize(calcNewSize(sz))
        case EstimateSize(sMin, sMax) => TableSize(calcNewSize(sMin), calcNewSize(sMax))
        case UnknownSize              => UnknownSize
        case InfiniteSize             => InfiniteSize
      }

      Table(StreamT.wrapEffect(loop(slices, 0)), newSize)
    }

    /**
      * In order to call partitionMerge, the table must be sorted according to
      * the values specified by the partitionBy transspec.
      */
    def partitionMerge(partitionBy: TransSpec1)(f: Table => M[Table]): M[Table] = {
      // Find the first element that compares LT
      @tailrec def findEnd(compare: Int => Ordering, imin: Int, imax: Int): Int = {
        val minOrd = compare(imin)
        if (minOrd eq EQ) {
          val maxOrd = compare(imax)
          if (maxOrd eq EQ) {
            imax + 1
          } else if (maxOrd eq LT) {
            val imid   = imin + ((imax - imin) / 2)
            val midOrd = compare(imid)
            if (midOrd eq LT) {
              findEnd(compare, imin, imid - 1)
            } else if (midOrd eq EQ) {
              findEnd(compare, imid, imax - 1)
            } else {
              sys.error("Inputs to partitionMerge not sorted.")
            }
          } else {
            sys.error("Inputs to partitionMerge not sorted.")
          }
        } else if ((minOrd eq LT) && (compare(imax) eq LT)) {
          imin
        } else {
          sys.error("Inputs to partitionMerge not sorted.")
        }
      }

      def subTable(comparatorGen: Slice => (Int => Ordering), slices: StreamT[M, Slice]): M[Table] = {
        def subTable0(slices: StreamT[M, Slice], subSlices: StreamT[M, Slice], size: Int): M[Table] = {
          slices.uncons flatMap {
            case Some((head, tail)) =>
              val headComparator = comparatorGen(head)
              val spanEnd        = findEnd(headComparator, 0, head.size - 1)
              if (spanEnd < head.size) {
                M.point(Table(subSlices ++ (head.take(spanEnd) :: StreamT.empty[M, Slice]), ExactSize(size + spanEnd)))
              } else {
                subTable0(tail, subSlices ++ (head :: StreamT.empty[M, Slice]), size + head.size)
              }

            case None =>
              M.point(Table(subSlices, ExactSize(size)))
          }
        }

        subTable0(slices, StreamT.empty[M, Slice], 0)
      }

      def dropAndSplit(comparatorGen: Slice => (Int => Ordering), slices: StreamT[M, Slice], spanStart: Int): StreamT[M, Slice] = StreamT.wrapEffect {
        slices.uncons map {
          case Some((head, tail)) =>
            val headComparator = comparatorGen(head)
            val spanEnd        = findEnd(headComparator, spanStart, head.size - 1)
            if (spanEnd < head.size) {
              stepPartition(head, spanEnd, tail)
            } else {
              dropAndSplit(comparatorGen, tail, 0)
            }

          case None =>
            StreamT.empty[M, Slice]
        }
      }

      def stepPartition(head: Slice, spanStart: Int, tail: StreamT[M, Slice]): StreamT[M, Slice] = {
        val comparatorGen = (s: Slice) => {
          val rowComparator = Slice.rowComparatorFor(head, s) { s0 =>
            s0.columns.keys collect {
              case ColumnRef(path @ CPath(CPathField("0"), _ @_ *), _) => path
            }
          }

          (i: Int) =>
            rowComparator.compare(spanStart, i)
        }

        val groupTable                       = subTable(comparatorGen, head.drop(spanStart) :: tail)
        val groupedM                         = groupTable.map(_.transform(DerefObjectStatic(Leaf(Source), CPathField("1")))).flatMap(f)
        val groupedStream: StreamT[M, Slice] = StreamT.wrapEffect(groupedM.map(_.slices))

        groupedStream ++ dropAndSplit(comparatorGen, head :: tail, spanStart)
      }

      val keyTrans = OuterObjectConcat(
        WrapObject(partitionBy, "0"),
        WrapObject(Leaf(Source), "1")
      )

      this.transform(keyTrans).compact(TransSpec1.Id).slices.uncons map {
        case Some((head, tail)) =>
          Table(stepPartition(head, 0, tail), UnknownSize)
        case None =>
          Table.empty
      }
    }

    def normalize: Table = Table(slices.filter(!_.isEmpty), size)

    def schemas: M[Set[JType]] = {

      // Returns true iff masks contains an array equivalent to mask.
      def contains(masks: List[Array[Int]], mask: Array[Int]): Boolean = {

        @tailrec
        def equal(x: Array[Int], y: Array[Int], i: Int): Boolean =
          if (i >= x.length) {
            true
          } else if (x(i) != y(i)) {
            false
          } else {
            equal(x, y, i + 1)
          }

        @tailrec
        def loop(xs: List[Array[Int]], y: Array[Int]): Boolean = xs match {
          case x :: xs if x.length == y.length && equal(x, y, 0) => true
          case _ :: xs                                           => loop(xs, y)
          case Nil                                               => false
        }

        loop(masks, mask)
      }

      def isZero(x: Array[Int]): Boolean = {
        @tailrec def loop(i: Int): Boolean =
          if (i < 0) {
            true
          } else if (x(i) != 0) {
            false
          } else {
            loop(i - 1)
          }

        loop(x.length - 1)
      }

      // Constructs a schema from a set of defined ColumnRefs. Metadata is
      // ignored and there can be no unions. The set of ColumnRefs must all be
      // defined and hence must create a valid JSON object.
      def mkSchema(cols: List[ColumnRef]): Option[JType] = {
        def leafType(ctype: CType): JType = ctype match {
          case CBoolean               => JBooleanT
          case CLong | CDouble | CNum => JNumberT
          case CString                => JTextT
          case CDate                  => JDateT
          case CPeriod                => JPeriodT
          case CArrayType(elemType)   => leafType(elemType)
          case CEmptyObject           => JObjectFixedT(Map.empty)
          case CEmptyArray            => JArrayFixedT(Map.empty)
          case CNull                  => JNullT
          case CUndefined             => sys.error("not supported")
        }

        def fresh(paths: List[CPathNode], leaf: JType): Option[JType] = paths match {
          case CPathField(field) :: paths =>
            fresh(paths, leaf) map { tpe =>
              JObjectFixedT(Map(field -> tpe))
            }
          case CPathIndex(i) :: paths =>
            fresh(paths, leaf) map { tpe =>
              JArrayFixedT(Map(i -> tpe))
            }
          case CPathArray :: paths   => fresh(paths, leaf) map (JArrayHomogeneousT(_))
          case CPathMeta(field) :: _ => None
          case Nil                   => Some(leaf)
        }

        def merge(schema: Option[JType], paths: List[CPathNode], leaf: JType): Option[JType] = (schema, paths) match {
          case (Some(JObjectFixedT(fields)), CPathField(field) :: paths) =>
            merge(fields get field, paths, leaf) map { tpe =>
              JObjectFixedT(fields + (field -> tpe))
            } orElse schema
          case (Some(JArrayFixedT(indices)), CPathIndex(idx) :: paths) =>
            merge(indices get idx, paths, leaf) map { tpe =>
              JArrayFixedT(indices + (idx -> tpe))
            } orElse schema
          case (None, paths) =>
            fresh(paths, leaf)
          case (jtype, paths) =>
            sys.error("Invalid schema.") // This shouldn't happen for any real data.
        }

        cols.foldLeft(None: Option[JType]) {
          case (schema, ColumnRef(cpath, ctype)) =>
            merge(schema, cpath.nodes, leafType(ctype))
        }
      }

      // Collects all possible schemas from some slices.
      def collectSchemas(schemas: Set[JType], slices: StreamT[M, Slice]): M[Set[JType]] = {
        def buildMasks(cols: Array[Column], sliceSize: Int): List[Array[Int]] = {
          import java.util.Arrays.copyOf
          val mask = RawBitSet.create(cols.length)

          @tailrec def build0(row: Int, masks: List[Array[Int]]): List[Array[Int]] = {
            if (row < sliceSize) {
              RawBitSet.clear(mask)

              var j = 0
              while (j < cols.length) {
                if (cols(j) isDefinedAt row) RawBitSet.set(mask, j)
                j += 1
              }

              build0(row + 1, if (!contains(masks, mask) && !isZero(mask)) copyOf(mask, mask.length) :: masks else masks)
            } else masks
          }

          build0(0, Nil)
        }

        slices.uncons flatMap {
          case Some((slice, slices)) =>
            val (refs0, cols0) = slice.columns.unzip

            val masks                        = buildMasks(cols0.toArray, slice.size)
            val refs: List[(ColumnRef, Int)] = refs0.zipWithIndex.toList
            val next = masks flatMap { schemaMask =>
              mkSchema(refs collect { case (ref, i) if RawBitSet.get(schemaMask, i) => ref })
            }

            collectSchemas(schemas ++ next, slices)

          case None =>
            M.point(schemas)
        }
      }

      collectSchemas(Set.empty, slices)
    }

    def renderJson(prefix: String = "", delimiter: String = "\n", suffix: String = ""): StreamT[M, CharBuffer] =
      ColumnarTableModule.renderJson(slices, prefix, delimiter, suffix)

    def renderCsv(): StreamT[M, CharBuffer] =
      ColumnarTableModule.renderCsv(slices)

    def slicePrinter(prelude: String)(f: Slice => String): Table = {
      Table(
        StreamT(
          StreamT
            .Skip({
            println(prelude);
            slices map { s =>
              println(f(s)); s
            }
          })
            .point[M]),
        size)
    }

    def logged(logger: Logger, logPrefix: String = "", prelude: String = "", appendix: String = "")(f: Slice => String): Table = {
      val preludeEffect  = StreamT(StreamT.Skip({ logger.debug(logPrefix + " " + prelude); StreamT.empty[M, Slice] }).point[M])
      val appendixEffect = StreamT(StreamT.Skip({ logger.debug(logPrefix + " " + appendix); StreamT.empty[M, Slice] }).point[M])
      val sliceEffect = if (logger.isTraceEnabled) slices map { s =>
        logger.trace(logPrefix + " " + f(s)); s
      } else slices
      Table(preludeEffect ++ sliceEffect ++ appendixEffect, size)
    }

    def printer(prelude: String = "", flag: String = ""): Table = slicePrinter(prelude)(s => s.toJsonString(flag))

    def toStrings: M[Iterable[String]] = {
      toEvents { (slice, row) =>
        slice.toString(row)
      }
    }

    def toJson: M[Iterable[JValue]] = {
      toEvents { (slice, row) =>
        slice.toJson(row)
      }
    }

    private def toEvents[A](f: (Slice, RowId) => Option[A]): M[Iterable[A]] = {
      for (stream <- self.compact(Leaf(Source)).slices.toStream) yield {
        for (slice <- stream; i <- 0 until slice.size; a <- f(slice, i)) yield a
      }
    }

    def metrics = TableMetrics(readStarts.get, blockReads.get)
  }
}
