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
package table

import quasar.blueeyes._
import quasar.contrib.fs2.convert
import quasar.contrib.std.errorImpossible
import quasar.precog.BitSet
import quasar.precog.common._
import quasar.precog.util.RawBitSet
import quasar.yggdrasil.bytecode._
import quasar.yggdrasil.util._
import quasar.yggdrasil.table.cf.util.{ Remap, Empty }
import quasar.yggdrasil.table.ctrie._

import qdata.QDataDecode
import qdata.time.{DateTimeInterval, OffsetDate}

import cats.effect.{IO, LiftIO}

import TransSpecModule._
import org.slf4j.Logger
import org.slf4s.Logging
import quasar.precog.util.IOUtils
import scalaz._, Scalaz._, Ordering._
import shims._

import java.io.File
import java.nio.CharBuffer
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}

import scala.annotation.tailrec
import scala.collection.immutable.Set
import scala.concurrent.ExecutionContext

trait ColumnarTableTypes {
  type Reducer[α] = CReducer[α]
  type RowId      = Int
}

trait ColumnarTableModuleConfig {
  def maxSliceRows: Int

  // This is a slice size that we'd like our slices to be at least as large as.
  def minIdealSliceRows: Int = maxSliceRows / 4

  // This is what we consider a "small" slice. This may affect points where
  // we take proactive measures to prevent problems caused by small slices.
  def smallSliceRows: Int

  def maxSaneCrossRows: Long = 2400000000L // 2.4 billion
}

object ColumnarTableModule extends Logging {

  // shadow instance of scalaz type classes for `Id`,
  // because with shims we have ambiguous instances
  // the right-hand side is necessary to prevent `idInstance`
  // from being marked unused.
  private val idInstance: Int = idInstance + 1

  def renderJson(slices: StreamT[IO, Slice], prefix: String, delimiter: String, suffix: String, precise: Boolean): StreamT[IO, CharBuffer] = {
    def wrap(stream: StreamT[IO, CharBuffer]) = {
      if (prefix == "" && suffix == "") stream
      else if (suffix == "") CharBuffer.wrap(prefix) :: stream
      else if (prefix == "") stream ++ (CharBuffer.wrap(suffix) :: StreamT.empty[IO, CharBuffer])
      else CharBuffer.wrap(prefix) :: (stream ++ (CharBuffer.wrap(suffix) :: StreamT.empty[IO, CharBuffer]))
    }

    def foldFlatMap(slices: StreamT[IO, Slice], rendered: Boolean): StreamT[IO, CharBuffer] = {
      StreamT[IO, CharBuffer](slices.step map {
        case StreamT.Yield(slice, tail) =>
          val (seq, rendered2) = slice.renderJson(delimiter, precise)
          val stream = StreamT.fromIterable(seq).trans(λ[Id ~> IO](IO.pure(_)))

          val stream2 = if (rendered && rendered2)
            CharBuffer.wrap(delimiter) :: stream
          else
            stream

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
    *
    * If `assumeHomogeneous` is true, the columns from the first slice
    * will dictate the header only one column from a polymorphic
    * `ColumnRef` locus (e.g. a field which can be String or Long) will
    * be used. If `assumeHomogeneous` is false, then the dataset will be
    * fully traversed to infer the schema, which will then be used to
    * render the CSV bytes in a second traversal.
    *
    * If `assumeHomogeneous` is true and the data is *not* homogeneous,
    * the results are going to be quite bizarre. You're almost certainly
    * going to see some missing columns, some map deref errors as you
    * cross slice boundaries, etc. It's just bad. The only for of
    * heterogeneity which is allowed when `assumeHomogeneous` is true is
    * holes in data. In other words, you can have columns which are
    * undefined for a particular row, so long as it's the same column set
    * throughout the entire dataset *and* so long as you don't have any
    * locus conflicts (i.e. a column which could be String or Long).
    */
  def renderCsv(slices: StreamT[IO, Slice], assumeHomogeneous: Boolean): StreamT[IO, CharBuffer] = {
    val schemaFirstTailOptM: IO[Option[(Map[CPath, Set[CType]], Slice, StreamT[IO, Slice])]] = {
      if (assumeHomogeneous) {
        slices.uncons map {
          case Some((head, tail)) =>
            Some((head.groupedColumnRefs, head, tail))

          case None =>
            None
        }
      } else {
        (groupedColumnRefs(slices) |@| slices.uncons) {
          case (schema, Some((head, tail))) =>
            Some((schema, head, tail))

          case _ =>
            None
        }
      }
    }

    StreamT wrapEffect {
      schemaFirstTailOptM map {
        case Some((groupedRefs, head, tail)) =>
          val schema: List[List[ColumnRef]] = {
            groupedRefs.toList.sortBy(_._1) map {
              case (path, tpes) => tpes.toList.map(ColumnRef(path, _))
            }
          }

          val schemaRenders = {
            schema map {
              // render the first path only
              case ColumnRef(path, _) :: _ =>
                val candidateHead = path.nodes.head match {
                  case CPathField(name) => name
                  case CPathMeta(name) => name
                  case CPathIndex(index) => index
                  case CPathArray => errorImpossible
                }

                val candidateTail = path.nodes.tail.map(_.toString).mkString

                val candidate = candidateHead + candidateTail

                if (candidate.indexOf('"') >= 0 ||
                    candidate.indexOf('\n') >= 0 ||
                    candidate.indexOf('\r') >= 0 ||
                    candidate.indexOf(',') >= 0) {

                  "\"" + candidate.replace("\"", "\"\"") + "\""
                } else {
                  candidate
                }

              case Nil => sys.error("list of schema candidates was empty")
            }
          }

          val schemaRender =
            CharBuffer.wrap(schemaRenders.mkString("", ",", "\r\n"))

          val bodyRender = (head :: tail) flatMap { slice =>
            StreamT.fromIterable(slice.renderCsv(schema, assumeHomogeneous)).trans(λ[Id ~> IO](IO.pure(_)))
          }

          schemaRender :: bodyRender

        case None => StreamT.empty[IO, CharBuffer]
      }
    }
  }

  // I love how StreamT#foldMap does almost exactly the opposite of what anyone sane would want
  def groupedColumnRefs(slices: StreamT[IO, Slice]): IO[Map[CPath, Set[CType]]] =
    slices.foldLeftRec(Map[CPath, Set[CType]]())(_ |+| _.groupedColumnRefs)
}

trait ColumnarTableModule
    extends TableModule
    with ColumnarTableTypes
    with IdSourceScannerModule
    with SliceTransforms
    with SamplableColumnarTableModule
    with IndicesModule {

  // shadow instance of scalaz type classes for `Id`,
  // because with shims we have ambiguous instances
  // the right-hand side is necessary to prevent `idInstance`
  // from being marked unused.
  private val idInstance: Int = idInstance + 1

  import TableModule._
  import trans.{Range => _, _}

  type Table <: ColumnarTable
  type TableCompanion <: ColumnarTableCompanion

  def newScratchDir(): File = IOUtils.createTmpDir("ctmscratch").unsafePerformIO
  def jdbmCommitInterval: Long = 200000l

  trait ColumnarTableCompanion extends TableCompanionLike {
    def apply(slices: StreamT[IO, Slice], size: TableSize): Table

    def singleton(slice: Slice): Table

    def empty: Table = Table(StreamT.empty[IO, Slice], ExactSize(0))

    def uniformDistribution(init: MmixPrng): Table = {
      val gen: StreamT[IO, Slice] = StreamT.unfoldM[IO, Slice, MmixPrng](init) { prng =>
        val (column, nextGen) = Column.uniformDistribution(prng)
        (Slice(Config.maxSliceRows, Map(ColumnRef(CPath.Identity, CDouble) -> column)), nextGen).some.point[IO]
      }

      Table(gen, InfiniteSize)
    }

    def constBoolean(v: Set[Boolean]): Table = {
      val column = ArrayBoolColumn(v.toArray)
      Table(Slice(v.size, Map(ColumnRef(CPath.Identity, CBoolean) -> column)) :: StreamT.empty[IO, Slice], ExactSize(v.size))
    }

    def constLong(v: Set[Long]): Table = {
      val column = ArrayLongColumn(v.toArray)
      Table(Slice(v.size, Map(ColumnRef(CPath.Identity, CLong) -> column)) :: StreamT.empty[IO, Slice], ExactSize(v.size))
    }

    def constDouble(v: Set[Double]): Table = {
      val column = ArrayDoubleColumn(v.toArray)
      Table(Slice(v.size, CTrie(ColumnRef(CPath.Identity, CDouble) -> column)) :: StreamT.empty[IO, Slice], ExactSize(v.size))
    }

    def constDecimal(v: Set[BigDecimal]): Table = {
      val column = ArrayNumColumn(v.toArray)
      Table(Slice(v.size, Map(ColumnRef(CPath.Identity, CNum) -> column)) :: StreamT.empty[IO, Slice], ExactSize(v.size))
    }

    def constString(v: Set[String]): Table = {
      val column = ArrayStrColumn(v.toArray)
      Table(Slice(v.size, Map(ColumnRef(CPath.Identity, CString) -> column)) :: StreamT.empty[IO, Slice], ExactSize(v.size))
    }

    def constOffsetDateTime(v: Set[OffsetDateTime]): Table = {
      val column = ArrayOffsetDateTimeColumn(v.toArray)
      Table(Slice(v.size, Map(ColumnRef(CPath.Identity, COffsetDateTime) -> column)) :: StreamT.empty[IO, Slice], ExactSize(v.size))
    }

    def constOffsetTime(v: Set[OffsetTime]): Table = {
      val column = ArrayOffsetTimeColumn(v.toArray)
      Table(Slice(v.size, Map(ColumnRef(CPath.Identity, COffsetTime) -> column)) :: StreamT.empty[IO, Slice], ExactSize(v.size))
    }

    def constOffsetDate(v: Set[OffsetDate]): Table = {
      val column = ArrayOffsetDateColumn(v.toArray)
      Table(Slice(v.size, Map(ColumnRef(CPath.Identity, COffsetDate) -> column)) :: StreamT.empty[IO, Slice], ExactSize(v.size))
    }

    def constLocalDateTime(v: Set[LocalDateTime]): Table = {
      val column = ArrayLocalDateTimeColumn(v.toArray)
      Table(Slice(v.size, Map(ColumnRef(CPath.Identity, CLocalDateTime) -> column)) :: StreamT.empty[IO, Slice], ExactSize(v.size))
    }

    def constLocalTime(v: Set[LocalTime]): Table = {
      val column = ArrayLocalTimeColumn(v.toArray)
      Table(Slice(v.size, Map(ColumnRef(CPath.Identity, CLocalTime) -> column)) :: StreamT.empty[IO, Slice], ExactSize(v.size))
    }

    def constLocalDate(v: Set[LocalDate]): Table = {
      val column = ArrayLocalDateColumn(v.toArray)
      Table(Slice(v.size, Map(ColumnRef(CPath.Identity, CLocalDate) -> column)) :: StreamT.empty[IO, Slice], ExactSize(v.size))
    }

    def constInterval(v: Set[DateTimeInterval]): Table = {
      val column = ArrayIntervalColumn(v.toArray)
      Table(Slice(v.size, Map(ColumnRef(CPath.Identity, CInterval) -> column)) :: StreamT.empty[IO, Slice], ExactSize(v.size))
    }

    def constNull: Table =
      Table(Slice(1, Map(ColumnRef(CPath.Identity, CNull) -> new InfiniteColumn with NullColumn)) :: StreamT.empty[IO, Slice], ExactSize(1))

    def constEmptyObject: Table =
      Table(Slice(1, Map(ColumnRef(CPath.Identity, CEmptyObject) -> new InfiniteColumn with EmptyObjectColumn)) :: StreamT.empty[IO, Slice], ExactSize(1))

    def constEmptyArray: Table =
      Table(Slice(1, Map(ColumnRef(CPath.Identity, CEmptyArray) -> new InfiniteColumn with EmptyArrayColumn)) :: StreamT.empty[IO, Slice], ExactSize(1))

    def transformStream[A](sliceTransform: SliceTransform1[A], slices: StreamT[IO, Slice]): StreamT[IO, Slice] = {
      def stream(state: A, slices: StreamT[IO, Slice]): StreamT[IO, Slice] = StreamT(
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
              IO.pure(StreamT.Done)
            }
          }
        } yield back
      )

      stream(sliceTransform.initial, slices)
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

    def fromQDataStream[M[_]: Monad: MonadFinalizers[?[_], IO]: LiftIO, A: QDataDecode](
        values: fs2.Stream[IO, A])(
        implicit ec: ExecutionContext)
        : M[Table] = {
      val sliceStream = Slice.allFromQData(values)

      for {
        d <- LiftIO[M].liftIO(convert.toStreamT(sliceStream))

        _ <- MonadFinalizers[M, IO].tell(List(d.dispose))

        slices = d.unsafeValue
      } yield Table(slices, UnknownSize)
    }

    @deprecated("Use fromQDataStream", "52.0.2")
    def fromRValues(values: Stream[RValue], maxSliceRows: Option[Int] = None): Table = {
      val sliceSize = maxSliceRows.getOrElse(Config.maxSliceRows)

      def makeSlice(data: Stream[RValue]): (Slice, Stream[RValue]) = {
        val (prefix, suffix) = data.splitAt(sliceSize)

        (Slice.fromRValues(prefix), suffix)
      }

      Table(
        StreamT.unfoldM(values) { events =>
          IO(
            (!events.isEmpty) option {
              makeSlice(events.toStream)
            }
          )
        },
        ExactSize(values.length))
    }

    def join(left: Table, right: Table, orderHint: Option[JoinOrder] = None)(leftKeySpec: TransSpec1,
                                                                             rightKeySpec: TransSpec1,
                                                                             joinSpec: TransSpec2): IO[(JoinOrder, Table)] = {
      val emptySpec = trans.ConstLiteral(CEmptyArray, Leaf(Source))
      for {
        left0 <- left.sort(leftKeySpec)
        right0 <- right.sort(rightKeySpec)
        cogrouped = left0.cogroup(leftKeySpec, rightKeySpec, right0)(emptySpec, emptySpec, trans.WrapArray(joinSpec))
      } yield {
        JoinOrder.KeyOrder -> cogrouped.transform(trans.DerefArrayStatic(Leaf(Source), CPathIndex(0)))
      }
    }

    def cross(left: Table, right: Table, orderHint: Option[CrossOrder] = None)(spec: TransSpec2): IO[(CrossOrder, Table)] = {
      import CrossOrder._
      IO(orderHint match {
        case Some(CrossRight | CrossRightLeft) =>
          CrossRight -> right.cross(left)(TransSpec2.flip(spec))
        case _ =>
          CrossLeft -> left.cross(right)(spec)
      })
    }
  }

  abstract class ColumnarTable(slices0: StreamT[IO, Slice], val size: TableSize)
    extends TableLike with SamplableColumnarTable { self: Table =>
    import SliceTransform._

    private final val readStarts = new java.util.concurrent.atomic.AtomicInteger
    private final val blockReads = new java.util.concurrent.atomic.AtomicInteger

    val slices: StreamT[IO, Slice] = StreamT(
      IO(
        StreamT.Skip({
          readStarts.getAndIncrement
          slices0.map(s => { blockReads.getAndIncrement; s })
        })
      )
    )

    /**
      * Folds over the table to produce a single value (stored in a singleton table).
      */
    def reduce[A](reducer: Reducer[A])(implicit monoid: Monoid[A]): IO[A] = {
      def rec(stream: StreamT[IO, A], acc: A): IO[A] = {
        stream.uncons flatMap {
          case Some((head, tail)) => rec(tail, head |+| acc)
          case None               => IO.pure(acc)
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

    def force: IO[Table] = {
      def loop(slices: StreamT[IO, Slice], acc: List[Slice], size: Long): IO[(List[Slice], Long)] = slices.uncons flatMap {
        case Some((slice, tail)) if slice.size > 0 =>
          loop(tail, slice.materialized :: acc, size + slice.size)
        case Some((_, tail)) =>
          loop(tail, acc, size)
        case None =>
          IO.pure((acc.reverse, size))
      }
      val former = λ[Id.Id ~> IO](IO.pure(_))
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

          IO.pure(back)
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
    def zip(t2: Table): IO[Table] = {
      def rec(slices1: StreamT[IO, Slice], slices2: StreamT[IO, Slice]): StreamT[IO, Slice] = {
        StreamT(slices1.uncons flatMap {
          case Some((head1, tail1)) =>
            slices2.uncons map {
              case Some((head2, tail2)) =>
                StreamT.Yield(head1 zip head2, rec(tail1, tail2))
              case None =>
                StreamT.Done
            }

          case None =>
            IO.pure(StreamT.Done)
        })
      }

      val resultSize = EstimateSize(0, size.maxSize min t2.size.maxSize)
      IO(Table(rec(slices, t2.slices), resultSize))

      // todo investigate why the code below makes all of RandomLibSpecs explode
      // val resultSlices = Apply[({ type l[a] = StreamT[IO, a] })#l].zip.zip(slices, t2.slices) map { case (s1, s2) => s1.zip(s2) }
      // Table(resultSlices, resultSize)
    }

    def toArray[A](implicit tpe: CValueType[A]): Table = {
      val slices2: StreamT[IO, Slice] = slices map { _.toArray[A] }
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
        case Nil          => Slice.empty
        case slice :: Nil => slice
        case slices =>
          val slice = Slice.concat(slices)
          if (slices.size > (slice.size / Config.smallSliceRows)) {
            slice.materialized // Deal w/ lots of small slices by materializing them.
          } else {
            slice
          }
      }

      def step(sliceSize: Int, acc: List[Slice], stream: StreamT[IO, Slice]): IO[StreamT.Step[Slice, StreamT[IO, Slice]]] = {
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
                IO(StreamT.Yield(slice, StreamT(step(0, Nil, suffix :: tail))))
              } else {
                val slice = concat(head :: acc)
                IO(StreamT.Yield(slice, StreamT(step(0, Nil, tail))))
              }

            } else {
              // Just keep swimming (aka accumulating).
              step(sliceSize + head.size, head :: acc, tail)
            }

          case None =>
            if (sliceSize > 0) {
              IO(StreamT.Yield(concat(acc), StreamT.empty[IO, Slice]))
            } else {
              IO.pure(StreamT.Done)
            }
        }
      }

      Table(StreamT(step(0, Nil, slices)), size)
    }

    /**
      * Cogroups this table with another table, using equality on the specified
      * transformation on rows of the table.
      */
    def cogroup(
      leftKey: TransSpec1,
      rightKey: TransSpec1,
      that: Table)(
      leftResultTrans: TransSpec1,
      rightResultTrans: TransSpec1,
      bothResultTrans: TransSpec2)
    : Table = {

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
                                  bothTransform: SliceTransform2[BR]): IO[(Slice, LR, RR, BR)] = {

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
                                  tail: StreamT[IO, Slice])

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
        case class EndLeft(lr: LR, lhead: Slice, ltail: StreamT[IO, Slice]) extends CogroupState
        case class Cogroup(lr: LR,
                           rr: RR,
                           br: BR,
                           left: SlicePosition[LK],
                           right: SlicePosition[RK],
                           rightStart: Option[SlicePosition[RK]],
                           rightEnd: Option[SlicePosition[RK]])
            extends CogroupState
        case class EndRight(rr: RR, rhead: Slice, rtail: StreamT[IO, Slice]) extends CogroupState
        case object CogroupDone extends CogroupState

        // step is the continuation function fed to uncons. It is called once for each emitted slice
        def step(state: CogroupState): IO[Option[(Slice, CogroupState)]] = {

          // step0 is the inner monadic recursion needed to cross slice boundaries within the emission of a slice
          def step0(lr: LR,
                    rr: RR,
                    br: BR,
                    leftPosition: SlicePosition[LK],
                    rightPosition: SlicePosition[RK],
                    rightStart0: Option[SlicePosition[RK]],
                    rightEnd0: Option[SlicePosition[RK]])(
              ibufs: IndexBuffers = new IndexBuffers(leftPosition.key.size, rightPosition.key.size)): IO[Option[(Slice, CogroupState)]] = {

            val SlicePosition(lSliceId, lpos0, lkstate, lkey, lhead, ltail) = leftPosition
            val SlicePosition(rSliceId, rpos0, rkstate, rkey, rhead, rtail) = rightPosition

            val comparator = Slice.rowComparatorFor(lkey, rkey) {
              // since we've used the key transforms, and since transforms are contractually
              // forbidden from changing slice size, we can just use all
              _.columns.keys map (_.selector)
            }

            // the inner tight loop; this will recur while we're within the bounds of
            // a pair of slices. Any operation that must cross slice boundaries
            // must exit this inner loop and recur through the outer monadic loop
            // xrstart is an int with sentinel value for efficiency, but is Option at the slice level.
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
                      // Workaround in the following 3 cases:
                      // Comparator also returns EQ if both are undefined
                      // However we don't want to treat these as EQ because that
                      // would *multiply* left rows with right rows.
                      // We want to *sum* them instead. We can accomplish by
                      // treating 2 undefineds as if they were LT
                      case EQ if lkey.isDefinedAt(lpos) =>
                        ibufs.advanceBoth(lpos, rpos)
                        buildRemappings(lpos, rpos + 1, rightStart, rightEnd, endRight)
                      // .. so we treat 2 undefineds as LT instead:
                      case EQ | LT if rightStartSliceId == rSliceId =>
                        buildRemappings(lpos + 1, rightStartPos, rightStart, Some(rightPosition.copy(pos = rpos)), endRight)
                      case EQ | LT =>
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
                      // Workaround in the following 2 cases:
                      // Comparator also returns EQ if both are undefined
                      // However we don't want to treat these as EQ because that
                      // would *multiply* left rows with right rows.
                      // We want to *sum* them instead. We can accomplish by
                      // treating 2 undefineds as if they were LT
                      case EQ if lkey.isDefinedAt(lpos) =>
                        ibufs.advanceBoth(lpos, rpos)
                        buildRemappings(lpos, rpos + 1, Some(rightPosition.copy(pos = rpos)), None, endRight)
                      case EQ | LT =>
                        ibufs.advanceLeft(lpos)
                        buildRemappings(lpos + 1, rpos, None, None, endRight)
                      case GT =>
                        ibufs.advanceRight(rpos)
                        buildRemappings(lpos, rpos + 1, None, None, endRight)
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

            def continue(nextStep: NextStep[LK, RK]): IO[Option[(Slice, CogroupState)]] = nextStep match {
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
                        IO.pure(Some(completeSlice -> nextState))
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
                        IO.pure(Some(completeSlice -> nextState))
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

            case CogroupDone => IO.pure(None)
          }
        } // end of step

        val initialState = for {
          leftUnconsed <- self.slices.uncons
          rightUnconsed <- that.slices.uncons

          back <- {
            val cogroup = for {
              lp <- leftUnconsed
              rp <- rightUnconsed
              (leftHead, leftTail) = lp
              (rightHead, rightTail) = rp
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
              } map { IO.pure }
            } orElse {
              rightUnconsed map {
                case (head, tail) => EndRight(strr.initial, head, tail)
              } map { IO.pure }
            }

            optM map { m =>
              m map { _.some }
            } getOrElse {
              IO.pure(none)
            }
          }
        } yield back

        Table(StreamT.wrapEffect(initialState map { state =>
          StreamT.unfoldM[IO, Slice, CogroupState](state getOrElse CogroupDone)(step(_))
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
      def cross0[A](transform: SliceTransform2[A]): IO[StreamT[IO, Slice]] = {
        case class CrossState(a: A, position: Int, tail: StreamT[IO, Slice])

        def crossBothSingle(lhead: Slice, rhead: Slice)(a0: A): IO[(A, StreamT[IO, Slice])] = {
          // We try to fill out the slices as much as possible, so we work with
          // several rows from the left at a time.

          val lrowsPerSlice = math.max(1, Config.maxSliceRows / rhead.size)
          val sliceSize     = lrowsPerSlice * rhead.size

          // Note that this is still memory efficient, as the columns are re-used
          // between all slices.

          val results = (0 until lhead.size by lrowsPerSlice).foldLeft(IO.pure((a0, List.empty[Slice]))) {
            case (accM, offset) =>
              accM flatMap {
                case (a, acc) =>
                  val rows = math.min(sliceSize, (lhead.size - offset) * rhead.size)

                  val lslice = Slice(
                    rows,
                    lhead.columns.lazyMapValues(Remap({ i =>
                      offset + (i / rhead.size)
                    })(_).get))

                  val rslice = Slice(
                    rows,
                    if (rhead.size == 0)
                      rhead.columns.lazyMapValues(Empty(_).get)
                    else
                      rhead.columns.lazyMapValues(Remap(_ % rhead.size)(_).get))

                  transform.f(a, lslice, rslice) map {
                    case (b, resultSlice) =>
                      (b, resultSlice :: acc)
                  }
              }
          }

          results map {
            case (a1, slices) =>
              val sliceStream = slices.reverse.toStream
              (a1, StreamT.fromStream(IO.pure(sliceStream)))
          }
        }

        def crossLeftSingle(lhead: Slice, right: StreamT[IO, Slice])(a0: A): StreamT[IO, Slice] = {
          def step(state: CrossState): IO[Option[(Slice, CrossState)]] = {
            if (state.position < lhead.size) {
              state.tail.uncons flatMap {
                case Some((rhead, rtail0)) =>
                  val lslice = Slice(
                    rhead.size,
                    lhead.columns.lazyMapValues(Remap(i => state.position)(_).get))

                  transform.f(state.a, lslice, rhead) map {
                    case (a0, resultSlice) =>
                      Some((resultSlice, CrossState(a0, state.position, rtail0)))
                  }

                case None =>
                  step(CrossState(state.a, state.position + 1, right))
              }
            } else {
              IO.pure(None)
            }
          }

          StreamT.unfoldM(CrossState(a0, 0, right))(step _)
        }

        def crossRightSingle(left: StreamT[IO, Slice], rhead: Slice)(a0: A): StreamT[IO, Slice] = {
          StreamT(left.uncons flatMap {
            case Some((lhead, ltail0)) =>
              crossBothSingle(lhead, rhead)(a0) map {
                case (a1, prefix) =>
                  StreamT.Skip(prefix ++ crossRightSingle(ltail0, rhead)(a1))
              }

            case None =>
              IO.pure(StreamT.Done)
          })
        }

        def crossBoth(ltail: StreamT[IO, Slice], rtail: StreamT[IO, Slice]): StreamT[IO, Slice] = {
          // This doesn't carry the Transform's state around, so, I think it is broken.
          ltail.flatMap(crossLeftSingle(_, rtail)(transform.initial))
        }

        // We canonicalize the tables so that no slices are too small.
        val left  = this.canonicalize(Config.minIdealSliceRows, Some(Config.maxSliceRows))
        val right = that.canonicalize(Config.minIdealSliceRows, Some(Config.maxSliceRows))

        (left.slices.uncons |@| right.slices.uncons).tupled flatMap {
          case (Some((lhead, ltail)), Some((rhead, rtail))) =>
            (ltail.uncons |@| rtail.uncons).tupled flatMap {
              case (None, None) =>
                // both are small sets, so find the cross in memory
                crossBothSingle(lhead, rhead)(transform.initial) map { _._2 }

              case (None, Some((rthead, rttail))) =>
                // left side is a small set, so restart it in memory
                IO(crossLeftSingle(lhead, rhead :: rthead :: rttail)(transform.initial))

              case (Some((lthead, lttail)), None) =>
                // right side is a small set, so restart it in memory
                IO(crossRightSingle(lhead :: lthead :: lttail, rhead)(transform.initial))

              case (Some((lthead, lttail)), Some((rthead, rttail))) =>
                // both large sets, so just walk the left restarting the right.
                IO(crossBoth(lhead :: lthead :: lttail, rhead :: rthead :: rttail))
            }

          case _ => IO.pure(StreamT.empty[IO, Slice])
        }
      }

      // TODO: We should be able to fully compute the size of the result above.
      val newSize = (size, that.size) match {
        case (ExactSize(l), ExactSize(r))         => TableSize(l max r, l * r)
        case (EstimateSize(ln, lx), ExactSize(r)) => TableSize(ln max r, lx * r)
        case (ExactSize(l), EstimateSize(rn, rx)) => TableSize(l max rn, l * rx)
        case _                                    => UnknownSize // Bail on anything else for now (see above TODO)
      }

      Table(StreamT(cross0(composeSliceTransform2(spec)) map { tail =>
        StreamT.Skip(tail)
      }), newSize)
    }

    def leftShift(focus: CPath, emitOnUndef: Boolean): Table = {
      def lens(slice: Slice): (Map[ColumnRef, Column], Map[ColumnRef, Column]) = {
        val (focused: Map[ColumnRef, Column], unfocused: Map[ColumnRef, Column]) =
          slice.columns.partition(_._1.selector.hasPrefix(focus))

        // partition by elems being flattenable or not
        val (flattenable, unflattenable) = focused partition {
          case (ColumnRef(`focus`, CArrayType(_)), _) => true
          case (ColumnRef(`focus`, _), _) => false
          case _ => true
        }

        // remap focused, flattenables to be at "root"
        val remapped = flattenable map {
          case (ColumnRef(path, tpe), col) =>
            (ColumnRef(path.dropPrefix(focus).get, tpe), col)
        }

        (remapped, unfocused)
      }

      def merge(
          focused: Map[ColumnRef, Column],
          innerIndex: Map[CPathNode, Int],
          expansion: CF1,
          highWaterMark: Int)
          : Map[ColumnRef, Column] = {

        val remapped: List[(ColumnRef, Int, Column)] = focused.toList map {
          case (ColumnRef(CPath.Identity, CArrayType(tpe)), col: HomogeneousArrayColumn[a]) =>
            errorImpossible

          // because of how we have defined things, path is guaranteed NOT to be Identity now
          case (ColumnRef(path, tpe), col) =>
            (ColumnRef(path.tail, tpe), innerIndex(path.head.get), col)
        }

        // put together all the same-ref columns which now are mapped to the same path
        remapped.groupBy(_._1).map({
          // we just expanded, so this should ALWAYS be hit in the singleton case
          case (ref, List((_, locus, col: BitsetColumn))) =>
            // we materialized so this is safe
            val bs = col.definedAt
            val sparsened = bs.sparsenByMod(locus, highWaterMark)

            // explode column and then sparsen by mod ring
            val expanded =
              expansion.andThen(cf.util.filterExclusive(sparsened))(col).get

            ref -> expanded

          // the key here is the column ref; the value is the list of same-type triples
          case (ref, toMerge) =>
            val cols = new Array[Column](highWaterMark)

            // it's just easier to represent things this way
            toMerge foreach {
              case (_, idx, col) => cols(idx) = col
            }

            ref -> cf.util.ModUnion(ref.ctype, cols)
        })(collection.breakOut)
      }

      def idColumns(
          innerHeads: Vector[CPathNode],
          pathNode: CPathNode,
          definedness: BitSet)
          : Map[ColumnRef, Column] = {

        val refinedHeads: Vector[String \/ Int] = innerHeads collect {
          case CPathField(field) => -\/(field)
          case CPathIndex(idx) => \/-(idx)
        }

        val hasFields: Boolean = refinedHeads.exists(_.isLeft)
        val hasIndices: Boolean = refinedHeads.exists(_.isRight)

        // generate the field names column
        val fieldsCol: Option[Column] = if (hasFields) {
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
        val indicesCol: Option[Column] = if (hasIndices) {
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
        val fassigned: List[(ColumnRef, Column)] =
          fieldsCol.map(col => ColumnRef(pathNode, CString) -> col).toList
        val iassigned: List[(ColumnRef, Column)] =
          indicesCol.map(col => ColumnRef(pathNode, CLong) -> col).toList

        // merge them together to produce the heterogeneous output
        CTrie(fassigned ++ iassigned: _*)
      }

      def leftShiftFocused(
          merged: Map[ColumnRef, Column],
          innerHeads: Vector[CPathNode],
          definedness: BitSet)
          : Map[ColumnRef, Column] = {

        // move all of our results into index 1 of an array
        val indexed: Map[ColumnRef, Column] = merged map {
          case (ColumnRef(path, tpe), col) =>
            ColumnRef(1 \: path, tpe) -> col
        }

        // .. and our ids into index 0 of an array
        val idCols: Map[ColumnRef, Column] = idColumns(innerHeads, CPathIndex(0), definedness)

        // put the focus prefix BACK on the results and ids (which are now in an array together)
        (indexed ++ idCols) map {
          case (ColumnRef(path, tpe), col) =>
            ColumnRef(focus \ path, tpe) -> col
        }
      }

      def leftShiftUnfocused(
          unfocused: Map[ColumnRef, Column],
          definedness: BitSet,
          size: Int,
          expansion: CF1,
          highWaterMark: Int)
          : Map[ColumnRef, Column] = {

        // expand all of the unfocused columns
        val unfocusedExpanded: Map[ColumnRef, Column] = unfocused map {
          case (ref, col) => ref -> expansion(col).get
        }

        // we need to go back to our original columns and filter them by results
        // if we don't do this, the data will be highly sparse (like an outer join)
        unfocusedExpanded map {
          case (ref, col) =>
            ref -> cf.util.filter(0, size * highWaterMark, definedness)(col).get
        }
      }

      val slices2: StreamT[IO, Slice] = slices flatMap { slice0 =>
        // we have to compact since some of our masking will "resurrect" fully-undefined rows (see below)
        val slice = slice0.compact(slice0, AnyDefined)

        val (focused, unfocused) = lens(slice)

        val innerHeads: Vector[CPathNode] = {
          val unsorted = focused.keys.toVector flatMap {
            case ColumnRef(path, _) => path.head.toVector
          }

          // make sure there's at least 1 element when emitOnUndef
          val unsorted0 =
            if (unsorted.isEmpty && emitOnUndef)
              Vector(CPathIndex(0))
            else
              unsorted

          // sort the index lexicographically except in the case of indices
          // also: indices come first
          unsorted0 sortWith {
            case (CPathIndex(i1), CPathIndex(i2)) => i1 < i2
            case (CPathIndex(i1), p2) => true
            case (p1, CPathIndex(i2)) => false
            case (p1, p2) => p1.toString < p2.toString
          }
        }

        val innerIndex: Map[CPathNode, Int] = Map(innerHeads.zipWithIndex: _*)

        val primitiveWaterMarks: List[Int] = focused.toList collect {
          case (ColumnRef(CPath.Identity, CArrayType(_)), col: HomogeneousArrayColumn[a]) =>
            (0 until slice.size).map(col(_).length).max
        }

        // .max doesn't work, because Scala doesn't understand monoids
        val primitiveMax: Int = primitiveWaterMarks.fold(0)(math.max)

        // TODO doesn't handle the case where we have a sparse array with a missing column!
        // this value may be 0 if we're looking at CEmptyObject | CEmptyArray
        // and emitOnUndef = false
        val highWaterMark: Int =
          math.max(innerHeads.length, primitiveMax)

        val resplit: Vector[Slice] = if (slice.size * highWaterMark > Config.maxSliceRows) {
          val numSplits =
            math.ceil((slice.size * highWaterMark).toDouble / Config.maxSliceRows).toInt

          val size = math.ceil(Config.maxSliceRows.toDouble / highWaterMark).toInt

          // we repeatedly apply windowing to slice.  this avoids linear delegation through Remap
          val acc = (0 until numSplits).foldLeft(Vector.empty[Slice]) {
            case (acc, split) =>
              acc :+ slice.takeRange(size * split, size)
          }

          acc.filterNot(_.isEmpty)
        } else {
          Vector(slice)
        }

        // eagerly force the slices, since we'll be backtracking within each
        // shadow the outer `slice`
        StreamT.fromIterable(resplit).map(_.materialized).trans(λ[Id.Id ~> IO](IO.pure(_))) map { slice =>
          // ...and the outer `focused` and `unfocused`
          val (focused, unfocused) = lens(slice)

          // a CF1 for inflating column sizes to account for shifting
          val expansion = cf.util.Remap(_ / highWaterMark)

          val merged: Map[ColumnRef, Column] =
            merge(focused, innerIndex, expansion, highWaterMark)

          // figure out the definedness of the exploded, filtered result
          // this is necessary so we can implement inner-concat semantics
          val definedness: BitSet = {
            val bsing = merged.values map {
              case col: BitsetColumn => col.definedAt
              case col => col.definedAt(0, slice.size * highWaterMark)
            }

            bsing.reduceOption(_ | _).getOrElse(new BitSet)
          }

          val focusedTransformed: Map[ColumnRef, Column] =
            leftShiftFocused(merged, innerHeads, definedness)

          val unfocusedDefinedness = if (emitOnUndef) {
            val definedness2 = definedness.copy
            // this is where we necro the undefined rows
            definedness2.setByMod(highWaterMark)
            definedness2
          } else {
            definedness
          }

          val unfocusedTransformed: Map[ColumnRef, Column] =
            leftShiftUnfocused(unfocused, unfocusedDefinedness, slice.size, expansion, highWaterMark)

          // glue everything back together with the unfocused and compute the new size
          Slice(slice.size * highWaterMark, focusedTransformed ++ unfocusedTransformed)
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
        def stream(state: (Option[Slice], T), slices: StreamT[IO, Slice]): StreamT[IO, Slice] = StreamT(
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
                IO.pure(StreamT.Done)
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

    def drop(count: Long): Table = {
      val slices2: StreamT[IO, StreamT[IO, Slice]] =
        StreamT.unfoldM[IO, StreamT[IO, Slice], Option[(StreamT[IO, Slice], Long)]](Some((slices, 0L))) {
          case Some((slices, dropped)) =>
            slices.uncons map {
              case Some((slice, tail)) =>
                if (slice.size <= count - dropped)
                  Some((StreamT.empty[IO, Slice], Some((tail, dropped + slice.size))))
                else
                  Some((slice.drop((count - dropped).toInt) :: tail, None))

              case None => None
            }

          case None => IO.pure(None)
        }

      Table(slices2.flatMap(x => x), size + ExactSize(-count))
    }

    def take(count: Long): Table = {
      val slices2 = StreamT.unfoldM[IO, StreamT[IO, Slice], Option[(StreamT[IO, Slice], Long)]](Some((slices, 0L))) {
        case Some((slices, taken)) =>
          slices.uncons map {
            case Some((slice, tail)) =>
              if (slice.size <= count - taken)
                Some((slice :: StreamT.empty[IO, Slice], Some((tail, taken + slice.size))))
              else
                Some((slice.take((count - taken).toInt) :: StreamT.empty[IO, Slice], None))

            case None => None
          }

        case None => IO.pure(None)
      }

      Table(slices2.flatMap(x => x), EstimateSize(0, count))
    }
    /**
      * In order to call partitionMerge, the table must be sorted according to
      * the values specified by the partitionBy transspec.
      */
    def partitionMerge(partitionBy: TransSpec1, keepKey: Boolean = false)(f: Table => IO[Table]): IO[Table] = {
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

      def subTable(comparatorGen: Slice => (Int => Ordering), slices: StreamT[IO, Slice]): IO[Table] = {
        def subTable0(slices: StreamT[IO, Slice], subSlices: StreamT[IO, Slice], size: Int): IO[Table] = {
          slices.uncons flatMap {
            case Some((head, tail)) =>
              val headComparator = comparatorGen(head)
              val spanEnd        = findEnd(headComparator, 0, head.size - 1)
              if (spanEnd < head.size) {
                IO(Table(subSlices ++ (head.take(spanEnd) :: StreamT.empty[IO, Slice]), ExactSize(size + spanEnd)))
              } else {
                subTable0(tail, subSlices ++ (head :: StreamT.empty[IO, Slice]), size + head.size)
              }

            case None =>
              IO(Table(subSlices, ExactSize(size)))
          }
        }

        subTable0(slices, StreamT.empty[IO, Slice], 0)
      }

      def dropAndSplit(comparatorGen: Slice => (Int => Ordering), slices: StreamT[IO, Slice], spanStart: Int): StreamT[IO, Slice] = StreamT.wrapEffect {
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
            StreamT.empty[IO, Slice]
        }
      }

      def stepPartition(head: Slice, spanStart: Int, tail: StreamT[IO, Slice]): StreamT[IO, Slice] = {
        val comparatorGen = (s: Slice) => {
          val rowComparator = Slice.rowComparatorFor(head, s) { s0 =>
            s0.columns.keys collect {
              case ColumnRef(path @ CPath(CPathField("0"), _ @_ *), _) => path
            }
          }

          (i: Int) =>
            rowComparator.compare(spanStart, i)
        }

        val groupTable = subTable(comparatorGen, head.drop(spanStart) :: tail)

        val groupedM = (if (keepKey)
          groupTable
        else
          groupTable.map(_.transform(DerefObjectStatic(Leaf(Source), CPathField("1"))))).flatMap(f)

        val groupedStream: StreamT[IO, Slice] = StreamT.wrapEffect(groupedM.map(_.slices))

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

    def schemas: IO[Set[JType]] = {

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
          case COffsetDateTime        => JOffsetDateTimeT
          case COffsetTime            => JOffsetTimeT
          case COffsetDate            => JOffsetDateT
          case CLocalDateTime         => JLocalDateTimeT
          case CLocalTime             => JLocalTimeT
          case CLocalDate             => JLocalDateT
          case CInterval              => JIntervalT
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
      def collectSchemas(schemas: Set[JType], slices: StreamT[IO, Slice]): IO[Set[JType]] = {
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
            IO.pure(schemas)
        }
      }

      collectSchemas(Set.empty, slices)
    }

    def renderJson(prefix: String = "", delimiter: String = "\n", suffix: String = "", precise: Boolean = false): StreamT[IO, CharBuffer] =
      ColumnarTableModule.renderJson(slices, prefix, delimiter, suffix, precise)

    def renderCsv(assumeHomogeneous: Boolean): StreamT[IO, CharBuffer] = {
      def numToString: CF1 = CF1P {
        case c: LongColumn => new Map1Column(c) with StrColumn with NoCsvEscape {
          def apply(row: Int) = c(row).toString
        }
        case c: DoubleColumn => new Map1Column(c) with StrColumn with NoCsvEscape {
          def apply(row: Int) = c(row).toString
        }
        case c: NumColumn => new Map1Column(c) with StrColumn with NoCsvEscape {
          def apply(row: Int) = c(row).toString
        }
        case c => c
      }

      val transformedSlices: StreamT[IO, Slice] =
        if (assumeHomogeneous)
          this.transform(trans.DeepMap1(trans.TransSpec1.Id, numToString)).slices
        else
          slices

      ColumnarTableModule.renderCsv(transformedSlices, assumeHomogeneous)
    }

    def slicePrinter(prelude: String)(f: Slice => String): Table = {
      Table(
        StreamT(
          IO(
            StreamT.Skip({
              println(prelude);
              slices map { s =>
                println(f(s)); s
              }
            })
          )
        ),
      size)
    }

    def logged(logger: Logger, logPrefix: String = "", prelude: String = "", appendix: String = "")(f: Slice => String): Table = {
      val preludeEffect: StreamT[IO, Slice] =
        StreamT(IO(StreamT.Skip({ logger.debug(logPrefix + " " + prelude); StreamT.empty[IO, Slice] })))
      val appendixEffect: StreamT[IO, Slice] =
        StreamT(IO(StreamT.Skip({ logger.debug(logPrefix + " " + appendix); StreamT.empty[IO, Slice] })))
      val sliceEffect = if (logger.isTraceEnabled) slices map { s =>
        logger.trace(logPrefix + " " + f(s)); s
      } else slices
      Table(preludeEffect ++ sliceEffect ++ appendixEffect, size)
    }

    def printer(prelude: String = "", flag: String = ""): Table = slicePrinter(prelude)(s => s.toJsonString(flag))

    def toStrings: IO[Iterable[String]] = {
      toEvents { (slice, row) =>
        slice.toString(row)
      }
    }

    def toJson: IO[Iterable[RValue]] = {
      toEvents { (slice, row) =>
        val rvalue = slice.toRValue(row)
        if (rvalue != CUndefined) Some(rvalue)
        else None
      }
    }

    private def toEvents[A](f: (Slice, RowId) => Option[A]): IO[Iterable[A]] = {
      for (stream <- self.compact(Leaf(Source)).slices.toStream) yield {
        for (slice <- stream; i <- 0 until slice.size; a <- f(slice, i)) yield a
      }
    }

    def metrics = TableMetrics(readStarts.get, blockReads.get)
  }
}
