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
import quasar.yggdrasil.util.RangeUtil

import scala.collection.mutable

import scalaz._
import Scalaz._

class LongAdder {
  var t = 0L
  val ts = mutable.ArrayBuffer.empty[BigDecimal]

  final def maxLongSqrt = 3037000499L

  def add(x: BigDecimal): Unit = ts.append(x)

  def addSquare(x: Long) =
    if (x < maxLongSqrt)
      add(x * x)
    else
      add(BigDecimal(x) pow 2)

  def add(x: Long): Unit = {
    val y = t + x
    if ((~(x ^ t) & (x ^ y)) >= 0L) {
      t = y
    } else {
      ts.append(BigDecimal(t))
      t = x
    }
  }
  def total(): BigDecimal = ts.foldLeft(BigDecimal(0))(_ + _) + t
}

trait ReductionLibModule extends ColumnarTableLibModule {
  trait ReductionLib extends ColumnarTableLib {
    import BigDecimalOperations._

    val CountMonoid = implicitly[Monoid[Count.Result]]
    object Count extends Reduction {
      // limiting ourselves to 9.2e18 rows doesn't seem like a problem.
      type Result = Long

      implicit val monoid = CountMonoid

      val tpe = UnaryOperationType(JType.JUniverseT, JNumberT)

      def reducer: Reducer[Result] = new CReducer[Result] {
        def reduce(schema: CSchema, range: Range) = {
          val cx = schema.columns(JType.JUniverseT).toArray
          var count = 0L
          RangeUtil.loop(range) { i =>
            if (Column.isDefinedAt(cx, i)) count += 1L
          }
          count
        }
      }

      def extract(res: Result): Table = Table.constLong(Set(res))

      def extractValue(res: Result) = Some(CNum(res))
    }

    object Max extends Reduction {
      type Result = Option[BigDecimal]

      implicit val monoid = new Monoid[Result] {
        def zero = None
        def append(left: Result, right: => Result): Result = {
          (for (l <- left; r <- right) yield l max r) orElse left orElse right
        }
      }

      val tpe = UnaryOperationType(JNumberT, JNumberT)

      def reducer: Reducer[Result] = new CReducer[Result] {
        def reduce(schema: CSchema, range: Range): Result = {
          val maxs = schema.columns(JNumberT) map {
            case col: LongColumn =>
              // for longs, we'll use a Boolean to track whether zmax was really
              // seen or not.
              var zmax = Long.MinValue
              val seen = RangeUtil.loopDefined(range, col) { i =>
                val z = col(i)
                if (z > zmax) zmax = z
              }
              if (seen) Some(BigDecimal(zmax)) else None

            case col: DoubleColumn =>
              // since -inf is not a legal value, it's a great starting point for
              // finding the max because any legal value will be greater.
              var zmax = Double.NegativeInfinity
              RangeUtil.loopDefined(range, col) { i =>
                val z = col(i)
                if (z > zmax) zmax = z
              }
              if (zmax > Double.NegativeInfinity) Some(BigDecimal(zmax)) else None

            case col: NumColumn =>
              // we can just use a null BigDecimal to signal that we haven't
              // found a value yet.
              var zmax: BigDecimal = null
              RangeUtil.loopDefined(range, col) { i =>
                val z = col(i)
                if (zmax == null || z > zmax) zmax = z
              }
              if (zmax != null) Some(zmax) else None

            case _ => None
          }

          // now we just find the max out of all of our column types
          if (maxs.isEmpty) None else maxs.suml(monoid)
        }
      }

      def extract(res: Result): Table =
        res map { v =>
          Table.constDecimal(Set(v))
        } getOrElse Table.empty

      def extractValue(res: Result) = res map { CNum(_) }
    }

    val MinMonoid = implicitly[Monoid[Min.Result]]

    object Min extends Reduction {
      type Result = Option[BigDecimal]

      implicit val monoid = new Monoid[Result] {
        def zero = None
        def append(left: Result, right: => Result): Result = {
          (for (l <- left; r <- right) yield l min r) orElse left orElse right
        }
      }

      val tpe = UnaryOperationType(JNumberT, JNumberT)

      def reducer: Reducer[Result] = new CReducer[Result] {
        def reduce(schema: CSchema, range: Range): Result = {
          val mins = schema.columns(JNumberT) map {
            case col: LongColumn =>
              // for longs, we'll use a Boolean to track whether zmin was really
              // seen or not.
              var zmin = Long.MaxValue
              val seen = RangeUtil.loopDefined(range, col) { i =>
                val z = col(i)
                if (z < zmin) zmin = z
              }
              if (seen) Some(BigDecimal(zmin)) else None

            case col: DoubleColumn =>
              // since +inf is not a legal value, it's a great starting point for
              // finding the min because any legal value will be less.
              var zmin = Double.PositiveInfinity
              RangeUtil.loopDefined(range, col) { i =>
                val z = col(i)
                if (z < zmin) zmin = z
              }
              if (zmin < Double.PositiveInfinity) Some(BigDecimal(zmin)) else None

            case col: NumColumn =>
              // we can just use a null BigDecimal to signal that we haven't
              // found a value yet.
              var zmin: BigDecimal = null
              RangeUtil.loopDefined(range, col) { i =>
                val z = col(i)
                if (zmin == null || z < zmin) zmin = z
              }
              if (zmin != null) Some(zmin) else None

            case _ => None
          }

          // now we just find the min out of all of our column types
          if (mins.isEmpty) None else mins.suml(monoid)
        }
      }

      def extract(res: Result): Table =
        res map { v =>
          Table.constDecimal(Set(v))
        } getOrElse Table.empty

      def extractValue(res: Result) = res map { CNum(_) }
    }

    val SumMonoid = implicitly[Monoid[Sum.Result]]
    object Sum extends Reduction {
      type Result = Option[BigDecimal]

      implicit val monoid = SumMonoid

      val tpe = UnaryOperationType(JNumberT, JNumberT)

      def reducer: Reducer[Result] = new CReducer[Result] {
        def reduce(schema: CSchema, range: Range) = {

          val sum = schema.columns(JNumberT) map {

            case col: LongColumn =>
              val ls = new LongAdder()
              val seen = RangeUtil.loopDefined(range, col) { i =>
                ls.add(col(i))
              }
              if (seen) Some(ls.total) else None

            // TODO: exactness + overflow
            case col: DoubleColumn =>
              var t = 0.0
              val seen = RangeUtil.loopDefined(range, col) { i =>
                t += col(i)
              }
              if (seen) Some(BigDecimal(t)) else None

            case col: NumColumn =>
              var t = BigDecimal(0)
              val seen = RangeUtil.loopDefined(range, col) { i =>
                t += col(i)
              }
              if (seen) Some(t) else None

            case _ => None
          }

          if (sum.isEmpty) None else sum.suml(monoid)
        }
      }

      def extract(res: Result): Table =
        res map { v =>
          Table.constDecimal(Set(v))
        } getOrElse Table.empty

      def extractValue(res: Result) = res map { CNum(_) }
    }

    val MeanMonoid = implicitly[Monoid[Mean.Result]]
    object Mean extends Reduction {
      type Result        = Option[InitialResult]
      type InitialResult = (BigDecimal, Long) // (sum, count)

      implicit val monoid = MeanMonoid

      val tpe = UnaryOperationType(JNumberT, JNumberT)

      def reducer: Reducer[Result] = new Reducer[Result] {
        def reduce(schema: CSchema, range: Range): Result = {
          val results = schema.columns(JNumberT) map {

            case col: LongColumn =>
              val ls = new LongAdder()
              var count = 0L
              RangeUtil.loopDefined(range, col) { i =>
                ls.add(col(i))
                count += 1L
              }
              if (count > 0L) Some((ls.total, count)) else None

            case col: DoubleColumn =>
              var count = 0L
              var t     = BigDecimal(0)
              RangeUtil.loopDefined(range, col) { i =>
                t += col(i)
                count += 1L
              }
              if (count > 0L) Some((t, count)) else None

            case col: NumColumn =>
              var count = 0L
              var t     = BigDecimal(0)
              RangeUtil.loopDefined(range, col) { i =>
                t += col(i)
                count += 1L
              }
              if (count > 0L) Some((t, count)) else None

            case _ => None
          }

          if (results.isEmpty) None else results.suml(monoid)
        }
      }

      def perform(res: Result): Option[BigDecimal] = res map {
        case (sum, count) => sum / count
      }

      def extract(res: Result): Table =
        perform(res) map {
          case v => Table.constDecimal(Set(v))
        } getOrElse Table.empty

      def extractValue(res: Result) = perform(res) map { CNum(_) }
    }

    object GeometricMean extends Reduction {
      type Result        = Option[InitialResult]
      type InitialResult = (BigDecimal, Long)

      implicit val monoid = new Monoid[Result] {
        def zero = None
        def append(left: Result, right: => Result) = {
          val both = for ((l1, l2) <- left; (r1, r2) <- right) yield (l1 * r1, l2 + r2)
          both orElse left orElse right
        }
      }

      val tpe = UnaryOperationType(JNumberT, JNumberT)

      def reducer: Reducer[Result] = new Reducer[Option[(BigDecimal, Long)]] {
        def reduce(schema: CSchema, range: Range): Result = {
          val results = schema.columns(JNumberT) map {
            case col: LongColumn =>
              var prod  = BigDecimal(1)
              var count = 0L
              RangeUtil.loopDefined(range, col) { i =>
                prod *= col(i)
                count += 1L
              }
              if (count > 0) Some((prod, count)) else None

            case col: DoubleColumn =>
              var prod  = BigDecimal(1)
              var count = 0L
              RangeUtil.loopDefined(range, col) { i =>
                prod *= col(i)
                count += 1L
              }
              if (count > 0) Some((prod, count)) else None

            case col: NumColumn =>
              var prod  = BigDecimal(1)
              var count = 0L
              RangeUtil.loopDefined(range, col) { i =>
                prod *= col(i)
                count += 1L
              }
              if (count > 0) Some((prod, count)) else None

            case _ => None
          }

          if (results.isEmpty) None else results.suml(monoid)
        }
      }

      private def perform(res: Result) =
        res map {
          case (prod, count) => math.pow(prod.toDouble, 1 / count.toDouble)
        } filter (StdLib.doubleIsDefined)

      def extract(res: Result): Table =
        perform(res) map { v =>
          Table.constDouble(Set(v))
        } getOrElse {
          Table.empty
        }

      def extractValue(res: Result) = perform(res).map(CNum(_))
    }

    val SumSqMonoid = implicitly[Monoid[SumSq.Result]]
    object SumSq extends Reduction {
      type Result = Option[BigDecimal]

      implicit val monoid = SumSqMonoid

      val tpe = UnaryOperationType(JNumberT, JNumberT)

      def reducer: Reducer[Result] = new Reducer[Result] {
        def reduce(schema: CSchema, range: Range): Result = {
          val result = schema.columns(JNumberT) map {

            case col: LongColumn =>
              val ls = new LongAdder()
              val seen = RangeUtil.loopDefined(range, col) { i =>
                ls.addSquare(col(i))
              }
              if (seen) Some(ls.total) else None

            case col: DoubleColumn =>
              var t = BigDecimal(0)
              val seen = RangeUtil.loopDefined(range, col) { i =>
                t += BigDecimal(col(i)) pow 2
              }
              if (seen) Some(t) else None

            case col: NumColumn =>
              var t = BigDecimal(0)
              val seen = RangeUtil.loopDefined(range, col) { i =>
                t += col(i) pow 2
              }
              if (seen) Some(t) else None

            case _ => None
          }

          if (result.isEmpty) None else result.suml(monoid)
        }
      }

      def extract(res: Result): Table =
        res map { v =>
          Table.constDecimal(Set(v))
        } getOrElse Table.empty

      def extractValue(res: Result) = res map { CNum(_) }
    }

    class CountSumSumSqReducer extends Reducer[Option[(Long, BigDecimal, BigDecimal)]] {
      def reduce(schema: CSchema, range: Range): Option[(Long, BigDecimal, BigDecimal)] = {
        val result = schema.columns(JNumberT) map {
          case col: LongColumn =>
            var count = 0L
            val sum   = new LongAdder()
            val sumsq = new LongAdder()
            val seen = RangeUtil.loopDefined(range, col) { i =>
              val z = col(i)
              count += 1
              sum.add(z)
              sumsq.addSquare(z)
            }

            if (seen) Some((count, sum.total, sumsq.total)) else None

          case col: DoubleColumn =>
            var count = 0L
            var sum   = BigDecimal(0)
            var sumsq = BigDecimal(0)
            val seen = RangeUtil.loopDefined(range, col) { i =>
              val z = BigDecimal(col(i))
              count += 1
              sum += z
              sumsq += z pow 2
            }

            if (seen) Some((count, sum, sumsq)) else None

          case col: NumColumn =>
            var count = 0L
            var sum   = BigDecimal(0)
            var sumsq = BigDecimal(0)
            val seen = RangeUtil.loopDefined(range, col) { i =>
              val z = col(i)
              count += 1
              sum += z
              sumsq += z pow 2
            }

            if (seen) Some((count, sum, sumsq)) else None

          case _ => None
        }

        if (result.isEmpty) None else result.suml
      }
    }

    val VarianceMonoid = implicitly[Monoid[Variance.Result]]
    object Variance extends Reduction {
      type Result = Option[InitialResult]

      type InitialResult = (Long, BigDecimal, BigDecimal)

      implicit val monoid = VarianceMonoid

      val tpe = UnaryOperationType(JNumberT, JNumberT)

      def reducer: Reducer[Result] = new CountSumSumSqReducer()

      def perform(res: Result) = res flatMap {
        case (count, sum, sumsq) if count > 0 =>
          val n = (sumsq - (sum * sum / count)) / count
          Some(n)
        case _ =>
          None
      }

      def extract(res: Result): Table =
        perform(res) map { v =>
          Table.constDecimal(Set(v))
        } getOrElse Table.empty

      def extractValue(res: Result) = perform(res) map { CNum(_) }
    }

    val StdDevMonoid = implicitly[Monoid[StdDev.Result]]
    object StdDev extends Reduction {
      type Result        = Option[InitialResult]
      type InitialResult = (Long, BigDecimal, BigDecimal) // (count, sum, sumsq)

      implicit val monoid = StdDevMonoid

      val tpe = UnaryOperationType(JNumberT, JNumberT)

      def reducer: Reducer[Result] = new CountSumSumSqReducer()

      def perform(res: Result) = res flatMap {
        case (count, sum, sumsq) if count > 0 =>
          val n = sqrt(count * sumsq - sum * sum) / count
          Some(n)
        case _ =>
          None
      }

      def extract(res: Result): Table =
        perform(res) map { v =>
          Table.constDecimal(Set(v))
        } getOrElse Table.empty

      // todo using toDouble is BAD
      def extractValue(res: Result) = perform(res) map { CNum(_) }
    }

    object Forall extends Reduction {
      type Result = Option[Boolean]

      val tpe = UnaryOperationType(JBooleanT, JBooleanT)

      implicit val monoid = new Monoid[Option[Boolean]] {
        def zero = None

        def append(left: Option[Boolean], right: => Option[Boolean]) = {
          val both = for (l <- left; r <- right) yield l && r
          both orElse left orElse right
        }
      }

      def reducer: Reducer[Result] = new CReducer[Result] {
        def reduce(schema: CSchema, range: Range) = {
          if (range.isEmpty) {
            None
          } else {
            var back    = true
            var defined = false

            schema.columns(JBooleanT) foreach { c =>
              val bc = c.asInstanceOf[BoolColumn]
              var acc = back

              val idef = RangeUtil.loopDefined(range, bc) { i =>
                acc &&= bc(i)
              }

              back &&= acc

              if (idef) {
                defined = true
              }
            }

            if (defined)
              Some(back)
            else
              None
          }
        }
      }

      private val default = true
      private def perform(res: Result) = res getOrElse default

      def extract(res: Result): Table = Table.constBoolean(Set(perform(res)))

      def extractValue(res: Result) = Some(CBoolean(perform(res)))
    }

    object Exists extends Reduction {
      type Result = Option[Boolean]

      val tpe = UnaryOperationType(JBooleanT, JBooleanT)

      implicit val monoid = new Monoid[Option[Boolean]] {
        def zero = None

        def append(left: Option[Boolean], right: => Option[Boolean]) = {
          val both = for (l <- left; r <- right) yield l || r
          both orElse left orElse right
        }
      }

      def reducer: Reducer[Result] = new CReducer[Result] {
        def reduce(schema: CSchema, range: Range) = {
          if (range.isEmpty) {
            None
          } else {
            var back    = false
            var defined = false

            schema.columns(JBooleanT) foreach { c =>
              val bc = c.asInstanceOf[BoolColumn]
              var acc = back

              val idef = RangeUtil.loopDefined(range, bc) { i =>
                acc ||= bc(i)
              }

              back ||= acc

              if (idef) {
                defined = true
              }
            }

            if (defined)
              Some(back)
            else
              None
          }
        }
      }

      private val default = false
      private def perform(res: Result) = res getOrElse default

      def extract(res: Result): Table = Table.constBoolean(Set(perform(res)))

      def extractValue(res: Result) = Some(CBoolean(perform(res)))
    }

    object First extends Reduction {
      import scala.util.control.Breaks._

      type Result = Option[RValue]

      implicit val monoid = new Monoid[Option[RValue]] {
        def zero = None

        def append(left: Option[RValue], right: => Option[RValue]) =
          left orElse right
      }

      val tpe = UnaryOperationType(JType.JUniverseT, JType.JUniverseT)

      def reducer: Reducer[Result] = new CReducer[Result] {
        def reduce(schema: CSchema, range: Range) = {
          val slice = Slice(range.end, schema.columnMap(JType.JUniverseT))
          var result: Option[RValue] = None
          breakable {
            RangeUtil.loop(range) { i =>
              if (slice.isDefinedAt(i)) {
                result = Some(slice.toRValue(i))
                break()
              }
            }
          }
          result
        }
      }

      def extract(res: Result): Table =
        Table.fromRValues(res.toStream, None)

      def extractValue(res: Result) = res
    }

    object Last extends Reduction {
      type Result = Option[() => RValue]

      implicit val monoid = new Monoid[Option[() => RValue]] {
        def zero = None

        def append(left: Option[() => RValue], right: => Option[() => RValue]) =
          right orElse left
      }

      val tpe = UnaryOperationType(JType.JUniverseT, JType.JUniverseT)

      def reducer: Reducer[Result] = new CReducer[Result] {
        def reduce(schema: CSchema, range: Range) = {
          val slice = Slice(range.end, schema.columnMap(JType.JUniverseT))
          var result = -1
          RangeUtil.loop(range) { i =>
            if (slice.isDefinedAt(i)) {
              result = i
            }
          }

          // this allows unboxed lambda lifting
          val finalResult = result

          if (result >= 0)
            Some(() => slice.toRValue(finalResult))
          else
            None
        }
      }

      def extract(res: Result): Table =
        Table.fromRValues(res.toStream.map(_()), None)

      def extractValue(res: Result) = res.map(_())
    }

    object UnshiftArray extends Reduction {
      type Result = List[RValue]

      implicit val monoid = Scalaz.listMonoid[RValue]

      val tpe = UnaryOperationType(JType.JUniverseT, JArrayUnfixedT)

      def reducer: Reducer[Result] = new CReducer[Result] {
        def reduce(schema: CSchema, range: Range) = {
          val slice = Slice(range.end, schema.columnMap(JType.JUniverseT))          

          val buffer = mutable.ListBuffer[RValue]()
          RangeUtil.loop(range) { i =>
            if (slice.isDefinedAt(i)) {
              buffer += slice.toRValue(i)
            }
          }

          buffer.toList
        }
      }

      def extract(res: Result): Table =
        Table.fromRValues(extractValue(res).toStream, Some(1))

      def extractValue(res: Result) = Some(RArray(res))
    }
  }
}
