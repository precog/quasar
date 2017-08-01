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

import quasar.precog.common._
import quasar.yggdrasil._
import quasar.yggdrasil.bytecode._
import quasar.yggdrasil.table._

import scalaz._, Scalaz._

import java.time.ZonedDateTime

trait TableLibModule[M[+ _]] extends TableModule[M] with TransSpecModule {
  type Lib <: TableLib
  implicit def M: Monad[M]

  object TableLib {
    private val defaultMorphism1Opcode = new java.util.concurrent.atomic.AtomicInteger(0)
    private val defaultReductionOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  }

  trait MorphLogger {
    def info(msg: String): M[Unit]
    def warn(msg: String): M[Unit]
    def error(msg: String): M[Unit]
    def die(): M[Unit]
  }

  trait TableLib extends Library {
    import TableLib._
    import trans._

    lazy val libMorphism1 = _libMorphism1
    lazy val libMorphism2 = _libMorphism2
    lazy val lib1         = _lib1
    lazy val lib2         = _lib2
    lazy val libReduction = _libReduction

    def _libMorphism1: Set[Morphism1] = Set()
    def _libMorphism2: Set[Morphism2] = Set()
    def _lib1: Set[Op1]               = Set()
    def _lib2: Set[Op2]               = Set()
    def _libReduction: Set[Reduction] = Set()

    trait Morph1Apply {
      def apply(input: Table): M[Table]
    }

    sealed trait MorphismAlignment
    object MorphismAlignment {
      case class Match(morph: M[Morph1Apply])                                                    extends MorphismAlignment
      case class Cross(morph: M[Morph1Apply])                                                    extends MorphismAlignment
      case class Custom(alignment: IdentityPolicy, f: (Table, Table) => M[(Table, Morph1Apply)]) extends MorphismAlignment
    }

    abstract class Morphism1(val namespace: Vector[String], val name: String) extends Morphism1Like with Morph1Apply {
      val opcode: Int       = defaultMorphism1Opcode.getAndIncrement
      val rowLevel: Boolean = false
    }

    abstract class Morphism2(val namespace: Vector[String], val name: String) extends Morphism2Like {
      val opcode: Int           = defaultMorphism1Opcode.getAndIncrement
      val rowLevel: Boolean     = false
      val multivariate: Boolean = false

      /**
        * This specifies how to align the 2 arguments as they are inputted. For
        * instance, if we use MorphismAlignment.Cross, then the 2 tables will be
        * crossed, then passed to `morph1`.
        */
      def alignment: MorphismAlignment
    }

    abstract class Op1(namespace: Vector[String], name: String) extends Morphism1(namespace, name) with Op1Like {
      def spec[A <: SourceType](source: TransSpec[A]): TransSpec[A]

      def fold[A](op1: Op1 => A, op1F1: Op1F1 => A): A = op1(this)
      def apply(table: Table)       = sys.error("morphism application of an op1 is wrong")
    }

    abstract class Op1F1(namespace: Vector[String], name: String) extends Op1(namespace, name) {
      def spec[A <: SourceType](source: TransSpec[A]): TransSpec[A] =
        trans.Map1(source, f1)

      def f1: F1

      override val rowLevel: Boolean = true

      override def fold[A](op1: Op1 => A, op1F1: Op1F1 => A): A = op1F1(this)
    }

    abstract class Op2(namespace: Vector[String], name: String) extends Morphism2(namespace, name) with Op2Like {
      val alignment = MorphismAlignment.Match(M.point {
        new Morph1Apply {
          def apply(input: Table) = sys.error("morphism application of an op2 is wrong")
        }
      })

      def spec[A <: SourceType](left: TransSpec[A], right: TransSpec[A]): TransSpec[A]

      def fold[A](op2: Op2 => A, op2F2: Op2F2 => A): A = op2(this)
    }

    trait Op2Array extends Op2 {
      def spec[A <: SourceType](left: TransSpec[A], right: TransSpec[A]): TransSpec[A] = {
        trans.MapWith(trans.InnerArrayConcat(trans.WrapArray(trans.Map1(left, prepare)), trans.WrapArray(trans.Map1(right, prepare))), mapper)
      }

      def prepare: F1

      def mapper: Mapper
    }

    abstract class Op2F2(namespace: Vector[String], name: String) extends Op2(namespace, name) {
      def spec[A <: SourceType](left: TransSpec[A], right: TransSpec[A]): TransSpec[A] =
        trans.Map2(left, right, f2)

      def f2: F2

      override val rowLevel: Boolean = true

      override def fold[A](op2: Op2 => A, op2F2: Op2F2 => A): A = op2F2(this)
    }

    abstract class Reduction(val namespace: Vector[String], val name: String)(implicit M: Monad[M]) extends ReductionLike with Morph1Apply {
      val opcode: Int       = defaultReductionOpcode.getAndIncrement
      val rowLevel: Boolean = false

      type Result

      def monoid: Monoid[Result]
      def reducer: Reducer[Result]
      def extract(res: Result): Table
      def extractValue(res: Result): Option[RValue]

      def apply(table: Table): M[Table] = table.reduce(reducer)(monoid) map extract
    }

    def coalesce(reductions: List[(Reduction, Option[(JType => JType, ColumnRef => Option[ColumnRef])])]): Reduction
  }
}

trait ColumnarTableLibModule[M[+ _]] extends TableLibModule[M] with ColumnarTableModule[M] {
  trait ColumnarTableLib extends TableLib {
    class WrapArrayTableReduction(val r: Reduction, val jtypef: Option[(JType => JType, ColumnRef => Option[ColumnRef])]) extends Reduction(r.namespace, r.name) {
      type Result = r.Result
      val tpe = r.tpe

      def monoid = r.monoid
      def reducer = new CReducer[Result] {
        def reduce(schema: CSchema, range: Range): Result = {
          jtypef match {
            case Some((ft, fr)) =>
              val cols0 = new CSchema {
                def columnRefs = schema.columnRefs
                def columnMap(tpe: JType) =
                  schema.columnMap(ft(tpe)) flatMap {
                    case (ref, col) => fr(ref).map(_ -> col).toList
                  }
              }
              r.reducer.reduce(cols0, range)
            case None =>
              r.reducer.reduce(schema, range)
          }
        }
      }

      def extract(res: Result): Table =
        r.extract(res).transform(trans.WrapArray(trans.Leaf(trans.Source)))

      def extractValue(res: Result) = r.extractValue(res)
    }

    def coalesce(reductions: List[(Reduction, Option[(JType => JType, ColumnRef => Option[ColumnRef])])]): Reduction = {
      def rec(reductions: List[(Reduction, Option[(JType => JType, ColumnRef => Option[ColumnRef])])], acc: Reduction): Reduction = {
        reductions match {
          case (x, jtypef) :: xs => {
            val impl = new Reduction(Vector(), "") {
              type Result = (x.Result, acc.Result)

              def reducer = new CReducer[Result] {
                def reduce(schema: CSchema, range: Range): Result = {
                  jtypef match {
                    case Some((ft, fr)) =>
                      val cols0 = new CSchema {
                        def columnRefs = schema.columnRefs
                        def columnMap(tpe: JType) =
                          schema.columnMap(ft(tpe)) flatMap {
                            case (ref, col) => fr(ref).map(_ -> col).toList
                          }
                      }
                      (x.reducer.reduce(cols0, range), acc.reducer.reduce(schema, range))
                    case None =>
                      (x.reducer.reduce(schema, range), acc.reducer.reduce(schema, range))
                  }
                }
              }

              implicit val monoid: Monoid[Result] = new Monoid[Result] {
                def zero = (x.monoid.zero, acc.monoid.zero)
                def append(r1: Result, r2: => Result): Result = {
                  (x.monoid.append(r1._1, r2._1), acc.monoid.append(r1._2, r2._2))
                }
              }

              def extract(r: Result): Table = {
                import trans._

                val left  = x.extract(r._1)
                val right = acc.extract(r._2)

                left.cross(right)(OuterArrayConcat(WrapArray(Leaf(SourceLeft)), Leaf(SourceRight)))
              }

              // TODO: Can't translate this into a CValue. Evaluator
              // won't inline the results. See call to inlineNodeValue
              def extractValue(res: Result) = None

              val tpe = UnaryOperationType(JUnionT(x.tpe.arg, acc.tpe.arg), JArrayUnfixedT)
            }

            rec(xs, impl)
          }

          case Nil => acc
        }
      }

      val (impl1, jtype1) = reductions.head
      rec(reductions.tail, new WrapArrayTableReduction(impl1, jtype1))
    }
  }
}

trait StdLibModule[M[+ _]]
    extends InfixLibModule[M]
    with UnaryLibModule[M]
    with ArrayLibModule[M]
    with MathLibModule[M]
    with TypeLibModule[M]
    with TimeLibModule[M]
    with StringLibModule[M]
    with ReductionLibModule[M]
    with RandomLibModule[M] {
  type Lib <: StdLib

  trait StdLib
      extends InfixLib
      with UnaryLib
      with ArrayLib
      with MathLib
      with TypeLib
      with TimeLib
      with StringLib
      with ReductionLib
      with RandomLib
}

object StdLib {
  import java.lang.Double.{ isNaN, isInfinite }

  def doubleIsDefined(n: Double) = !(isNaN(n) || isInfinite(n))

  object StrFrom {
    class L(c: LongColumn, defined: Long => Boolean, f: Long => String) extends Map1Column(c) with StrColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class D(c: DoubleColumn, defined: Double => Boolean, f: Double => String) extends Map1Column(c) with StrColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class N(c: NumColumn, defined: BigDecimal => Boolean, f: BigDecimal => String) extends Map1Column(c) with StrColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class S(c: StrColumn, defined: String => Boolean, f: String => String) extends Map1Column(c) with StrColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class Dt(c: DateColumn, defined: ZonedDateTime => Boolean, f: ZonedDateTime => String) extends Map1Column(c) with StrColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class SS(c1: StrColumn, c2: StrColumn, defined: (String, String) => Boolean, f: (String, String) => String) extends Map2Column(c1, c2) with StrColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class SD(c1: StrColumn, c2: DoubleColumn, defined: (String, Double) => Boolean, f: (String, Double) => String) extends Map2Column(c1, c2) with StrColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) &&
          c1(row) != null && doubleIsDefined(c2(row)) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class SL(c1: StrColumn, c2: LongColumn, defined: (String, Long) => Boolean, f: (String, Long) => String) extends Map2Column(c1, c2) with StrColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && c1(row) != null && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class SN(c1: StrColumn, c2: NumColumn, defined: (String, BigDecimal) => Boolean, f: (String, BigDecimal) => String)
        extends Map2Column(c1, c2)
        with StrColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && c1(row) != null && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }
  }

  object LongFrom {

    class D(c: DoubleColumn, defined: Long => Boolean, f: Long => Long) extends Map1Column(c) with LongColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row).toLong) && c(row).ceil == c(row)

      def apply(row: Int) = f(c(row).toLong)
    }

    class L(c: LongColumn, defined: Long => Boolean, f: Long => Long) extends Map1Column(c) with LongColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class N(c: NumColumn, defined: Long => Boolean, f: Long => Long) extends Map1Column(c) with LongColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row).toLong)

      def apply(row: Int) = f(c(row).toLong)
    }

    class S(c: StrColumn, defined: String => Boolean, f: String => Long) extends Map1Column(c) with LongColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class Dt(c: DateColumn, defined: ZonedDateTime => Boolean, f: ZonedDateTime => Long) extends Map1Column(c) with LongColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class LL(c1: LongColumn, c2: LongColumn, defined: (Long, Long) => Boolean, f: (Long, Long) => Long) extends Map2Column(c1, c2) with LongColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class SS(c1: StrColumn, c2: StrColumn, defined: (String, String) => Boolean, f: (String, String) => Long) extends Map2Column(c1, c2) with LongColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class SD(c1: StrColumn, c2: DoubleColumn, defined: (String, Double) => Boolean, f: (String, Double) => Long) extends Map2Column(c1, c2) with LongColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class SL(c1: StrColumn, c2: LongColumn, defined: (String, Long) => Boolean, f: (String, Long) => Long) extends Map2Column(c1, c2) with LongColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class SN(c1: StrColumn, c2: NumColumn, defined: (String, BigDecimal) => Boolean, f: (String, BigDecimal) => Long)
        extends Map2Column(c1, c2)
        with LongColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }
  }

  object DoubleFrom {
    class D(c: DoubleColumn, defined: Double => Boolean, f: Double => Double) extends Map1Column(c) with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row)) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c(row))
    }

    class L(c: LongColumn, defined: Double => Boolean, f: Double => Double) extends Map1Column(c) with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row).toDouble) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c(row))
    }

    class N(c: NumColumn, defined: Double => Boolean, f: Double => Double) extends Map1Column(c) with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row).toDouble) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c(row).toDouble)
    }

    class DD(c1: DoubleColumn, c2: DoubleColumn, defined: (Double, Double) => Boolean, f: (Double, Double) => Double)
        extends Map2Column(c1, c2)
        with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row)) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class DL(c1: DoubleColumn, c2: LongColumn, defined: (Double, Double) => Boolean, f: (Double, Double) => Double)
        extends Map2Column(c1, c2)
        with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row).toDouble) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c1(row), c2(row).toDouble)
    }

    class DN(c1: DoubleColumn, c2: NumColumn, defined: (Double, Double) => Boolean, f: (Double, Double) => Double)
        extends Map2Column(c1, c2)
        with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row).toDouble) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c1(row), c2(row).toDouble)
    }

    class LD(c1: LongColumn, c2: DoubleColumn, defined: (Double, Double) => Boolean, f: (Double, Double) => Double)
        extends Map2Column(c1, c2)
        with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row).toDouble, c2(row)) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c1(row).toDouble, c2(row))
    }

    class LL(c1: LongColumn, c2: LongColumn, defined: (Double, Double) => Boolean, f: (Double, Double) => Double)
        extends Map2Column(c1, c2)
        with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) &&
          defined(c1(row).toDouble, c2(row).toDouble) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c1(row).toDouble, c2(row).toDouble)
    }

    class LN(c1: LongColumn, c2: NumColumn, defined: (Double, Double) => Boolean, f: (Double, Double) => Double) extends Map2Column(c1, c2) with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) &&
          defined(c1(row).toDouble, c2(row).toDouble) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c1(row).toDouble, c2(row).toDouble)
    }

    class ND(c1: NumColumn, c2: DoubleColumn, defined: (Double, Double) => Boolean, f: (Double, Double) => Double)
        extends Map2Column(c1, c2)
        with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row).toDouble, c2(row)) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c1(row).toDouble, c2(row))
    }

    class NL(c1: NumColumn, c2: LongColumn, defined: (Double, Double) => Boolean, f: (Double, Double) => Double) extends Map2Column(c1, c2) with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) &&
          defined(c1(row).toDouble, c2(row).toDouble) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c1(row).toDouble, c2(row).toDouble)
    }

    class NN(c1: NumColumn, c2: NumColumn, defined: (Double, Double) => Boolean, f: (Double, Double) => Double) extends Map2Column(c1, c2) with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) &&
          defined(c1(row).toDouble, c2(row).toDouble) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c1(row).toDouble, c2(row).toDouble)
    }
  }

  object NumFrom {

    // unsafe!  use only if you know what you're doing
    class D(c: DoubleColumn, defined: BigDecimal => Boolean, f: BigDecimal => BigDecimal) extends Map1Column(c) with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(BigDecimal(c(row)))

      def apply(row: Int) = f(BigDecimal(c(row)))
    }

    // unsafe!  use only if you know what you're doing
    class L(c: LongColumn, defined: BigDecimal => Boolean, f: BigDecimal => BigDecimal) extends Map1Column(c) with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(BigDecimal(c(row)))

      def apply(row: Int) = f(BigDecimal(c(row)))
    }

    class N(c: NumColumn, defined: BigDecimal => Boolean, f: BigDecimal => BigDecimal) extends Map1Column(c) with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class S(c: StrColumn, defined: String => Boolean, f: String => BigDecimal) extends Map1Column(c) with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class DD(c1: DoubleColumn, c2: DoubleColumn, defined: (BigDecimal, BigDecimal) => Boolean, f: (BigDecimal, BigDecimal) => BigDecimal)
        extends Map2Column(c1, c2)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(BigDecimal(c1(row)), BigDecimal(c2(row)))

      def apply(row: Int) = f(BigDecimal(c1(row)), BigDecimal(c2(row)))
    }

    class DL(c1: DoubleColumn, c2: LongColumn, defined: (BigDecimal, BigDecimal) => Boolean, f: (BigDecimal, BigDecimal) => BigDecimal)
        extends Map2Column(c1, c2)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(BigDecimal(c1(row)), BigDecimal(c2(row)))

      def apply(row: Int) = f(BigDecimal(c1(row)), BigDecimal(c2(row)))
    }

    class DN(c1: DoubleColumn, c2: NumColumn, defined: (BigDecimal, BigDecimal) => Boolean, f: (BigDecimal, BigDecimal) => BigDecimal)
        extends Map2Column(c1, c2)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(BigDecimal(c1(row)), c2(row))

      def apply(row: Int) = f(BigDecimal(c1(row)), c2(row))
    }

    class LD(c1: LongColumn, c2: DoubleColumn, defined: (BigDecimal, BigDecimal) => Boolean, f: (BigDecimal, BigDecimal) => BigDecimal)
        extends Map2Column(c1, c2)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(BigDecimal(c1(row)), BigDecimal(c2(row)))

      def apply(row: Int) = f(BigDecimal(c1(row)), BigDecimal(c2(row)))
    }

    class LL(c1: LongColumn, c2: LongColumn, defined: (BigDecimal, BigDecimal) => Boolean, f: (BigDecimal, BigDecimal) => BigDecimal)
        extends Map2Column(c1, c2)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(BigDecimal(c1(row)), BigDecimal(c2(row)))

      def apply(row: Int) = f(BigDecimal(c1(row)), BigDecimal(c2(row).toDouble))
    }

    class LN(c1: LongColumn, c2: NumColumn, defined: (BigDecimal, BigDecimal) => Boolean, f: (BigDecimal, BigDecimal) => BigDecimal)
        extends Map2Column(c1, c2)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(BigDecimal(c1(row)), c2(row))

      def apply(row: Int) = f(BigDecimal(c1(row)), c2(row))
    }

    class ND(c1: NumColumn, c2: DoubleColumn, defined: (BigDecimal, BigDecimal) => Boolean, f: (BigDecimal, BigDecimal) => BigDecimal)
        extends Map2Column(c1, c2)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), BigDecimal(c2(row)))

      def apply(row: Int) = f(c1(row), BigDecimal(c2(row)))
    }

    class NL(c1: NumColumn, c2: LongColumn, defined: (BigDecimal, BigDecimal) => Boolean, f: (BigDecimal, BigDecimal) => BigDecimal)
        extends Map2Column(c1, c2)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), BigDecimal(c2(row)))

      def apply(row: Int) = f(c1(row), BigDecimal(c2(row)))
    }

    class NN(c1: NumColumn, c2: NumColumn, defined: (BigDecimal, BigDecimal) => Boolean, f: (BigDecimal, BigDecimal) => BigDecimal)
        extends Map2Column(c1, c2)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }
  }

  object BoolFrom {
    class B(c: BoolColumn, f: Boolean => Boolean) extends Map1Column(c) with BoolColumn {

      def apply(row: Int) = f(c(row))
    }

    class BB(c1: BoolColumn, c2: BoolColumn, f: (Boolean, Boolean) => Boolean) extends Map2Column(c1, c2) with BoolColumn {

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class S(c: StrColumn, defined: String => Boolean, f: String => Boolean) extends Map1Column(c) with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class SS(c1: StrColumn, c2: StrColumn, defined: (String, String) => Boolean, f: (String, String) => Boolean) extends Map2Column(c1, c2) with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class DD(c1: DoubleColumn, c2: DoubleColumn, defined: (Double, Double) => Boolean, f: (Double, Double) => Boolean)
        extends Map2Column(c1, c2)
        with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class DL(c1: DoubleColumn, c2: LongColumn, defined: (Double, Long) => Boolean, f: (Double, Long) => Boolean) extends Map2Column(c1, c2) with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class DN(c1: DoubleColumn, c2: NumColumn, defined: (Double, BigDecimal) => Boolean, f: (Double, BigDecimal) => Boolean)
        extends Map2Column(c1, c2)
        with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class LD(c1: LongColumn, c2: DoubleColumn, defined: (Long, Double) => Boolean, f: (Long, Double) => Boolean) extends Map2Column(c1, c2) with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class LL(c1: LongColumn, c2: LongColumn, defined: (Long, Long) => Boolean, f: (Long, Long) => Boolean) extends Map2Column(c1, c2) with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class LN(c1: LongColumn, c2: NumColumn, defined: (Long, BigDecimal) => Boolean, f: (Long, BigDecimal) => Boolean)
        extends Map2Column(c1, c2)
        with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class ND(c1: NumColumn, c2: DoubleColumn, defined: (BigDecimal, Double) => Boolean, f: (BigDecimal, Double) => Boolean)
        extends Map2Column(c1, c2)
        with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class NL(c1: NumColumn, c2: LongColumn, defined: (BigDecimal, Long) => Boolean, f: (BigDecimal, Long) => Boolean)
        extends Map2Column(c1, c2)
        with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class NN(c1: NumColumn, c2: NumColumn, defined: (BigDecimal, BigDecimal) => Boolean, f: (BigDecimal, BigDecimal) => Boolean)
        extends Map2Column(c1, c2)
        with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class Dt(c: DateColumn, defined: ZonedDateTime => Boolean, f: ZonedDateTime => Boolean) extends Map1Column(c) with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class DtDt(c1: DateColumn, c2: DateColumn, defined: (ZonedDateTime, ZonedDateTime) => Boolean, f: (ZonedDateTime, ZonedDateTime) => Boolean)
        extends Map2Column(c1, c2)
        with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }
  }

  val StrAndDateT = JUnionT(JTextT, JDateT)

  def dateToStrCol(c: DateColumn): StrColumn = new StrColumn {
    def isDefinedAt(row: Int): Boolean = c.isDefinedAt(row)
    def apply(row: Int): String        = c(row).toString
  }
}
