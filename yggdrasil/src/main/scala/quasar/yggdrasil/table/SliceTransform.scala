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
import quasar.contrib.std.errorImpossible
import quasar.precog.BitSet
import quasar.precog.common._
import quasar.precog.util._
import quasar.yggdrasil.bytecode.{ JBooleanT, JObjectUnfixedT, JArrayUnfixedT }
import quasar.yggdrasil.table.ctrie._

import java.util.Arrays
import scala.collection.mutable
import scala.annotation.tailrec

import cats.effect.IO

import scalaz._
import scalaz.std.tuple._
import scalaz.syntax.monad._
import scalaz.syntax.bifunctor._

import shims._

trait SliceTransforms extends TableModule with ColumnarTableTypes with ObjectConcatHelpers with ArrayConcatHelpers with MapUtils {

  import trans._

  protected object SliceTransform {
    def identity[A](initial: A) = SliceTransform1.liftM[A](initial, (a: A, s: Slice) => (a, s))
    def left[A](initial: A)     = SliceTransform2.liftM[A](initial, (a: A, sl: Slice, sr: Slice) => (a, sl))
    def right[A](initial: A)    = SliceTransform2.liftM[A](initial, (a: A, sl: Slice, sr: Slice) => (a, sr))

    def liftM(f: Slice => Slice): SliceTransform1[Unit] =
      SliceTransform1.liftM[Unit]((), { (u, s) =>
        (u, f(s))
      })

    def lift(f: Slice => IO[Slice]): SliceTransform1[Unit] =
      SliceTransform1[Unit]((), { (_, s) =>
        f(s) map { ((), _) }
      })

    def composeSliceTransform(spec: TransSpec1): SliceTransform1[_] = {
      composeSliceTransform2(spec).parallel
    }

    // No transform defined herein may reduce the size of a slice. Be it known!
    def composeSliceTransform2(spec: TransSpec[SourceType]): SliceTransform2[_] = {

      def equalsImpl(sl: Slice, sr: Slice): Slice =

        /*
         * 1. split each side into numeric and non-numeric columns
         * 2. cogroup non-numerics by ColumnRef
         * 3. for each pair of matched columns, compare by cf.std.Eq
         * 4. when matched column is undefined, return true iff other side is undefined
         * 5. reduce non-numeric matches by And
         * 6. analogously for numeric columns, save for the following
         * 7. for numeric columns with multiple possible types, consider all
         *    matches and reduce the results by And
         * 8. reduce numeric and non-numeric results by And
         * 9. mask definedness on output column by definedness on input columns
         *
         * Generally speaking, for each pair of columns cogrouped by path, we
         * compute a single equality column which is defined everywhere and
         * definedness behaves according to the following truth table:
         *
         * - v1 = v2 == <equality>
         * - v1 = undefined = false
         * - undefined = v2 = false
         * - undefined = undefined = true
         *
         * The undefined comparison is required because we are comparing columns
         * that may be nested inside a larger object.  We don't have the information
         * at the pair level to determine whether the answer should be
         * undefined for the entire row, or if the current column pair simply
         * does not contribute to the final truth value.  Thus, we return a
         * "non-contributing" value (the zero for boolean And) and mask definedness
         * as a function of all columns.  A computed result will be overridden
         * by undefined (masked off) if *either* the LHS or the RHS is fully
         * undefined (for all columns) at a particular row.
         */

        Slice(
          sl.size,
          {
            val (leftNonNum, leftNum) = sl.columns partition {
              case (ColumnRef(_, CLong | CDouble | CNum), _) => false
              case _                                         => true
            }

            val (rightNonNum, rightNum) = sr.columns partition {
              case (ColumnRef(_, CLong | CDouble | CNum), _) => false
              case _                                         => true
            }

            val groupedNonNum = (leftNonNum mapValues { _ :: Nil }) cogroup (rightNonNum mapValues { _ :: Nil })

            val simplifiedGroupNonNum = groupedNonNum map {
              case (_, Left3(column))                        => Left(column)
              case (_, Right3(column))                       => Left(column)
              case (_, Middle3((left :: Nil, right :: Nil))) => Right((left, right))
              case (_, x)                                    => abort("Unexpected: " + x)
            }

            class FuzzyEqColumn(left: Column, right: Column) extends BoolColumn {
              val equality = cf.std.Eq(left, right).get.asInstanceOf[BoolColumn] // yay!
              def isDefinedAt(row: Int) = (left isDefinedAt row) || (right isDefinedAt row)
              def apply(row: Int)       = equality.isDefinedAt(row) && equality(row)
            }

            val testedNonNum: Array[BoolColumn] = simplifiedGroupNonNum.map({
              case Left(column) =>
                new BoolColumn {
                  def isDefinedAt(row: Int) = column.isDefinedAt(row)
                  def apply(row: Int)       = false
                }

              case Right((left, right)) =>
                new FuzzyEqColumn(left, right)

            })(collection.breakOut)

            // numeric stuff

            def stripTypes(cols: Map[ColumnRef, Column]) = {
              cols.foldLeft(Map[CPath, Set[Column]]()) {
                case (acc, (ColumnRef(path, _), column)) => {
                  val set = acc get path map { _ + column } getOrElse Set(column)
                  acc.updated(path, set)
                }
              }
            }

            val leftNumMulti  = stripTypes(leftNum)
            val rightNumMulti = stripTypes(rightNum)

            val groupedNum = leftNumMulti cogroup rightNumMulti

            val simplifiedGroupedNum = groupedNum map {
              case (_, Left3(column))  => Left(column): Either[Column, (Set[Column], Set[Column])]
              case (_, Right3(column)) => Left(column): Either[Column, (Set[Column], Set[Column])]

              case (_, Middle3((left, right))) =>
                Right((left, right)): Either[Column, (Set[Column], Set[Column])]
            }

            val testedNum: Array[BoolColumn] = simplifiedGroupedNum.map({
              case Left(column) =>
                new BoolColumn {
                  def isDefinedAt(row: Int) = column.isDefinedAt(row)
                  def apply(row: Int)       = false
                }

              case Right((left, right)) =>
                val tests: Array[BoolColumn] = (for (l <- left; r <- right) yield {
                  new FuzzyEqColumn(l, r)
                }).toArray
                new OrLotsColumn(tests)
            })(collection.breakOut)

            val unifiedNonNum = new AndLotsColumn(testedNonNum)
            val unifiedNum    = new AndLotsColumn(testedNum)
            val unified = new BoolColumn {
              def isDefinedAt(row: Int): Boolean = unifiedNonNum.isDefinedAt(row) || unifiedNum.isDefinedAt(row)
              def apply(row: Int): Boolean = {
                val left  = !unifiedNonNum.isDefinedAt(row) || unifiedNonNum(row)
                val right = !unifiedNum.isDefinedAt(row) || unifiedNum(row)
                left && right
              }
            }

            val mask = sl.definedAt & sr.definedAt
            val column = new BoolColumn {
              def isDefinedAt(row: Int) = mask(row) && unified.isDefinedAt(row)
              def apply(row: Int)       = unified(row)
            }

            Map(ColumnRef(CPath.Identity, CBoolean) -> column)
          })

      //todo missing case WrapObjectDynamic
      val result = spec match {
        case Leaf(source) if source == Source || source == SourceLeft =>
          SliceTransform.left(())

        case Leaf(source) if source == SourceRight =>
          SliceTransform.right(())

        case Map1(source, f) =>
          composeSliceTransform2(source) map {
            _ mapRoot f
          }

        case DeepMap1(source, f) =>
          composeSliceTransform2(source) map {
            _ mapColumns f
          }

        case Map2(left, right, f) =>
          val l0 = composeSliceTransform2(left)
          val r0 = composeSliceTransform2(right)

          l0.zip(r0) { (sl, sr) =>
            Slice(
              sl.size,
              {
                val resultColumns = for {
                  cl <- sl.columns collect { case (ref, col) if ref.selector == CPath.Identity => col }
                  cr <- sr.columns collect { case (ref, col) if ref.selector == CPath.Identity => col }
                  result <- f(cl, cr)
                } yield result

                resultColumns.groupBy(_.tpe) map {
                  case (tpe, cols) =>
                    val col = cols reduceLeft { (c1, c2) =>
                      Column.unionRightSemigroup.append(c1, c2)
                    }
                    (ColumnRef(CPath.Identity, tpe), col)
                }
              })
            }

        case MapN(contents, f) =>
          composeSliceTransform2(contents) map { slice =>
            Slice(
              slice.size,
              {
                // find all of the scalar array values at root
                val cols = slice.columns.toList collect {
                  case (ref @ ColumnRef(CPath(CPathIndex(_)), _), col) => ref -> col
                }

                // group them up (note this may be sparse! that's why it must be a Map not a Vector)
                val grouped = cols groupBy {
                  case (ColumnRef(CPath(CPathIndex(i)), _), _) => i
                } map { case (i, bucket) => i -> bucket.map(_._2) }

                // to mirror the semantics of the concrete cardinalities, we do the cross-product of possible index columns
                val processed = grouped.keys.reduceOption(_ max _) map { maxIndex =>
                  def permuteFrom(i: Int): Stream[List[Column]] = {
                    if (i > maxIndex) {   // it's maxIndex, not length, so we don't want >=
                      Stream(Nil)
                    } else {
                      val potentials: Stream[Column] =
                        grouped.get(i).getOrElse(List(UndefinedColumn.raw)).toStream

                      potentials flatMap { col =>
                        permuteFrom(i + 1).map(col :: _)
                      }
                    }
                  }

                  val back: Map[ColumnRef, Column] =
                    permuteFrom(0).flatMap(f(_).toList).map({ col =>
                      ColumnRef(CPath.Identity, col.tpe) -> col
                    })(collection.breakOut)

                  back
                }

                processed.getOrElse(Map())
              })
          }

        case Filter(source, predicate) =>
          val typed = Typed(predicate, JBooleanT)
          composeSliceTransform2(source).zip(composeSliceTransform2(typed)) { (s: Slice, filter: Slice) =>
            assert(filter.size == s.size)

            if (s.columns.isEmpty) {
              s
            } else {
              val definedAt = new BitSet
              filter.columns.values foreach {
                case col: BoolColumn => cf.util.isSatisfied(col) foreach (c => definedAt or c.definedAt(0, s.size))
                case x               => abort("Unexpected: " + x)
              }
              s mapColumns { cf.util.filter(0, s.size, definedAt) }
            }
          }

        case Equal(left, right) =>
          val l0 = composeSliceTransform2(left)
          val r0 = composeSliceTransform2(right)

          l0.zip(r0)(equalsImpl)

        case EqualLiteral(source, value, invert) => {
          import cf.std.Eq

          val sourceSlice = composeSliceTransform2(source)

          def complement(col: BoolColumn) = new BoolColumn {
            def isDefinedAt(row: Int) = col.isDefinedAt(row)
            def apply(row: Int)       = !col(row)
          }

          sourceSlice map { ss =>
            Slice(
              ss.size,
              {
                val (comparable0, other0) = ss.columns.toList.partition {
                  case (ref @ ColumnRef(CPath.Identity, tpe), col) if CType.canCompare(CType.of(value), tpe) => true
                  case _                                                                                     => false
                }

                val comparable = comparable0.map(_._2).flatMap { col =>
                  Eq.partialRight(value)(col).map(_.asInstanceOf[BoolColumn])
                }
                val other = other0.map(_._2).map { col =>
                  new Map1Column(col) with BoolColumn { def apply(row: Int) = false }
                }

                val columns = comparable ++ other
                val aggregate = new BoolColumn {
                  def isDefinedAt(row: Int) = columns.exists { _.isDefinedAt(row) }
                  def apply(row: Int) = columns.exists { col =>
                    col.isDefinedAt(row) && col(row)
                  }
                }

                Map(ColumnRef(CPath.Identity, CBoolean) -> (if (invert) complement(aggregate) else aggregate))
              })
          }
        }

        case Within(item, in) =>
          composeSliceTransform2(item).zip(composeSliceTransform2(in)) { (itemS, inS) =>
            val emptyArrayDefined = inS.columns.get(ColumnRef(CPath.Identity, CEmptyArray)) map { col =>
              col.definedAt(0, inS.size)
            } getOrElse (new BitSet)

            val indices: Set[Int] = inS.columns.keySet collect {
              case ColumnRef(CPath(CPathIndex(i), _*), _) => i
            }

            // we force the slice, because we're going to re-use it many times
            val materializedItem = itemS.materialized

            val tests: List[BoolColumn] = indices.toList map { i =>
              val s = equalsImpl(materializedItem, inS.deref(CPathIndex(i))).materialized

              // we know this is defined, because of equality
              s.columns(ColumnRef(CPath.Identity, CBoolean)).asInstanceOf[BoolColumn]
            }

            val testDefined: List[BitSet] = tests.map {
              case c: ArrayBoolColumn => c.defined
              case c: SingletonBoolColumn => SingletonColumn.Defined
              case _ => errorImpossible
            }

            val definedM = testDefined.headOption map { bits =>
              val target = bits.copy    // it's important to copy, since or is in-place
              testDefined.drop(1).foreach(target.or)
              target
            }

            val testValues: List[BitSet] = tests.map {
              case c: ArrayBoolColumn => c.values
              case c: SingletonBoolColumn => SingletonBoolColumn.bitset(c.value)
              case _ => errorImpossible
            }

            val valuesM = testValues.headOption map { bits =>
              val target = bits.copy    // it's important to copy, since or is in-place
              testValues.drop(1).foreach(target.or)
              target
            }

            val defined = definedM.getOrElse(new BitSet)
            val values = valuesM.getOrElse(new BitSet)

            // we bring in empty array definedness after the fact
            // note that the value will always be false
            defined.or(emptyArrayDefined)

            val results = new ArrayBoolColumn(defined, values)

            Slice(
              materializedItem.size,
              Map(ColumnRef(CPath.Identity, CBoolean) -> results))
          }

        case ArrayLength(source) =>
          composeSliceTransform2(source).map(_.arrayLength)

        case Range(lower, upper) =>
          composeSliceTransform2(upper).zip(composeSliceTransform2(lower)) { (upperS, lowerS) =>
            val upperColumns: Map[ColumnRef, Column] = upperS.materialized.columns
            val lowerColumns: Map[ColumnRef, Column] = lowerS.materialized.columns

            val lowerC: LongColumn = lowerColumns.getOrElse(ColumnRef(CPath.Identity, CLong), {
              val numCol: NumColumn = lowerColumns(ColumnRef(CPath.Identity, CNum)).asInstanceOf[NumColumn]
              numCol match {
                case c: ArrayNumColumn => new ArrayLongColumn(c.defined, c.values.map(_.toLong))
                case c: SingletonNumColumn => SingletonLongColumn(c.value.toLong)
                case _ => errorImpossible
              }
            }).asInstanceOf[LongColumn]
            val upperC: LongColumn = upperColumns.getOrElse(ColumnRef(CPath.Identity, CLong), {
              val numCol: NumColumn = upperColumns(ColumnRef(CPath.Identity, CNum)).asInstanceOf[NumColumn]
              numCol match {
                case c: ArrayNumColumn => new ArrayLongColumn(c.defined, c.values.map(_.toLong))
                case c: SingletonNumColumn => SingletonLongColumn(c.value.toLong)
                case _ => errorImpossible
              }
            }).asInstanceOf[LongColumn]

            val lowerA: Array[Long] = lowerC match {
              case c: ArrayLongColumn => c.values
              case c: SingletonLongColumn => Array(c.value)
              case _ => errorImpossible
            }
            val upperA: Array[Long] = upperC match {
              case c: ArrayLongColumn => c.values
              case c: SingletonLongColumn => Array(c.value)
              case _ => errorImpossible
            }

            val minNumRows =
              if (lowerA.length > upperA.length) upperA.length
              else lowerA.length
            val maxNumRows =
              if (lowerA.length < upperA.length) upperA.length
              else lowerA.length
            var biggestRange = 0L
            var i = 0
            while (i < minNumRows) {
              biggestRange = java.lang.Math.max(upperA(i) - lowerA(i), biggestRange)
              i += 1
            }
            if (biggestRange > Int.MaxValue) {
              val message = s"Biggest range would have $biggestRange elements, the maximum range size is ${Int.MaxValue}"
              throw new Exception(message)
            }
            val arraysBuildr = new mutable.ListBuffer[Array[Long]]()
            val bitsetsBuildr = new mutable.ListBuffer[BitSet]()
            val stateArray: Array[Long] = Arrays.copyOf(lowerA, maxNumRows)
            val stateBitset: BitSet = new BitSet(maxNumRows)
            stateBitset.set(0, maxNumRows)
            var done = 0
            var r = 0
            while (r < stateArray.length && done != stateArray.length) {
              if (stateArray(r) > upperA(r)) {
                done += 1
                stateBitset.clear(r)
              }
              r += 1
            }
            var columns = 0
            var doneLooping = done == stateArray.length
            while (!doneLooping) {
              if (done < stateArray.length) {
                arraysBuildr += Arrays.copyOf(stateArray, maxNumRows)
                bitsetsBuildr += stateBitset.copy()
              } else {
                doneLooping = true
              }
              var row = 0
              done = 0
              while (row < minNumRows) {
                if (stateArray(row) >= upperA(row)) {
                  stateBitset.clear(row)
                  done += 1
                } else {
                  stateArray(row) = stateArray(row) + 1
                  columns += 1
                }
                row += 1
              }
            }
            def allColumns(arrs: List[Array[Long]], defineds: List[BitSet]): Map[ColumnRef, Column] = {
              @tailrec
              def rec(i: Int,
                      arrs: List[Array[Long]], defineds: List[BitSet],
                      accum: Map[ColumnRef, Column]): Map[ColumnRef, Column] =
                (arrs, defineds) match {
                  case (a :: as, d :: ds) =>
                    val ref = ColumnRef(CPath(CPathIndex(i) :: Nil), CLong)
                    val column = new ArrayLongColumn(d, a)
                    val newAccum = accum + (ref -> column)
                    rec(i + 1, as, ds, newAccum)
                  case _ => accum
                }
              rec(0, arrs, defineds, CTrie.empty)
            }
            val arrays = arraysBuildr.result()
            val bitsets = bitsetsBuildr.result()
            Slice(
              bitsets.length * maxNumRows,
              allColumns(arrays, bitsets))
          }

        case ConstLiteral(value, target) =>
          composeSliceTransform2(target) map { _.definedConst(value) }

        case WrapObject(source, field) =>
          composeSliceTransform2(source) map {
            _ wrap CPathField(field)
          }

        case WrapArray(source) =>
          composeSliceTransform2(source) map {
            _ wrap CPathIndex(0)
          }

        case OuterObjectConcat(objects @ _ *) =>
          if (objects.size == 1) {
            val typed = Typed(objects.head, JObjectUnfixedT)
            composeSliceTransform2(typed)
          } else {
            objects.map(composeSliceTransform2).reduceLeft { (l0, r0) =>
              l0.zip(r0) { (sl, sr) =>
                Slice(
                  sl.size,
                  {
                    val (leftObjectBits, leftEmptyBits)   = buildFilters(sl.columns, sl.size, filterObjects, filterEmptyObjects)
                    val (rightObjectBits, rightEmptyBits) = buildFilters(sr.columns, sr.size, filterObjects, filterEmptyObjects)

                    val (leftFields, rightFields) = buildFields(sl.columns, sr.columns)

                    val emptyBits = buildOuterBits(leftEmptyBits, rightEmptyBits, leftObjectBits, rightObjectBits)

                    val emptyObjects    = buildEmptyObjects(emptyBits)
                    val nonemptyObjects = buildNonemptyObjects(leftFields, rightFields, sr.size)

                    emptyObjects ++ nonemptyObjects
                  })
              }
            }
          }

        case InnerObjectConcat(objects @ _ *) =>
          /**
            * This test is for special casing object concats when we know we
            * won't have any unions, or funky behaviour arising from empty
            * objects.
            */
          def isDisjoint(s1: Slice, s2: Slice): Boolean = {
            false // TODO: We really want to optimize the case where
            // we are constructing a simple object from some
            // other object where usually the definedness is equal
            // on both sides, so we can just ++ the columns. But,
            // we need to be a bit smarter about checking for equal
            // definedness.

            // def containsEmptyObject(slice: Slice): Boolean =
            //   slice.columns.exists(_._1.ctype == CEmptyObject)

            // if (containsEmptyObject(s1) || containsEmptyObject(s2))
            //   return false

            // val keys = s1.columns.map(_._1.selector).toSet
            // !s2.columns.map(_._1.selector).exists(keys)
          }

          if (objects.size == 1) {
            val typed = Typed(objects.head, JObjectUnfixedT)
            composeSliceTransform2(typed)
          } else {
            objects map composeSliceTransform2 reduceLeft { (l0, r0) =>
              l0.zip(r0) { (sl0, sr0) =>
                val sl = sl0.typed(JObjectUnfixedT) // Help out the special cases.
                val sr = sr0.typed(JObjectUnfixedT)

                Slice(
                  sl.size,
                  {
                    if (sl.columns.isEmpty || sr.columns.isEmpty) {
                      CTrie.empty
                    } else if (isDisjoint(sl, sr)) {
                      // If we know sl & sr are disjoint, which is often the
                      // case for queries where objects are constructed
                      // manually, then we can do a lot less work.
                      sl.columns ++ sr.columns
                    } else {
                      val (leftObjectBits, leftEmptyBits)   = buildFilters(sl.columns, sl.size, filterObjects, filterEmptyObjects)
                      val (rightObjectBits, rightEmptyBits) = buildFilters(sr.columns, sr.size, filterObjects, filterEmptyObjects)

                      val (leftFields, rightFields) = buildFields(sl.columns, sr.columns)

                      val (emptyBits, nonemptyBits) = buildInnerBits(leftEmptyBits, rightEmptyBits, leftObjectBits, rightObjectBits)

                      val emptyObjects    = buildEmptyObjects(emptyBits)
                      val nonemptyObjects = buildNonemptyObjects(leftFields, rightFields, sr.size)

                      val result = emptyObjects ++ nonemptyObjects

                      result lazyMapValues { col =>
                        cf.util.filter(0, sl.size max sr.size, nonemptyBits)(col).get
                      }
                    }
                  })
              }
            }
          }

        case OuterArrayConcat(elements @ _ *) =>
          if (elements.size == 1) {
            val typed = Typed(elements.head, JArrayUnfixedT)
            composeSliceTransform2(typed)
          } else {
            elements.map(composeSliceTransform2).reduceLeft { (l0, r0) =>
              l0.zip(r0) { (sl, sr) =>
                Slice(
                  sl.size,
                  {
                    val (leftArrayBits, leftEmptyBits)   = buildFilters(sl.columns, sl.size, filterArrays, filterEmptyArrays)
                    val (rightArrayBits, rightEmptyBits) = buildFilters(sr.columns, sr.size, filterArrays, filterEmptyArrays)

                    val emptyBits = buildOuterBits(leftEmptyBits, rightEmptyBits, leftArrayBits, rightArrayBits)

                    val emptyArrays    = buildEmptyArrays(emptyBits)
                    val nonemptyArrays = buildNonemptyArrays(sl.columns, sr.columns)

                    emptyArrays ++ nonemptyArrays
                  })
              }
            }
          }

        case InnerArrayConcat(elements @ _ *) =>
          if (elements.size == 1) {
            val typed = Typed(elements.head, JArrayUnfixedT)
            composeSliceTransform2(typed)
          } else {
            elements.map(composeSliceTransform2).reduceLeft { (l0, r0) =>
              l0.zip(r0) { (sl, sr) =>
                Slice(
                  sl.size,
                  {
                    if (sl.columns.isEmpty || sr.columns.isEmpty) {
                      CTrie.empty
                    } else {
                      val (leftArrayBits, leftEmptyBits)   = buildFilters(sl.columns, sl.size, filterArrays, filterEmptyArrays)
                      val (rightArrayBits, rightEmptyBits) = buildFilters(sr.columns, sr.size, filterArrays, filterEmptyArrays)

                      val (emptyBits, nonemptyBits) = buildInnerBits(leftEmptyBits, rightEmptyBits, leftArrayBits, rightArrayBits)

                      val emptyArrays    = buildEmptyArrays(emptyBits)
                      val nonemptyArrays = buildNonemptyArrays(sl.columns, sr.columns)

                      val result = emptyArrays ++ nonemptyArrays

                      result lazyMapValues { col =>
                        cf.util.filter(0, sl.size max sr.size, nonemptyBits)(col).get
                      }
                    }
                  })
              }
            }
          }

        case ObjectDelete(source, mask) =>
          composeSliceTransform2(source) map {
            _ deleteFields mask
          }

        case Typed(source, tpe) =>
          composeSliceTransform2(source) map {
            _ typed tpe
          }

        case TypedSubsumes(source, tpe) =>
          composeSliceTransform2(source) map {
            _ typedSubsumes tpe
          }

        case IsType(source, tpe) =>
          composeSliceTransform2(source) map {
            _ isType tpe
          }

        case Scan(source, scanner) =>
          composeSliceTransform2(source) andThen {
            SliceTransform1.liftM[scanner.A](
              scanner.init, { (state: scanner.A, slice: Slice) =>
                val (newState, newCols) = scanner.scan(state, slice.columns, 0 until slice.size)

                val newSlice = Slice(slice.size, newCols)

                (newState, newSlice)
              }
            )
          }

        case MapWith(source, mapper0) =>
          composeSliceTransform2(source) andThen {
            mapper0.fold({ mapper =>
              SliceTransform1.liftM[Unit]((), { (_: Unit, slice: Slice) =>
                val cols = mapper.map(slice.columns, 0 until slice.size)
                ((), Slice(slice.size, cols))
              })
            }, { mapper =>
              SliceTransform1[Unit]((), { (_: Unit, slice: Slice) =>
                mapper.map(slice.columns, 0 until slice.size) map { cols =>
                  ((), Slice(slice.size, cols))
                }
              })
            })
          }

        case DerefMetadataStatic(source, field) =>
          composeSliceTransform2(source) map {
            _ deref field
          }

        case DerefObjectStatic(source, field) =>
          composeSliceTransform2(source) map {
            _ deref field
          }

        case DerefObjectDynamic(source, ref) =>
          val l0 = composeSliceTransform2(source)
          val r0 = composeSliceTransform2(ref)

          l0.zip(r0) { (slice, derefBy) =>
            assert(derefBy.columns.size <= 1)
            derefBy.columns.headOption collect {
              case (ColumnRef(CPath.Identity, CString), c: StrColumn)       =>
                DerefSlice(slice, { case row: Int if c.isDefinedAt(row) => CPathField(c(row)) })
            } getOrElse {
              slice
            }
          }

        case DerefArrayStatic(source, element) =>
          composeSliceTransform2(source) map {
            _ deref element
          }

        case DerefArrayDynamic(source, ref) =>
          val l0 = composeSliceTransform2(source)
          val r0 = composeSliceTransform2(ref)

          l0.zip(r0) { (slice, derefBy) =>
            assert(derefBy.columns.size <= 1)
            derefBy.columns.headOption collect {
              case (ColumnRef(CPath.Identity, CLong), c: LongColumn)        =>
                DerefSlice(slice, { case row: Int if c.isDefinedAt(row) => CPathIndex(c(row).toInt) })

              case (ColumnRef(CPath.Identity, CDouble), c: DoubleColumn)    =>
                DerefSlice(slice, { case row: Int if c.isDefinedAt(row) => CPathIndex(c(row).toInt) })

              case (ColumnRef(CPath.Identity, CNum), c: NumColumn)          =>
                DerefSlice(slice, { case row: Int if c.isDefinedAt(row) => CPathIndex(c(row).toInt) })
            } getOrElse {
              slice
            }
          }

        case ArraySwap(source, index) =>
          composeSliceTransform2(source) map {
            _ arraySwap index
          }

        case FilterDefined(source, definedFor, definedness) =>
          val sourceTransform = composeSliceTransform2(source)
          val keyTransform    = composeSliceTransform2(definedFor)

          sourceTransform.zip(keyTransform) { (s1, s2) =>
            s1.filterDefined(s2, definedness)
          }

        case IfUndefined(source, default) =>
          val sourceTransform = composeSliceTransform2(source)
          val defaultTransform = composeSliceTransform2(default)

          sourceTransform.zip(defaultTransform)(_.ifUndefined(_))

        case Cond(pred, left, right) => {
          val predTransform  = composeSliceTransform2(pred)
          val leftTransform  = composeSliceTransform2(left)
          val rightTransform = composeSliceTransform2(right)

          predTransform.zip2(leftTransform, rightTransform) { (predS, leftS, rightS) =>
            Slice(
              predS.size,
              {
                val size = predS.size

                predS.columns get ColumnRef(CPath.Identity, CBoolean) map { predC =>
                  val leftMask = predC.asInstanceOf[BoolColumn].asBitSet(false, size)

                  val rightMask = predC.asInstanceOf[BoolColumn].asBitSet(true, size)
                  rightMask.flip(0, size)

                  val grouped = (leftS.columns mapValues { _ :: Nil }) cogroup (rightS.columns mapValues { _ :: Nil })

                  val joined: Map[ColumnRef, Column] = grouped.map({
                    case (ref, Left3(col))  => ref -> cf.util.filter(0, size, leftMask)(col).get
                    case (ref, Right3(col)) => ref -> cf.util.filter(0, size, rightMask)(col).get
                    case (ref, Middle3((left :: Nil, right :: Nil))) => {
                      val left2  = cf.util.filter(0, size, leftMask)(left).get
                      val right2 = cf.util.filter(0, size, rightMask)(right).get

                      ref -> cf.util.MaskedUnion(leftMask)(left2, right2).get // safe because types are grouped
                    }
                    case (_, x) => abort("Unexpected: " + x)
                  })(collection.breakOut)

                  joined
                } getOrElse Map[ColumnRef, Column]()
              })
          }
        }

        case ToNumber(source) =>
          composeSliceTransform2(source) map {
            _ toNumber
          }
      }

      result
    }
  }

  protected sealed trait SliceTransform1[A] {
    import SliceTransform1._

    def initial: A
    def f: (A, Slice) => IO[(A, Slice)]
    def advance(slice: Slice): IO[(SliceTransform1[A], Slice)]

    def unlift: Option[(A, Slice) => (A, Slice)] = None

    def apply(slice: Slice): IO[(A, Slice)] = f(initial, slice)

    def mapState[B](f: A => B, g: B => A): SliceTransform1[B] =
      MappedState1[A, B](this, f, g)

    def zip[B](that: SliceTransform1[B])(combine: (Slice, Slice) => Slice): SliceTransform1[(A, B)] = {
      (this, that) match {
        case (sta: SliceTransform1S[_], stb: SliceTransform1S[_]) =>
          SliceTransform1S[(A, B)]((sta.initial, stb.initial), {
            case ((a0, b0), s0) =>
              val (a, sa) = sta.f0(a0, s0)
              val (b, sb) = stb.f0(b0, s0)
              assert(sa.size == sb.size)
              ((a, b), combine(sa, sb))
          })

        case (sta: SliceTransform1S[_], stb) =>
          SliceTransform1M[(A, B)]((sta.initial, stb.initial), {
            case ((a0, b0), s0) =>
              val (a, sa) = sta.f0(a0, s0)
              stb.f(b0, s0) map {
                case (b, sb) =>
                  assert(sa.size == sb.size)
                  ((a, b), combine(sa, sb))
              }
          })

        case (sta, stb: SliceTransform1S[_]) =>
          SliceTransform1M[(A, B)]((sta.initial, stb.initial), {
            case ((a0, b0), s0) =>
              sta.f(a0, s0) map {
                case (a, sa) =>
                  val (b, sb) = stb.f0(b0, s0)
                  assert(sa.size == sb.size)
                  ((a, b), combine(sa, sb))
              }
          })

        case (sta, stb) =>
          SliceTransform1[(A, B)]((sta.initial, stb.initial), {
            case ((a0, b0), s0) =>
              for (ares <- sta.f(a0, s0); bres <- stb.f(b0, s0)) yield {
                val (a, sa) = ares
                val (b, sb) = bres
                assert(sa.size == sb.size)
                ((a, b), combine(sa, sb))
              }
          })
      }
    }

    def zip2[B, C](t: SliceTransform1[B], t2: SliceTransform1[C])(combine: (Slice, Slice, Slice) => Slice): SliceTransform1[(A, B, C)] = {

      // We can do this in 4 cases efficiently simply be re-ordering the 3 sts.
      // Since they're done in parallel, we just need to make sure combine works.

      (this, t, t2) match {
        case (sta: SliceTransform1S[_], stb: SliceTransform1S[_], stc: SliceTransform1S[_]) =>
          SliceTransform1S((sta.initial, stb.initial, stc.initial), {
            case ((a0, b0, c0), s0) =>
              val (a, sa) = sta.f0(a0, s0)
              val (b, sb) = stb.f0(b0, s0)
              val (c, sc) = stc.f0(c0, s0)
              ((a, b, c), combine(sa, sb, sc))
          })

        case (sta, stb, stc) =>
          SliceTransform1M((sta.initial, stb.initial, stc.initial), {
            case ((a0, b0, c0), s0) =>
              for {
                resa <- sta.f(a0, s0)
                resb <- stb.f(b0, s0)
                resc <- stc.f(c0, s0)
              } yield {
                val (a, sa) = resa
                val (b, sb) = resb
                val (c, sc) = resc
                ((a, b, c), combine(sa, sb, sc))
              }
          })
      }
    }

    def map(mapFunc: Slice => Slice): SliceTransform1[A] = SliceTransform1.map(this)(mapFunc)

    def andThen[B](that: SliceTransform1[B]): SliceTransform1[(A, B)] = SliceTransform1.chain(this, that)
  }

  object SliceTransform1 {

    def liftM[A](init: A, f: (A, Slice) => (A, Slice)): SliceTransform1[A] =
      SliceTransform1S(init, f)

    def apply[A](init: A, f: (A, Slice) => IO[(A, Slice)]): SliceTransform1[A] =
      SliceTransform1M(init, f)

    private[table] val Identity: SliceTransform1S[Unit] = SliceTransform1S[Unit]((), { (u, s) =>
      (u, s)
    })

    private[table] def mapS[A](st: SliceTransform1S[A])(f: Slice => Slice): SliceTransform1S[A] =
      SliceTransform1S(st.initial, { case (a, s) => st.f0(a, s) :-> f })

    private def map[A](st: SliceTransform1[A])(f: Slice => Slice): SliceTransform1[A] = st match {
      case (st: SliceTransform1S[_])         => mapS(st)(f)
      case SliceTransform1M(i, g)            => SliceTransform1M(i, { case (a, s) => g(a, s) map (_ :-> f) })
      case SliceTransform1SMS(sta, stb, stc) => SliceTransform1SMS(sta, stb, mapS(stc)(f))
      case MappedState1(sta, to, from)       => MappedState1(map(sta)(f), to, from)
    }

    private def chainS[A, B](sta: SliceTransform1S[A], stb: SliceTransform1S[B]): SliceTransform1S[(A, B)] = {
      (sta, stb) match {
        case (Identity, stb) =>
          SliceTransform1S((sta.initial, stb.initial), {
            case ((_, b0), s0) => { (b: B) =>
              (sta.initial, b)
            } <-: stb.f0(b0, s0)
          })
        case (sta, Identity) =>
          SliceTransform1S((sta.initial, stb.initial), {
            case ((a0, _), s0) => { (a: A) =>
              (a, stb.initial)
            } <-: sta.f0(a0, s0)
          })
        case (SliceTransform1S(i1, f1), SliceTransform1S(i2, f2)) =>
          SliceTransform1S((i1, i2), {
            case ((a0, b0), s0) =>
              val (a, s1) = f1(a0, s0)
              val (b, s)  = f2(b0, s1)
              ((a, b), s)
          })
      }
    }

    // Note: This is here, rather than in SliceTransform1 trait, because Scala's
    // type unification doesn't deal well with `this`.
    private def chain[A, B](st0: SliceTransform1[A], st1: SliceTransform1[B]): SliceTransform1[(A, B)] = {
      (st0, st1) match {
        case (sta: SliceTransform1S[_], stb: SliceTransform1S[_]) =>
          chainS(sta, stb)

        case (SliceTransform1M(i0, f0), SliceTransform1M(i1, f1)) =>
          SliceTransform1M((i0, i1), {
            case ((a0, b0), s0) =>
              for (r0 <- f0(i0, s0); r1 <- f1(i1, r0._2)) yield ((r0._1, r1._1), r1._2)
          })

        case (sta: SliceTransform1S[_], stb: SliceTransform1M[_]) =>
          val st = SliceTransform1SMS(sta, stb, Identity)
          st.mapState[(A, B)]({ case (a, b, _) => (a, b) }, { case (a, b) => (a, b, ()) })

        case (sta: SliceTransform1M[_], stb: SliceTransform1S[_]) =>
          val st = SliceTransform1SMS(Identity, sta, stb)
          st.mapState[(A, B)]({ case (_, a, b) => (a, b) }, { case (a, b) => ((), a, b) })

        case (sta: SliceTransform1S[_], SliceTransform1SMS(stb, stc, std)) =>
          val st = SliceTransform1SMS(chainS(sta, stb), stc, std)
          st.mapState[(A, (Any, Any, Any))]({ case ((a, b), c, d) => (a, (b, c, d)) }, { case (a, (b, c, d)) => ((a, b), c, d) })

        case (SliceTransform1SMS(sta, stb, stc), std: SliceTransform1S[_]) =>
          val st = SliceTransform1SMS(sta, stb, chainS(stc, std))
          st.mapState[((Any, Any, Any), B)]({ case (a, b, (c, d)) => ((a, b, c), d) }, { case ((a, b, c), d) => (a, b, (c, d)) })

        case (sta: SliceTransform1M[_], SliceTransform1SMS(stb, stc, std)) =>
          val st = SliceTransform1SMS(Identity, sta andThen stb andThen stc, std)
          st.mapState[(A, (Any, Any, Any))]({ case (_, ((a, b), c), d) => (a, (b, c, d)) }, { case (a, (b, c, d)) => ((), ((a, b), c), d) })

        case (SliceTransform1SMS(sta, stb, stc), std: SliceTransform1M[_]) =>
          val st = SliceTransform1SMS(sta, stb andThen stc andThen std, Identity)
          st.mapState[((Any, Any, Any), B)]({ case (a, ((b, c), d), _) => ((a, b, c), d) }, { case ((a, b, c), d) => (a, ((b, c), d), ()) })

        case (SliceTransform1SMS(sta, stb, stc), SliceTransform1SMS(std, ste, stf)) =>
          val st = SliceTransform1SMS(sta, stb andThen stc andThen std andThen ste, stf)
          st.mapState[((Any, Any, Any), (Any, Any, Any))]({ case (a, (((b, c), d), e), f) => ((a, b, c), (d, e, f)) }, { case ((a, b, c), (d, e, f)) => (a, (((b, c), d), e), f) })

        case (MappedState1(sta, f, g), stb) =>
          (sta andThen stb).mapState(f <-: _, g <-: _)

        case (sta, MappedState1(stb, f, g)) =>
          (sta andThen stb).mapState(_ :-> f, _ :-> g)
      }
    }

    private[table] case class SliceTransform1S[A](initial: A, f0: (A, Slice) => (A, Slice)) extends SliceTransform1[A] {
      override def unlift = Some(f0)
      val f: (A, Slice) => IO[(A, Slice)] = { case (a, s) => IO(f0(a, s)) }
      def advance(s: Slice): IO[(SliceTransform1[A], Slice)] =
        IO({ (a: A) =>
          SliceTransform1S[A](a, f0)
        } <-: f0(initial, s))
    }

    private[table] case class SliceTransform1M[A](initial: A, f: (A, Slice) => IO[(A, Slice)]) extends SliceTransform1[A] {
      def advance(s: Slice): IO[(SliceTransform1[A], Slice)] = apply(s) map {
        case (next, slice) =>
          (SliceTransform1M[A](next, f), slice)
      }
    }

    private[table] case class SliceTransform1SMS[A, B, C](before: SliceTransform1S[A], transM: SliceTransform1[B], after: SliceTransform1S[C])
        extends SliceTransform1[(A, B, C)] {
      def initial: (A, B, C) = (before.initial, transM.initial, after.initial)

      val f: ((A, B, C), Slice) => IO[((A, B, C), Slice)] = {
        case ((a0, b0, c0), s) =>
          val (a, slice0) = before.f0(a0, s)
          transM.f(b0, slice0) map {
            case (b, slice1) =>
              val (c, slice) = after.f0(c0, slice1)
              ((a, b, c), slice)
          }
      }

      def advance(s: Slice): IO[(SliceTransform1[(A, B, C)], Slice)] = apply(s) map {
        case ((a, b, c), slice) =>
          val transM0 = SliceTransform1M(b, transM.f)
          (SliceTransform1SMS[A, B, C](before.copy(initial = a), transM0, after.copy(initial = c)), slice)
      }
    }

    private[table] case class MappedState1[A, B](st: SliceTransform1[A], to: A => B, from: B => A) extends SliceTransform1[B] {
      def initial: B = to(st.initial)
      def f: (B, Slice) => IO[(B, Slice)] = { (b, s) =>
        st.f(from(b), s) map (to <-: _)
      }
      def advance(s: Slice): IO[(SliceTransform1[B], Slice)] =
        st.advance(s) map { case (st0, s0) => (MappedState1[A, B](st0, to, from), s0) }
    }
  }

  protected sealed trait SliceTransform2[A] {
    import SliceTransform2._

    def initial: A
    def f: (A, Slice, Slice) => IO[(A, Slice)]
    def advance(sl: Slice, sr: Slice): IO[(SliceTransform2[A], Slice)]

    def unlift: Option[(A, Slice, Slice) => (A, Slice)] = None

    def apply(sl: Slice, sr: Slice): IO[(A, Slice)] = f(initial, sl, sr)

    def mapState[B](f: A => B, g: B => A): SliceTransform2[B] =
      MappedState2[A, B](this, f, g)

    def zip[B](that: SliceTransform2[B])(combine: (Slice, Slice) => Slice): SliceTransform2[(A, B)] = {
      (this, that) match {
        case (sta: SliceTransform2S[_], stb: SliceTransform2S[_]) =>
          SliceTransform2S[(A, B)]((sta.initial, stb.initial), {
            case ((a0, b0), sl0, sr0) =>
              val (a, sa) = sta.f0(a0, sl0, sr0)
              val (b, sb) = stb.f0(b0, sl0, sr0)
              assert(sa.size == sb.size)
              ((a, b), combine(sa, sb))
          })

        case (sta: SliceTransform2S[_], stb) =>
          SliceTransform2M[(A, B)]((sta.initial, stb.initial), {
            case ((a0, b0), sl0, sr0) =>
              val (a, sa) = sta.f0(a0, sl0, sr0)
              stb.f(b0, sl0, sr0) map {
                case (b, sb) =>
                  assert(sa.size == sb.size)
                  ((a, b), combine(sa, sb))
              }
          })

        case (sta, stb: SliceTransform2S[_]) =>
          SliceTransform2M[(A, B)]((sta.initial, stb.initial), {
            case ((a0, b0), sl0, sr0) =>
              sta.f(a0, sl0, sr0) map {
                case (a, sa) =>
                  val (b, sb) = stb.f0(b0, sl0, sr0)
                  assert(sa.size == sb.size)
                  ((a, b), combine(sa, sb))
              }
          })

        case (sta, stb) =>
          SliceTransform2[(A, B)]((sta.initial, stb.initial), {
            case ((a0, b0), sl0, sr0) =>
              for (ares <- sta.f(a0, sl0, sr0); bres <- stb.f(b0, sl0, sr0)) yield {
                val (a, sa) = ares
                val (b, sb) = bres
                assert(sa.size == sb.size)
                ((a, b), combine(sa, sb))
              }
          })
      }
    }

    def zip2[B, C](t: SliceTransform2[B], t2: SliceTransform2[C])(combine: (Slice, Slice, Slice) => Slice): SliceTransform2[(A, B, C)] = {

      // We can do this in 4 cases efficiently simply be re-ordering the 3 sts.
      // Since they're done in parallel, we just need to make sure combine works.

      (this, t, t2) match {
        case (sta: SliceTransform2S[_], stb: SliceTransform2S[_], stc: SliceTransform2S[_]) =>
          SliceTransform2S((sta.initial, stb.initial, stc.initial), {
            case ((a0, b0, c0), sl0, sr0) =>
              val (a, sa) = sta.f0(a0, sl0, sr0)
              val (b, sb) = stb.f0(b0, sl0, sr0)
              val (c, sc) = stc.f0(c0, sl0, sr0)
              ((a, b, c), combine(sa, sb, sc))
          })

        case (sta, stb, stc) =>
          SliceTransform2M((sta.initial, stb.initial, stc.initial), {
            case ((a0, b0, c0), sl0, sr0) =>
              for {
                resa <- sta.f(a0, sl0, sr0)
                resb <- stb.f(b0, sl0, sr0)
                resc <- stc.f(c0, sl0, sr0)
              } yield {
                val (a, sa) = resa
                val (b, sb) = resb
                val (c, sc) = resc
                ((a, b, c), combine(sa, sb, sc))
              }
          })
      }
    }

    def map(mapFunc: Slice => Slice): SliceTransform2[A] = SliceTransform2.map(this)(mapFunc)

    def andThen[B](that: SliceTransform1[B]): SliceTransform2[(A, B)] = SliceTransform2.chain(this, that)

    def parallel: SliceTransform1[A] = this match {
      case (st: SliceTransform2S[_]) =>
        SliceTransform1.liftM[A](initial, { (a, s) =>
          st.f0(a, s, s)
        })
      case _ =>
        SliceTransform1[A](initial, { (a, s) =>
          f(a, s, s)
        })
    }
  }

  object SliceTransform2 {
    import SliceTransform1._

    def liftM[A](init: A, f: (A, Slice, Slice) => (A, Slice)): SliceTransform2[A] =
      SliceTransform2S(init, f)

    def apply[A](init: A, f: (A, Slice, Slice) => IO[(A, Slice)]): SliceTransform2[A] =
      SliceTransform2M(init, f)

    private def mapS[A](st: SliceTransform2S[A])(f: Slice => Slice): SliceTransform2S[A] =
      SliceTransform2S(st.initial, { case (a, sl, sr) => st.f0(a, sl, sr) :-> f })

    private def map[A](st: SliceTransform2[A])(f: Slice => Slice): SliceTransform2[A] = st match {
      case (st: SliceTransform2S[_])   => mapS(st)(f)
      case SliceTransform2M(i, g)      => SliceTransform2M(i, { case (a, sl, sr) => g(a, sl, sr) map (_ :-> f) })
      case SliceTransform2SM(sta, stb) => SliceTransform2SM(sta, stb map f)
      case SliceTransform2MS(sta, stb) => SliceTransform2MS(sta, SliceTransform1.mapS(stb)(f))
      case MappedState2(sta, to, from) => MappedState2(map(sta)(f), to, from)
    }

    private def chainS[A, B](sta: SliceTransform2S[A], stb: SliceTransform1S[B]): SliceTransform2S[(A, B)] = {
      (sta, stb) match {
        case (sta, SliceTransform1.Identity) =>
          SliceTransform2S((sta.initial, stb.initial), {
            case ((a0, _), sl0, sr0) => { (a: A) =>
              (a, stb.initial)
            } <-: sta.f0(a0, sl0, sr0)
          })
        case (SliceTransform2S(i1, f1), SliceTransform1S(i2, f2)) =>
          SliceTransform2S((i1, i2), {
            case ((a0, b0), sl0, sr0) =>
              val (a, s1) = f1(a0, sl0, sr0)
              val (b, s)  = f2(b0, s1)
              ((a, b), s)
          })
      }
    }

    private def chain[A, B](st0: SliceTransform2[A], st1: SliceTransform1[B]): SliceTransform2[(A, B)] = {
      (st0, st1) match {
        case (sta, MappedState1(stb, f, g)) =>
          chain(sta, stb).mapState(_ :-> f, _ :-> g)

        case (sta: SliceTransform2S[_], stb: SliceTransform1S[_]) =>
          chainS(sta, stb)

        case (sta: SliceTransform2S[_], stb: SliceTransform1[_]) =>
          SliceTransform2SM(sta, stb)

        case (sta: SliceTransform2M[_], stb: SliceTransform1S[_]) =>
          SliceTransform2MS(sta, stb)

        case (sta: SliceTransform2M[_], stb: SliceTransform1[_]) =>
          SliceTransform2M((sta.initial, stb.initial), {
            case ((a0, b0), sl0, sr0) =>
              sta.f(a0, sl0, sr0) flatMap {
                case (a, s0)                      =>
                  stb.f(b0, s0) map { case (b, s) => ((a, b), s) }
              }
          })

        case (SliceTransform2SM(sta, stb), stc) =>
          val st = SliceTransform2SM(sta, stb andThen stc)
          st.mapState[((Any, Any), B)]({ case (a, (b, c)) => ((a, b), c) }, { case ((a, b), c) => (a, (b, c)) })

        case (SliceTransform2MS(sta, stb), stc) =>
          val st = chain(sta, stb andThen stc)
          st.mapState[((Any, Any), B)]({ case (a, (b, c)) => ((a, b), c) }, { case ((a, b), c) => (a, (b, c)) })

        case (MappedState2(sta, f, g), stb) =>
          chain(sta, stb).mapState(f <-: _, g <-: _)
      }
    }

    private case class SliceTransform2S[A](initial: A, f0: (A, Slice, Slice) => (A, Slice)) extends SliceTransform2[A] {
      override def unlift = Some(f0)
      val f: (A, Slice, Slice) => IO[(A, Slice)] = { case (a, sl, sr) => IO(f0(a, sl, sr)) }
      def advance(sl: Slice, sr: Slice): IO[(SliceTransform2[A], Slice)] =
        IO({ (a: A) =>
          SliceTransform2S[A](a, f0)
        } <-: f0(initial, sl, sr))
    }

    private case class SliceTransform2M[A](initial: A, f: (A, Slice, Slice) => IO[(A, Slice)]) extends SliceTransform2[A] {
      def advance(sl: Slice, sr: Slice): IO[(SliceTransform2[A], Slice)] = apply(sl, sr) map {
        case (next, slice) =>
          (SliceTransform2M[A](next, f), slice)
      }
    }

    private case class SliceTransform2SM[A, B](before: SliceTransform2S[A], after: SliceTransform1[B]) extends SliceTransform2[(A, B)] {
      def initial: (A, B) = (before.initial, after.initial)

      val f: ((A, B), Slice, Slice) => IO[((A, B), Slice)] = {
        case ((a0, b0), sl0, sr0) =>
          val (a, s0) = before.f0(a0, sl0, sr0)
          after.f(b0, s0) map { case (b, s) => ((a, b), s) }
      }

      def advance(sl: Slice, sr: Slice): IO[(SliceTransform2[(A, B)], Slice)] = apply(sl, sr) map {
        case ((a, b), slice) =>
          val after0 = SliceTransform1M(b, after.f)
          (SliceTransform2SM[A, B](before.copy(initial = a), after0), slice)
      }
    }

    private case class SliceTransform2MS[A, B](before: SliceTransform2[A], after: SliceTransform1S[B]) extends SliceTransform2[(A, B)] {
      def initial: (A, B) = (before.initial, after.initial)

      val f: ((A, B), Slice, Slice) => IO[((A, B), Slice)] = {
        case ((a0, b0), sl0, sr0) =>
          before.f(a0, sl0, sr0) map {
            case (a, s0) =>
              val (b, s) = after.f0(b0, s0)
              ((a, b), s)
          }
      }

      def advance(sl: Slice, sr: Slice): IO[(SliceTransform2[(A, B)], Slice)] = apply(sl, sr) map {
        case ((a, b), slice) =>
          val before0 = SliceTransform2M(a, before.f)
          (SliceTransform2MS[A, B](before0, after.copy(initial = b)), slice)
      }
    }

    private case class MappedState2[A, B](st: SliceTransform2[A], to: A => B, from: B => A) extends SliceTransform2[B] {
      def initial: B = to(st.initial)
      def f: (B, Slice, Slice) => IO[(B, Slice)] = { (b, sl, sr) =>
        st.f(from(b), sl, sr) map (to <-: _)
      }
      def advance(sl: Slice, sr: Slice): IO[(SliceTransform2[B], Slice)] =
        st.advance(sl, sr) map { case (st0, s0) => (MappedState2[A, B](st0, to, from), s0) }
    }
  }
}

trait ConcatHelpers {
  def buildFilters(columns: Map[ColumnRef, Column],
                   size: Int,
                   filter: Map[ColumnRef, Column] => Map[ColumnRef, Column],
                   filterEmpty: Map[ColumnRef, Column] => Map[ColumnRef, Column]) = {
    val definedBits = filter(columns).values.map(_.definedAt(0, size)).reduceOption(_ | _) getOrElse new BitSet
    val emptyBits   = filterEmpty(columns).values.map(_.definedAt(0, size)).reduceOption(_ | _) getOrElse new BitSet
    (definedBits, emptyBits)
  }

  def buildOuterBits(leftEmptyBits: BitSet, rightEmptyBits: BitSet, leftDefinedBits: BitSet, rightDefinedBits: BitSet): BitSet = {
    (rightEmptyBits & leftEmptyBits) |
      (rightEmptyBits &~ leftDefinedBits) |
      (leftEmptyBits &~ rightDefinedBits)
  }

  def buildInnerBits(leftEmptyBits: BitSet, rightEmptyBits: BitSet, leftDefinedBits: BitSet, rightDefinedBits: BitSet) = {
    val emptyBits    = rightEmptyBits & leftEmptyBits
    val nonemptyBits = leftDefinedBits & rightDefinedBits
    (emptyBits, nonemptyBits)
  }
}

trait ArrayConcatHelpers extends ConcatHelpers {
  def filterArrays(columns: Map[ColumnRef, Column]) = columns.filter {
    case (ColumnRef(CPath(CPathIndex(_), _ @_ *), _), _) => true
    case (ColumnRef(CPath.Identity, CEmptyArray), _)     => true
    case _                                               => false
  }

  def filterEmptyArrays(columns: Map[ColumnRef, Column]) = columns.filter {
    case (ColumnRef(CPath.Identity, CEmptyArray), _) => true
    case _                                           => false
  }

  def collectIndices(columns: Map[ColumnRef, Column]) = columns.collect {
    case (ref @ ColumnRef(CPath(CPathIndex(i), xs @ _ *), ctype), col) => (i, xs, ref, col)
  }

  def buildEmptyArrays(emptyBits: BitSet) = Map(ColumnRef(CPath.Identity, CEmptyArray) -> EmptyArrayColumn(emptyBits))

  def buildNonemptyArrays(left: Map[ColumnRef, Column], right: Map[ColumnRef, Column]) = {
    val leftIndices  = collectIndices(left)
    val rightIndices = collectIndices(right)

    val maxId = if (leftIndices.isEmpty) -1 else leftIndices.map(_._1).max
    val newCols = (leftIndices map { case (_, _, ref, col) => ref -> col }) ++
        (rightIndices map { case (i, xs, ref, col) => ColumnRef(CPath(CPathIndex(i + maxId + 1) :: xs.toList), ref.ctype) -> col })

    newCols.toMap
  }
}

trait ObjectConcatHelpers extends ConcatHelpers {
  def filterObjects(columns: Map[ColumnRef, Column]) = columns.filter {
    case (ColumnRef(CPath(CPathField(_), _ @_ *), _), _) => true
    case (ColumnRef(CPath.Identity, CEmptyObject), _)    => true
    case _                                               => false
  }

  def filterEmptyObjects(columns: Map[ColumnRef, Column]) = columns.filter {
    case (ColumnRef(CPath.Identity, CEmptyObject), _) => true
    case _                                            => false
  }

  def filterFields(columns: Map[ColumnRef, Column]) = columns.filter {
    case (ColumnRef(CPath(CPathField(_), _ @_ *), _), _) => true
    case _                                               => false
  }

  def buildFields(leftColumns: Map[ColumnRef, Column], rightColumns: Map[ColumnRef, Column]) =
    (filterFields(leftColumns), filterFields(rightColumns))

  def buildEmptyObjects(emptyBits: BitSet) = {
    if (emptyBits.isEmpty) CTrie.emptyCol
    else CTrie(ColumnRef(CPath.Identity, CEmptyObject) -> EmptyObjectColumn(emptyBits))
  }

  def buildNonemptyObjects(
      leftFields: Map[ColumnRef, Column],
      rightFields: Map[ColumnRef, Column],
      rightSize: Int): Map[ColumnRef, Column] = {

    val leftPrefixes: Set[CPathField] = leftFields.keySet collect {
      case ColumnRef(CPath(field @ CPathField(_), _*), _) => field
    }

    val rightPrefixes: Set[CPathField] = rightFields.keySet collect {
      case ColumnRef(CPath(field @ CPathField(_), _*), _) => field
    }

    val innerPrefixes = leftPrefixes.intersect(rightPrefixes)

    val (leftInner, leftOuter) = leftFields partition {
      case (ColumnRef(path, _), _) => innerPrefixes.exists(path.hasPrefixComponent)
    }

    val (rightInner, rightOuter) = rightFields partition {
      case (ColumnRef(path, _), _) => innerPrefixes.exists(path.hasPrefixComponent)
    }

    // for all overlapping prefixes between left and right
    val mergedPairs: Map[ColumnRef, Column] = innerPrefixes.flatMap({ prefix =>
      // find the left columns with the prefix
      val leftSelection = leftInner filter {
        case (ColumnRef(path, _), _) => path.hasPrefixComponent(prefix)
      }

      // find the right columns with the prefix
      val rightSelection = rightInner filter {
        case (ColumnRef(path, _), _) => path.hasPrefixComponent(prefix)
      }

      // where is this subset of right columns defined
      // wherever they are defined, we need to poke holes in the left at this prefix
      val mask = new BitSet(rightSize)
      rightSelection foreach {
        case (_, col) => mask.or(col.definedAt(0, rightSize))
      }

      // it's more efficient to use cf.util.filter, but that requires leftSize
      val filter = cf.util.filterBy(i => !mask.get(i))

      // poke holes in the left WHEREVER the right is defined (at the current prefix)
      val leftMasked = leftSelection map {
        case (ref, col) => ref -> filter(col).get
      }

      // find all of the shared path/type pairs EXACTLY at prefix
      val toMergeRefs = leftSelection.keySet filter {
        case ref @ ColumnRef(CPath(prefix), _) => rightSelection.contains(ref)
        case _ => false
      }

      // pull out the merge refs; we're just going to catenate all this stuff
      val leftUnmerged = leftMasked -- toMergeRefs
      val rightUnmerged = rightSelection -- toMergeRefs

      // merge the pairs we found
      val merged: Map[ColumnRef, Column] = toMergeRefs.map({ ref =>
        ref -> cf.util.UnionRight(leftMasked(ref), rightSelection(ref)).get
      })(collection.breakOut)

      // catenate all the things
      leftUnmerged ++ rightUnmerged ++ merged
    })(collection.breakOut)

    // ...ditto
    leftOuter ++ rightOuter ++ mergedPairs
  }
}
