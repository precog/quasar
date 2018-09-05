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

package quasar.yggdrasil.table

import quasar.blueeyes._, json._
import quasar.contrib.std.errorImpossible
import quasar.frontend.data.DataCodec
import quasar.precog._
import quasar.precog.common._
import quasar.precog.util._
import quasar.precog.util.RingDeque
import qdata.time.{DateTimeInterval, OffsetDate}
import quasar.yggdrasil._
import quasar.yggdrasil.TransSpecModule._
import quasar.yggdrasil.bytecode._
import quasar.yggdrasil.util.{CPathUtils, RangeUtil}

import scalaz._, Scalaz._, Ordering._
import shims._

import java.nio.CharBuffer
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}

import scala.annotation.{switch, tailrec}
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.specialized

abstract class Slice { source =>
  import Slice._
  import TableModule._

  def size: Int
  def isEmpty: Boolean = size == 0
  def nonEmpty         = !isEmpty

  def columns: Map[ColumnRef, Column]

  def groupedColumnRefs: Map[CPath, Set[CType]] =
    columns.keys.groupBy(_.selector).mapValues(_.map(_.ctype).toSet)

  def logicalColumns: JType => Set[Column] = { jtpe =>
    // TODO Use a flatMap and:
    // If ColumnRef(_, CArrayType(_)) and jType has a JArrayFixedT of this type,
    //   then we need to map these to multiple columns.
    // Else if Schema.includes(...), then return List(col).
    // Otherwise return Nil.
    columns collect {
      case (ColumnRef(cpath, ctype), col) if Schema.includes(jtpe, cpath, ctype) => col
    } toSet
  }

  lazy val valueColumns: Set[Column] = columns collect { case (ColumnRef(CPath.Identity, _), col) => col } toSet

  def isDefinedAt(row: Int) = columns.values.exists(_.isDefinedAt(row))

  def definedAt: BitSet = {
    val defined = BitSetUtil.create()
    columns foreach {
      case (_, col) =>
        defined.or(col.definedAt(0, size))
    }
    defined
  }

  def mapRoot(f: CF1): Slice = Slice(
    source.size,
    {
      val resultColumns = for {
        col <- source.columns collect { case (ref, col) if ref.selector == CPath.Identity => col }
        result <- f(col)
      } yield result

      resultColumns.groupBy(_.tpe) map {
        case (tpe, cols) => (ColumnRef(CPath.Identity, tpe), cols.reduceLeft((c1, c2) => Column.unionRightSemigroup.append(c1, c2)))
      }
    })

  def mapColumns(f: CF1): Slice = Slice(
    source.size,
    {
      val resultColumns: Seq[(ColumnRef, Column)] = for {
        (ref, col) <- source.columns.toSeq
        result <- f(col)
      } yield (ref.copy(ctype = result.tpe), result)

      resultColumns.groupBy(_._1) map {
        case (ref, pairs) => (ref, pairs.map(_._2).reduceLeft((c1, c2) => Column.unionRightSemigroup.append(c1, c2)))
      } toMap
    })

  def toArray[A](implicit tpe0: CValueType[A]) = {
    val size = source.size

    val cols0 = (source.columns).toList sortBy { case (ref, _) => ref.selector }
    val cols  = cols0 map { case (_, col) => col }

    def inflate[@specialized X: ClassTag](cols: Array[Int => X], row: Int) = {
      val as = new Array[X](cols.length)
      var i = 0
      while (i < cols.length) {
        as(i) = cols(i)(row)
        i += 1
      }
      as
    }

    def loopForall[X <: Column](cols: Array[X])(row: Int) = !cols.isEmpty && Loop.forall(cols)(_ isDefinedAt row)

    val columns: Map[ColumnRef, Column] = {
      Map((ColumnRef(CPath(CPathArray), CArrayType(tpe0)), tpe0 match {
        case CLong =>
          val longcols = cols.collect { case (col: LongColumn) => col }.toArray

          new HomogeneousArrayColumn[Long] {
            private val cols: Array[Int => Long] = longcols map { col =>
              col.apply _
            }

            val tpe = CArrayType(CLong)
            def isDefinedAt(row: Int)        = loopForall[LongColumn](longcols)(row)
            def apply(row: Int): Array[Long] = inflate(cols, row)
          }
        case CDouble =>
          val doublecols = cols.collect { case (col: DoubleColumn) => col }.toArray
          new HomogeneousArrayColumn[Double] {
            private val cols: Array[Int => Double] = doublecols map { x =>
              x(_)
            }

            val tpe = CArrayType(CDouble)
            def isDefinedAt(row: Int)          = loopForall[DoubleColumn](doublecols)(row)
            def apply(row: Int): Array[Double] = inflate(cols, row)
          }
        case CNum =>
          val numcols = cols.collect { case (col: NumColumn) => col }.toArray
          new HomogeneousArrayColumn[BigDecimal] {
            private val cols: Array[Int => BigDecimal] = numcols map { x =>
              x(_)
            }

            val tpe = CArrayType(CNum)
            def isDefinedAt(row: Int)              = loopForall[NumColumn](numcols)(row)
            def apply(row: Int): Array[BigDecimal] = inflate(cols, row)
          }
        case CBoolean =>
          val boolcols = cols.collect { case (col: BoolColumn) => col }.toArray
          new HomogeneousArrayColumn[Boolean] {
            private val cols: Array[Int => Boolean] = boolcols map { x =>
              x(_)
            }

            val tpe = CArrayType(CBoolean)
            def isDefinedAt(row: Int)           = loopForall[BoolColumn](boolcols)(row)
            def apply(row: Int): Array[Boolean] = inflate(cols, row)
          }
        case CString =>
          val strcols = cols.collect { case (col: StrColumn) => col }.toArray
          new HomogeneousArrayColumn[String] {
            private val cols: Array[Int => String] = strcols map { x =>
              x(_)
            }

            val tpe = CArrayType(CString)
            def isDefinedAt(row: Int)          = loopForall[StrColumn](strcols)(row)
            def apply(row: Int): Array[String] = inflate(cols, row)
          }
        case _ => sys.error("unsupported type")
      }))
    }
    Slice(size, columns)
  }

  /**
    * Transform this slice such that its columns are only defined for row indices
    * in the given BitSet.
    */
  def redefineWith(s: BitSet): Slice = mapColumns(cf.util.filter(0, size, s))

  def definedConst(value: CValue): Slice = {
    val size = source.size
    val columns = {
      Map(
        value match {
          case CString(s) =>
            (ColumnRef(CPath.Identity, CString), new StrColumn {
              def isDefinedAt(row: Int) = source.isDefinedAt(row)
              def apply(row: Int)       = s
            })
          case CBoolean(b) =>
            (ColumnRef(CPath.Identity, CBoolean), new BoolColumn {
              def isDefinedAt(row: Int) = source.isDefinedAt(row)
              def apply(row: Int)       = b
            })
          case CLong(l) =>
            (ColumnRef(CPath.Identity, CLong), new LongColumn {
              def isDefinedAt(row: Int) = source.isDefinedAt(row)
              def apply(row: Int)       = l
            })
          case CDouble(d) =>
            (ColumnRef(CPath.Identity, CDouble), new DoubleColumn {
              def isDefinedAt(row: Int) = source.isDefinedAt(row)
              def apply(row: Int)       = d
            })
          case CNum(n) =>
            (ColumnRef(CPath.Identity, CNum), new NumColumn {
              def isDefinedAt(row: Int) = source.isDefinedAt(row)
              def apply(row: Int)       = n
            })
          case COffsetDateTime(d) =>
            (ColumnRef(CPath.Identity, COffsetDateTime), new OffsetDateTimeColumn {
              def isDefinedAt(row: Int) = source.isDefinedAt(row)
              def apply(row: Int)       = d
            })
          case COffsetTime(d) =>
            (ColumnRef(CPath.Identity, COffsetTime), new OffsetTimeColumn {
              def isDefinedAt(row: Int) = source.isDefinedAt(row)
              def apply(row: Int)       = d
            })
          case COffsetDate(d) =>
            (ColumnRef(CPath.Identity, COffsetDate), new OffsetDateColumn {
              def isDefinedAt(row: Int) = source.isDefinedAt(row)
              def apply(row: Int)       = d
            })
          case CLocalDateTime(d) =>
            (ColumnRef(CPath.Identity, CLocalDateTime), new LocalDateTimeColumn {
              def isDefinedAt(row: Int) = source.isDefinedAt(row)
              def apply(row: Int)       = d
            })
          case CLocalTime(d) =>
            (ColumnRef(CPath.Identity, CLocalTime), new LocalTimeColumn {
              def isDefinedAt(row: Int) = source.isDefinedAt(row)
              def apply(row: Int)       = d
            })
          case CLocalDate(d) =>
            (ColumnRef(CPath.Identity, CLocalDate), new LocalDateColumn {
              def isDefinedAt(row: Int) = source.isDefinedAt(row)
              def apply(row: Int)       = d
            })
          case CInterval(p) =>
            (ColumnRef(CPath.Identity, CInterval), new IntervalColumn {
              def isDefinedAt(row: Int) = source.isDefinedAt(row)
              def apply(row: Int)       = p
            })
          case value: CArray[a] =>
            (ColumnRef(CPath.Identity, value.cType), new HomogeneousArrayColumn[a] {
              val tpe = value.cType
              def isDefinedAt(row: Int) = source.isDefinedAt(row)
              def apply(row: Int)       = value.value
            })
          case CNull =>
            (ColumnRef(CPath.Identity, CNull), new NullColumn {
              def isDefinedAt(row: Int) = source.isDefinedAt(row)
            })
          case CEmptyObject =>
            (ColumnRef(CPath.Identity, CEmptyObject), new EmptyObjectColumn {
              def isDefinedAt(row: Int) = source.isDefinedAt(row)
            })
          case CEmptyArray =>
            (ColumnRef(CPath.Identity, CEmptyArray), new EmptyArrayColumn {
              def isDefinedAt(row: Int) = source.isDefinedAt(row)
            })
          case CUndefined => sys.error("Cannot define a constant undefined value")
        }
      )
    }
    Slice(size, columns)
  }

  def deref(node: CPathNode): Slice = {
    val size = source.size
    val columns = node match {
      case CPathIndex(i) =>
        source.columns collect {
          case (ColumnRef(CPath(CPathArray, xs @ _ *), CArrayType(elemType)), col: HomogeneousArrayColumn[_]) =>
            (ColumnRef(CPath(xs: _*), elemType), col.select(i))

          case (ColumnRef(CPath(CPathIndex(`i`), xs @ _ *), ctype), col) =>
            (ColumnRef(CPath(xs: _*), ctype), col)
        }

      case _ =>
        source.columns collect {
          case (ColumnRef(CPath(`node`, xs @ _ *), ctype), col) =>
            (ColumnRef(CPath(xs: _*), ctype), col)
        }
    }
    Slice(size, columns)
  }

  def wrap(wrapper: CPathNode): Slice = {
    val size = source.size

    // This is a little weird; CPathArray actually wraps in CPathIndex(0).
    // Unfortunately, CArrayType(_) cannot wrap CNullTypes, so we can't just
    // arbitrarily wrap everything in a CPathArray.

    val columns = wrapper match {
      case CPathArray =>
        source.columns map {
          case (ColumnRef(CPath(nodes @ _ *), ctype), col) =>
            (ColumnRef(CPath(CPathIndex(0) +: nodes: _*), ctype), col)
        }
      case _ =>
        source.columns map {
          case (ColumnRef(CPath(nodes @ _ *), ctype), col) =>
            (ColumnRef(CPath(wrapper +: nodes: _*), ctype), col)
        }
    }
    Slice(size, columns)
  }

  // ARRAYS:
  // TODO Here, if we delete a JPathIndex/JArrayFixedT, then we need to
  // construct a new Homo*ArrayColumn that has some indices missing.
  //
  // -- I've added a col.without(indicies) method to H*ArrayColumn to support
  // this operation.
  //
  def delete(jtype: JType): Slice = {
    def fixArrays(columns: Map[ColumnRef, Column]): Map[ColumnRef, Column] = {
      columns.toSeq
        .sortBy(_._1)
        .foldLeft((Map.empty[Vector[CPathNode], Int], Map.empty[ColumnRef, Column])) {
          case ((arrayPaths, acc), (ColumnRef(jpath, ctype), col)) =>
            val (arrayPaths0, nodes) = jpath.nodes.foldLeft((arrayPaths, Vector.empty[CPathNode])) {
              case ((ap, nodes), CPathIndex(_)) =>
                val idx = ap.getOrElse(nodes, -1) + 1
                (ap + (nodes -> idx), nodes :+ CPathIndex(idx))

              case ((ap, nodes), fieldNode) => (ap, nodes :+ fieldNode)
            }

            (arrayPaths0, acc + (ColumnRef(CPath(nodes: _*), ctype) -> col))
        }
        ._2
    }

    // Used for homogeneous arrays. Constructs a function, suitable for use in a
    // flatMap, that will modify the homogeneous array according to `jType`.
    //
    def flattenDeleteTree[A](jType: JType, cType: CValueType[A], cPath: CPath): A => Option[A] = {
      val delete: A => Option[A] = _ => None
      val retain: A => Option[A] = Some(_)

      (jType, cType, cPath) match {
        case (JUnionT(aJType, bJType), _, _) =>
          flattenDeleteTree(aJType, cType, cPath) andThen (_ flatMap flattenDeleteTree(bJType, cType, cPath))
        case (JTextT, CString, CPath.Identity) =>
          delete
        case (JBooleanT, CBoolean, CPath.Identity) =>
          delete
        case (JNumberT, CLong | CDouble | CNum, CPath.Identity) =>
          delete
        case (JObjectUnfixedT, _, CPath(CPathField(_), _ *)) =>
          delete
        case (JObjectFixedT(fields), _, CPath(CPathField(name), cPath @ _ *)) =>
          fields get name map (flattenDeleteTree(_, cType, CPath(cPath: _*))) getOrElse (retain)
        case (JArrayUnfixedT, _, CPath(CPathArray | CPathIndex(_), _ *)) =>
          delete
        case (JArrayFixedT(elems), cType, CPath(CPathIndex(i), cPath @ _ *)) =>
          elems get i map (flattenDeleteTree(_, cType, CPath(cPath: _*))) getOrElse (retain)
        case (JArrayFixedT(elems), CArrayType(cElemType), CPath(CPathArray, cPath @ _ *)) =>
          val mappers = elems mapValues (flattenDeleteTree(_, cElemType, CPath(cPath: _*)))
          xs =>
            Some(xs.zipWithIndex map {
              case (x, j) =>
                mappers get j match {
                  case Some(f) => f(x)
                  case None => x
                }
            })
          case (JArrayHomogeneousT(jType), CArrayType(cType), CPath(CPathArray, _ *)) if Schema.ctypes(jType)(cType) =>
          delete
        case _ =>
          retain
      }
    }

    val size = source.size
    val columns = fixArrays(source.columns flatMap {
      case (ColumnRef(cpath, ctype), _) if Schema.includes(jtype, cpath, ctype) =>
        None

      case (ref @ ColumnRef(cpath, ctype: CArrayType[a]), col: HomogeneousArrayColumn[_]) if ctype == col.tpe =>
        val trans = flattenDeleteTree(jtype, ctype, cpath)
        Some((ref, new HomogeneousArrayColumn[a] {
          val tpe = ctype
          def isDefinedAt(row: Int)     = col.isDefinedAt(row)
          def apply(row: Int): Array[a] = trans(col(row).asInstanceOf[Array[a]]) getOrElse sys.error("Oh dear, this cannot be happening to me.")
        }))

      case (ref, col) =>
        Some((ref, col))
    })

    Slice (size, columns)
  }

  def deleteFields(prefixes: scala.collection.Set[CPathField]) = {
    val (removed, withoutPrefixes) = source.columns partition {
      case (ColumnRef(CPath(head @ CPathField(_), _ @_ *), _), _) => prefixes contains head
      case _ => false
    }

    val becomeEmpty = BitSetUtil.filteredRange(0, source.size) { i =>
      Column.isDefinedAt(removed.values.toArray, i) && !Column.isDefinedAt(withoutPrefixes.values.toArray, i)
    }

    val ref = ColumnRef(CPath.Identity, CEmptyObject)

    // The object might have become empty. Make the
    // EmptyObjectColumn defined at the row position.
    lazy val emptyObjectColumn = withoutPrefixes get ref map { c =>
      new EmptyObjectColumn {
        def isDefinedAt(row: Int) = c.isDefinedAt(row) || becomeEmpty(row)
      }
    } getOrElse {
      new EmptyObjectColumn {
        def isDefinedAt(row: Int) = becomeEmpty(row)
      }
    }

    val size = source.size
    val columns =
      if (becomeEmpty.isEmpty)
        withoutPrefixes
      else
        withoutPrefixes + (ref -> emptyObjectColumn)

    Slice(size, columns)
  }

  def typed(jtpe: JType): Slice = Slice(
    source.size,
    source.columns filter { case (ColumnRef(path, ctpe), _) => Schema.requiredBy(jtpe, path, ctpe) })

  def typedSubsumes(jtpe: JType): Slice = {
    val tuples: Seq[(CPath, CType)] = source.columns.map({ case (ColumnRef(path, ctpe), _) => (path, ctpe) })(collection.breakOut)
    val columns = if (Schema.subsumes(tuples, jtpe)) {
      source.columns filter { case (ColumnRef(path, ctpe), _) => Schema.requiredBy(jtpe, path, ctpe) }
    } else {
      Map.empty[ColumnRef, Column]
    }

    Slice(source.size, columns)
  }

  /**
    * returns a BoolColumn that is true if row subsumes jtype, false otherwise (unless undefined)
    * determine if the supplied jtype subsumes all the columns
    * if false, return a BoolColumn with all falses, defined by union
    * if true, collect just those columns that the jtype specifies
    * then on a row-by-row basis, using a BitSet, we use `Schema.findTypes(...)` to determine the Boolean values
    */
  def isType(jtpe: JType): Slice = {
    val size                               = source.size
    val pathsAndTypes: Seq[(CPath, CType)] = source.columns.toSeq map { case (ColumnRef(selector, ctype), _) => (selector, ctype) }

    // we cannot just use subsumes because there could be rows with undefineds in them
    val subsumes = Schema.subsumes(pathsAndTypes, jtpe)

    val definedBits = (source.columns).values.map(_.definedAt(0, size)).reduceOption(_ | _) getOrElse new BitSet

    val columns = if (subsumes) {
      val cols = source.columns filter { case (ColumnRef(path, ctpe), _) => Schema.requiredBy(jtpe, path, ctpe) }

      val included     = Schema.findTypes(jtpe, CPath.Identity, cols, size)
      val includedBits = BitSetUtil.filteredRange(0, size)(included)

      Map(ColumnRef(CPath.Identity, CBoolean) -> BoolColumn.Either(definedBits, includedBits))
    } else {
      Map(ColumnRef(CPath.Identity, CBoolean) -> BoolColumn.False(definedBits))
    }

    Slice (size, columns)
  }

  def toNumber = {
    val size = source.size
    val columns = source.columns.get(ColumnRef(CPath.Identity, CString)) match {
      case Some(c: StrColumn) =>
        val lc = ArrayLongColumn.empty(size)
        val dc = ArrayDoubleColumn.empty(size)
        val nc = ArrayNumColumn.empty(size)

        RangeUtil.loopDefined(0 to size, c){ i =>
          val s = c(i)
          try {
            val l = java.lang.Long.parseLong(s)
            lc.update(i, l)
          } catch {
            case _: NumberFormatException =>
              try {
                val n = BigDecimal(s)
                if (n.isDecimalDouble) {
                  dc.update(i, n.doubleValue)
                } else {
                  nc.update(i, n)
                }
              } catch {
                case _: NumberFormatException => // don't set anything
              }
          }
        }
        Map[ColumnRef, BitsetColumn](
          ColumnRef(CPath.Identity, CLong) -> lc,
          ColumnRef(CPath.Identity, CDouble) -> dc,
          ColumnRef(CPath.Identity, CNum) -> nc).foldLeft(Map.empty[ColumnRef, Column])
        { case (acc, (cref, col)) =>
            if (col.definedAt.size > 0) acc + (cref -> col.asInstanceOf[Column])
            else acc }
      case _ => Map.empty[ColumnRef, Column]
    }
    Slice(size, columns)
  }

  def arraySwap(index: Int) = {
    val size = source.size
    val columns = source.columns.collect {
      case (ColumnRef(cPath @ CPath(CPathArray, _ *), cType), col: HomogeneousArrayColumn[a]) =>
        (ColumnRef(cPath, cType), new HomogeneousArrayColumn[a] {
          val tpe = col.tpe
          def isDefinedAt(row: Int) = col.isDefinedAt(row)
          def apply(row: Int) = {
            val xs = col(row)
            if (index >= xs.length) xs
            else {
              val ys = tpe.elemType.classTag.newArray(xs.length)

              var i = 1
              while (i < ys.length) {
                ys(i) = xs(i)
                i += 1
              }

              ys(0) = xs(index)
              ys(index) = xs(0)
              ys
            }
          }
        })

      case (ColumnRef(CPath(CPathIndex(0), xs @ _ *), ctype), col) =>
        (ColumnRef(CPath(CPathIndex(index) +: xs: _*), ctype), col)

      case (ColumnRef(CPath(CPathIndex(`index`), xs @ _ *), ctype), col) =>
        (ColumnRef(CPath(CPathIndex(0) +: xs: _*), ctype), col)

      case c @ (ColumnRef(CPath(CPathIndex(i), xs @ _ *), ctype), col) => c
    }

    Slice(size, columns)
  }

  // Takes an array where the indices correspond to indices in this slice,
  // and the values give the indices in the sparsened slice.
  def sparsen(index: Array[Int], toSize: Int): Slice = Slice(
    toSize,
    source.columns lazyMapValues { col =>
      cf.util.Sparsen(index, toSize)(col).get //sparsen is total
    })


  def remap(indices: ArrayIntList) = Slice(
    indices.size,
    source.columns lazyMapValues { col =>
      cf.util.RemapIndices(indices).apply(col).get
    })

  def map(from: CPath, to: CPath)(f: CF1): Slice = {
    val size = source.size

    val columns: Map[ColumnRef, Column] = {
      val resultColumns = for {
        col <- source.columns collect { case (ref, col) if ref.selector.hasPrefix(from) => col }
        result <- f(col)
      } yield result

      resultColumns.groupBy(_.tpe) map {
        case (tpe, cols) => (ColumnRef(to, tpe), cols.reduceLeft((c1, c2) => Column.unionRightSemigroup.append(c1, c2)))
      }
    }

    Slice(size, columns)
  }

  def map2(froml: CPath, fromr: CPath, to: CPath)(f: CF2): Slice = {
    val size = source.size

    val columns: Map[ColumnRef, Column] = {
      val resultColumns = for {
        left <- source.columns collect { case (ref, col) if ref.selector.hasPrefix(froml) => col }
        right <- source.columns collect { case (ref, col) if ref.selector.hasPrefix(fromr) => col }
        result <- f(left, right)
      } yield result

      resultColumns.groupBy(_.tpe) map { case (tpe, cols) => (ColumnRef(to, tpe), cols.reduceLeft((c1, c2) => Column.unionRightSemigroup.append(c1, c2))) }
    }

    Slice(size, columns)
  }

  def filterDefined(filter: Slice, definedness: Definedness) = {
    val colValues = filter.columns.values.toArray
    lazy val defined = definedness match {
      case AnyDefined =>
        BitSetUtil.filteredRange(0, source.size) { i =>
          colValues.exists(_.isDefinedAt(i))
        }

      case AllDefined =>
        if (colValues.isEmpty)
          new BitSet
        else
          BitSetUtil.filteredRange(0, source.size) { i =>
            colValues.forall(_.isDefinedAt(i))
          }
    }

    val size = source.size
    val columns: Map[ColumnRef, Column] = source.columns lazyMapValues { col =>
      cf.util.filter(0, source.size, defined)(col).get
    }

    Slice(size, columns)
  }

  def ifUndefined(default: Slice): Slice = {
    val defined = {
      val bsing = source.columns.values map {
        case col: BitsetColumn => col.definedAt.copy
        case col => col.definedAt(0, size)
      }

      val back = bsing.reduceOption(_ | _).getOrElse(new BitSet)
      back.flip(0, size)
      back
    }

    val masked = default.columns lazyMapValues { col =>
      cf.util.filter(0, size, defined)(col).get
    }

    val merged = masked.foldLeft(source.columns) {
      case (acc, (ref, col)) if acc.contains(ref) =>
        acc.updated(ref, cf.util.UnionRight(col, acc(ref)).get)

      case (acc, pair) => acc + pair
    }

    Slice(source.size, merged)
  }

  def compact(filter: Slice, definedness: Definedness): Slice = {
    val cols = filter.columns

    lazy val retained = definedness match {
      case AnyDefined => {
        val acc = new ArrayIntList
        Loop.range(0, filter.size) { i =>
          if (cols.values.toArray.exists(_.isDefinedAt(i))) acc.add(i)
        }
        acc
      }

      case AllDefined => {
        val acc = new ArrayIntList
        val (numCols, otherCols) = cols partition {
          case (ColumnRef(_, ctype), _) =>
            ctype.isNumeric
        }

        val grouped = numCols groupBy { case (ColumnRef(cpath, _), _) => cpath }

        Loop.range(0, filter.size) { i =>
          val numBools = grouped.values map {
            case refs =>
              refs.values.toArray.exists(_.isDefinedAt(i))
          }

          val numBool   = numBools reduce { _ && _ }
          val otherBool = otherCols.values.toArray.forall(_.isDefinedAt(i))

          if (otherBool && numBool) acc.add(i)
        }
        acc
      }
    }

    lazy val size = retained.size
    lazy val columns: Map[ColumnRef, Column] = source.columns lazyMapValues { col =>
      (col |> cf.util.RemapIndices(retained)).get
    }

    Slice(size, columns)
  }

  def retain(refs: Set[ColumnRef]) =
    Slice(source.size, source.columns.filterKeys(refs))

  /**
    * Assumes that this and the previous slice (if any) are sorted.
    */
  def distinct(prevFilter: Option[Slice], filter: Slice): Slice = {
    lazy val retained: ArrayIntList = {
      val acc = new ArrayIntList

      def findSelfDistinct(prevRow: Int, curRow: Int) = {
        val selfComparator = rowComparatorFor(filter, filter)(_.columns.keys map (_.selector))

        @tailrec
        def findSelfDistinct0(prevRow: Int, curRow: Int): ArrayIntList = {
          if (curRow >= filter.size) acc
          else {
            val retain = selfComparator.compare(prevRow, curRow) != EQ
            if (retain) acc.add(curRow)
            findSelfDistinct0(if (retain) curRow else prevRow, curRow + 1)
          }
        }

        findSelfDistinct0(prevRow, curRow)
      }

      def findStraddlingDistinct(prev: Slice, prevRow: Int, curRow: Int) = {
        val straddleComparator = rowComparatorFor(prev, filter)(_.columns.keys map (_.selector))

        @tailrec
        def findStraddlingDistinct0(prevRow: Int, curRow: Int): ArrayIntList = {
          if (curRow >= filter.size) acc
          else {
            val retain = straddleComparator.compare(prevRow, curRow) != EQ
            if (retain) acc.add(curRow)
            if (retain)
              findSelfDistinct(curRow, curRow + 1)
            else
              findStraddlingDistinct0(prevRow, curRow + 1)
          }
        }

        findStraddlingDistinct0(prevRow, curRow)
      }

      val lastDefined = prevFilter.flatMap { slice =>
        (slice.size - 1 to 0 by -1).find(row => slice.columns.values.exists(_.isDefinedAt(row)))
      }.map {
        (prevFilter.get, _)
      }

      val firstDefined = (0 until filter.size).find(i => filter.columns.values.exists(_.isDefinedAt(i)))

      (lastDefined, firstDefined) match {
        case (Some((prev, i)), Some(j)) => findStraddlingDistinct(prev, i, j)
        case (_, Some(j)) => acc.add(j); findSelfDistinct(j, j + 1)
        case _ => acc
      }
    }

    lazy val size = retained.size
    lazy val columns: Map[ColumnRef, Column] = source.columns lazyMapValues { col =>
      (col |> cf.util.RemapIndices(retained)).get
    }
    Slice(size, columns)
  }

  def order: spire.algebra.Order[Int] =
    if (columns.size == 1) {
      val col = columns.head._2
      Column.rowOrder(col)
    } else {

      // The 2 cases are handled differently. In the first case, we don't have
      // any pesky homogeneous arrays and only 1 column per path. In this case,
      // we don't need to use the CPathTraversal machinery.

      type GroupedCols = Either[Map[CPath, Column], Map[CPath, Set[Column]]]

      val grouped = columns.foldLeft(Left(Map.empty): GroupedCols) {
        case (Left(acc), (ColumnRef(path, CArrayType(_)), col)) =>
          val acc0 = acc.map { case (k, v) => (k, Set(v)) }
          Right(acc0 + (path -> Set(col)))

        case (Left(acc), (ColumnRef(path, _), col)) =>
          acc get path map { col0 =>
            val acc0 = acc.map { case (k, v) => (k, Set(v)) }
            Right(acc0 + (path         -> Set(col0, col)))
          } getOrElse Left(acc + (path -> col))

        case (Right(acc), (ColumnRef(path, _), col)) =>
          Right(acc + (path -> (acc.getOrElse(path, Set.empty[Column]) + col)))
      }

      grouped match {
        case Left(cols0) =>
          val cols = cols0.toList
            .sortBy(_._1)
            .map {
              case (_, col) =>
                Column.rowOrder(col)
            }
            .toArray

          new spire.algebra.Order[Int] {
            def compare(i: Int, j: Int): Int = {
              var k = 0
              while (k < cols.length) {
                val cmp = cols(k).compare(i, j)
                if (cmp != 0)
                  return cmp
                k += 1
              }
              0
            }
          }

        case Right(cols) =>
          val paths     = cols.keys.toList
          val traversal = CPathTraversal(paths)
          traversal.rowOrder(paths, cols)
      }
    }

  def sortWith(keySlice: Slice, sortOrder: DesiredSortOrder = SortAscending): (Slice, Slice) = {

    // We filter out rows that are completely undefined.
    val order: Array[Int] = Array.range(0, source.size) filter { row =>
      keySlice.isDefinedAt(row) && source.isDefinedAt(row)
    }
    val rowOrder = if (sortOrder == SortAscending) keySlice.order else cats.kernel.Order.reverse(keySlice.order)
    spire.math.MergeSort.sort(order)(rowOrder, implicitly)

    val remapOrder = new ArrayIntList(order.size)
    var i = 0
    while (i < order.length) {
      remapOrder.add(i, order(i))
      i += 1
    }

    val sortedSlice    = source.remap(remapOrder)
    val sortedKeySlice = keySlice.remap(remapOrder)

    // TODO Remove the duplicate distinct call. Should be able to handle this in 1 pass.
    (sortedSlice.distinct(None, sortedKeySlice), sortedKeySlice.distinct(None, sortedKeySlice))
  }

  def sortBy(prefixes: Vector[CPath], sortOrder: DesiredSortOrder = SortAscending): Slice = {
    // TODO This is slow... Faster would require a prefix map or something... argh.
    val keySlice = {
      val size = source.size
      val columns: Map[ColumnRef, Column] = {
        prefixes.zipWithIndex.flatMap({
          case (prefix, i) =>
            source.columns collect {
              case (ColumnRef(path, tpe), col) if path hasPrefix prefix =>
                (ColumnRef(CPathIndex(i) \ path, tpe), col)
            }
        })(collection.breakOut)
      }
      Slice(size, columns)
    }

    source.sortWith(keySlice)._1
  }

  /**
    * Split the table at the specified index, exclusive. The
    * new prefix will contain all indices less than that index, and
    * the new suffix will contain indices >= that index.
    */
  def split(idx: Int): (Slice, Slice) = {
    (take(idx), drop(idx))
  }

  def take(sz: Int): Slice = {
    if (sz >= source.size) {
      source
    } else {
      val size = sz
      val columns = source.columns lazyMapValues { col =>
        (col |> cf.util.RemapFilter(_ < sz, 0)).get
      }
      Slice(size, columns)
    }
  }

  def drop(sz: Int): Slice = {
    if (sz <= 0) {
      source
    } else {
      val size = source.size - sz
      val columns = source.columns lazyMapValues { col =>
        (col |> cf.util.RemapFilter(_ < size, sz)).get
      }
      Slice(size, columns)
    }
  }

  def takeRange(startIndex: Int, numberToTake: Int): Slice = {
    val take2 = math.min(this.size, startIndex + numberToTake) - startIndex

    val size = take2
    val columns = source.columns lazyMapValues { col =>
      (col |> cf.util.RemapFilter(_ < take2, startIndex)).get
    }
    Slice(size, columns)
  }

  def zip(other: Slice): Slice = {
    val size = source.size min other.size
    val columns: Map[ColumnRef, Column] = other.columns.foldLeft(source.columns) {
      case (acc, (ref, col)) =>
        acc + (ref -> (acc get ref flatMap { c =>
                  cf.util.UnionRight(c, col)
                } getOrElse col))
    }
    Slice(size, columns)
  }

  /**
    * This creates a new slice with the same size and columns as this slice, but
    * whose values have been materialized and stored in arrays.
    */
  def materialized: Slice = {
    val size = source.size
    val columns = source.columns lazyMapValues {
      case col: BoolColumn =>
        val defined = col.definedAt(0, source.size)
        val values = BitSetUtil.filteredRange(0, source.size) { row =>
          defined(row) && col(row)
        }
        ArrayBoolColumn(defined, values)

      case col: LongColumn =>
        val defined = col.definedAt(0, source.size)
        val values  = new Array[Long](source.size)
        Loop.range(0, source.size) { row =>
          if (defined(row)) values(row) = col(row)
        }
        ArrayLongColumn(defined, values)

      case col: DoubleColumn =>
        val defined = col.definedAt(0, source.size)
        val values  = new Array[Double](source.size)
        Loop.range(0, source.size) { row =>
          if (defined(row)) values(row) = col(row)
        }
        ArrayDoubleColumn(defined, values)

      case col: NumColumn =>
        val defined = col.definedAt(0, source.size)
        val values  = new Array[BigDecimal](source.size)
        Loop.range(0, source.size) { row =>
          if (defined(row)) values(row) = col(row)
        }
        ArrayNumColumn(defined, values)

      case col: StrColumn =>
        val defined = col.definedAt(0, source.size)
        val values  = new Array[String](source.size)
        Loop.range(0, source.size) { row =>
          if (defined(row)) values(row) = col(row)
        }
        ArrayStrColumn(defined, values)

      case col: OffsetDateTimeColumn =>
        val defined = col.definedAt(0, source.size)
        val values  = new Array[OffsetDateTime](source.size)
        Loop.range(0, source.size) { row =>
          if (defined(row)) values(row) = col(row)
        }
        ArrayOffsetDateTimeColumn(defined, values)

      case col: OffsetTimeColumn =>
        val defined = col.definedAt(0, source.size)
        val values  = new Array[OffsetTime](source.size)
        Loop.range(0, source.size) { row =>
          if (defined(row)) values(row) = col(row)
        }
        ArrayOffsetTimeColumn(defined, values)

      case col: OffsetDateColumn =>
        val defined = col.definedAt(0, source.size)
        val values  = new Array[OffsetDate](source.size)
        Loop.range(0, source.size) { row =>
          if (defined(row)) values(row) = col(row)
        }
        ArrayOffsetDateColumn(defined, values)

      case col: LocalDateTimeColumn =>
        val defined = col.definedAt(0, source.size)
        val values  = new Array[LocalDateTime](source.size)
        Loop.range(0, source.size) { row =>
          if (defined(row)) values(row) = col(row)
        }
        ArrayLocalDateTimeColumn(defined, values)

      case col: LocalTimeColumn =>
        val defined = col.definedAt(0, source.size)
        val values  = new Array[LocalTime](source.size)
        Loop.range(0, source.size) { row =>
          if (defined(row)) values(row) = col(row)
        }
        ArrayLocalTimeColumn(defined, values)

      case col: LocalDateColumn =>
        val defined = col.definedAt(0, source.size)
        val values  = new Array[LocalDate](source.size)
        Loop.range(0, source.size) { row =>
          if (defined(row)) values(row) = col(row)
        }
        ArrayLocalDateColumn(defined, values)

      case col: IntervalColumn =>
        val defined = col.definedAt(0, source.size)
        val values  = new Array[DateTimeInterval](source.size)
        Loop.range(0, source.size) { row =>
          if (defined(row)) values(row) = col(row)
        }
        ArrayIntervalColumn(defined, values)

      case col: EmptyArrayColumn =>
        val ncol = MutableEmptyArrayColumn.empty()
        Loop.range(0, source.size) { row =>
          ncol.update(row, col.isDefinedAt(row))
        }
        ncol

      case col: EmptyObjectColumn =>
        val ncol = MutableEmptyObjectColumn.empty()
        Loop.range(0, source.size) { row =>
          ncol.update(row, col.isDefinedAt(row))
        }
        ncol

      case col: NullColumn =>
        val ncol = MutableNullColumn.empty()
        Loop.range(0, source.size) { row =>
          ncol.update(row, col.isDefinedAt(row))
        }
        ncol

      case col =>
        sys.error("Cannot materialise non-standard (extensible) column")
    }
    Slice(size, columns)
  }

  // will render a trailing newline
  // non-singleton inner lists represent type conflicts at a single CPath
  def renderCsv(
      headers: List[List[ColumnRef]],
      assumeHomogeneous: Boolean): Seq[CharBuffer] = {

    // faster caches
    val size = this.size

    // the goal here is to fill in empty values wherever we get an unknown column from a previous slice
    val DummyColumn = new NullColumn {
      def isDefinedAt(row: Int) = false
    }

    /*
     * In the arrays below, use the scalar case of the union if
     * assumeHomogeneous = true, since we're assuming that we can't
     * have path-locus conflicts (i.e. multiple column types at the
     * same path). This is a significant fastpath.
     */

    // CType | Array[CType]
    val ctypes: Array[AnyRef] = headers map { refs =>
      if (assumeHomogeneous)
        refs.head.ctype
      else
        refs.filter(this.columns.contains).map(_.ctype).toArray
    } toArray

    // Column | Array[Column]
    val columns: Array[AnyRef] = headers map { refs =>
      if (assumeHomogeneous)
        this.columns.getOrElse(refs.head, DummyColumn)
      else
        refs.flatMap(this.columns.get(_)).toArray
    } toArray

    def isDefinedAt(row: Int): Boolean = {
      var back = false
      var i = 0
      while (i < columns.length) {
        if (assumeHomogeneous) {
          back ||= columns(i).asInstanceOf[Column].isDefinedAt(row)
        } else {
          val candidates = columns(i).asInstanceOf[Array[Column]]
          var j = 0
          while (j < candidates.length) {
            back ||= candidates(j).isDefinedAt(row)

            if (back) {
              return true
            }
            j += 1
          }
        }

        if (back) {
          return true
        }
        i += 1
      }
      back
    }

    val ctx = BufferContext.csv(size) { (ctx, str) =>
      // fast-path the unescaped case
      if (str.indexOf('"') < 0 &&
          str.indexOf('\n') < 0 &&
          str.indexOf('\r') < 0 &&
          str.indexOf(',') < 0) {

        ctx.pushStr(str)
      } else {
        ctx.push('"')

        var i = 0
        while (i < str.length) {
          // pretty sure we don't need anything more elaborate than this?
          (str.charAt(i): @switch) match {
            case '"' => ctx.pushStr("\"\"")
            case c => ctx.push(c)
          }

          i += 1
        }

        ctx.push('"')
      }
    }

    @tailrec
    def renderColumns(row: Int, col: Int): Unit = {
      if (col < columns.length) {
        // we use assumeHomogeneous to determine the case in the union

        val candidates = if (!assumeHomogeneous)
          columns(col).asInstanceOf[Array[Column]]
        else
          null

        val column = if (assumeHomogeneous)
          columns(col).asInstanceOf[Column]
        else
          null

        def renderColumn(column: Column, tpe: CType): Unit = {
          tpe match {
            // short-circuit csv string escaping
            case CString if column.isInstanceOf[NoCsvEscape] =>
              val c = column.asInstanceOf[StrColumn]
              ctx.pushStr(c(row))

            case CString =>
              val c = column.asInstanceOf[StrColumn]
              ctx.renderString(c(row))

            case CBoolean =>
              val c = column.asInstanceOf[BoolColumn]
              ctx.renderBoolean(c(row))

            case CLong =>
              val c = column.asInstanceOf[LongColumn]
              ctx.renderLong(c(row))

            case CDouble =>
              val c = column.asInstanceOf[DoubleColumn]
              ctx.renderDouble(c(row))

            case CNum =>
              val c = column.asInstanceOf[NumColumn]
              ctx.renderNum(c(row))

            case CNull =>
              ctx.renderNull()

            case CEmptyObject =>
              ctx.renderEmptyObject()

            case CEmptyArray =>
              ctx.renderEmptyArray()

            case COffsetDateTime =>
              val c = column.asInstanceOf[OffsetDateTimeColumn]
              ctx.renderOffsetDateTime(c(row))

            case COffsetTime =>
              val c = column.asInstanceOf[OffsetTimeColumn]
              ctx.renderOffsetTime(c(row))

            case COffsetDate =>
              val c = column.asInstanceOf[OffsetDateColumn]
              ctx.renderOffsetDate(c(row))

            case CLocalDateTime =>
              val c = column.asInstanceOf[LocalDateTimeColumn]
              ctx.renderLocalDateTime(c(row))

            case CLocalTime =>
              val c = column.asInstanceOf[LocalTimeColumn]
              ctx.renderLocalTime(c(row))

            case CLocalDate =>
              val c = column.asInstanceOf[LocalDateColumn]
              ctx.renderLocalDate(c(row))

            case CInterval =>
              val c = column.asInstanceOf[IntervalColumn]
              ctx.renderInterval(c(row))

            case CArrayType(_) => errorImpossible

            case CUndefined =>
          }
        }

        if (assumeHomogeneous) {
          // this recomputes definedness :-(
          if (column.isDefinedAt(row)) {
            renderColumn(column, ctypes(col).asInstanceOf[CType])
          }
        } else {
          val hetTypes = ctypes(col).asInstanceOf[Array[CType]]

          @inline
          @tailrec
          def loop(i: Int): Unit = {
            if (i < candidates.length) {
              // this recomputes definedness :-(
              if (candidates(i).isDefinedAt(row))
                renderColumn(candidates(i), hetTypes(i))
              else
                loop(i + 1)
            }
          }

          loop(0)
        }

        if (col < columns.length - 1) {
          ctx.push(',')
        }

        renderColumns(row, col + 1)
      }
    }

    @inline
    @tailrec
    def render(row: Int): Unit = {
      if (row < size) {
        if (isDefinedAt(row)) {   // TODO do we want to just force a compact?
          renderColumns(row, 0)
          ctx.pushStr("\r\n")
        }

        render(row + 1)
      }
    }

    render(0)

    ctx.finish()
    ctx.toSeq()
  }

  def renderJson(delimiter: String, precise: Boolean): (Seq[CharBuffer], Boolean) = {
    sliceSchema.map(transformSchemaPrecise(precise)) match {
      case Some(schema) =>
        val depth = {
          def loop(schema: SchemaNode): Int = schema match {
            case obj: SchemaNode.Obj =>
              4 + (obj.values map loop max)

            case arr: SchemaNode.Arr =>
              2 + (arr.nodes map loop max)

            case union: SchemaNode.Union =>
              union.possibilities map loop max

            case SchemaNode.Leaf(_, _) => 0
          }

          loop(schema)
        }

        val ctx = BufferContext.json(size) { (ctx, str) =>
          @inline
          @tailrec
          def loop(str: String, idx: Int): Unit = {
            if (idx == 0) {
              ctx.push('"')
            }

            if (idx < str.length) {
              val c = str.charAt(idx)

              (c: @switch) match {
                case '"' => ctx.pushStr("\\\"")
                case '\\' => ctx.pushStr("\\\\")
                case '\b' => ctx.pushStr("\\b")
                case '\f' => ctx.pushStr("\\f")
                case '\n' => ctx.pushStr("\\n")
                case '\r' => ctx.pushStr("\\r")
                case '\t' => ctx.pushStr("\\t")

                case c =>
                  if ((c >= '\u0000' && c < '\u001f') || (c >= '\u0080' && c < '\u00a0') || (c >= '\u2000' && c < '\u2100')) {
                    ctx.pushStr("\\u")
                    ctx.pushStr("%04x".format(Character.codePointAt(str, idx)))
                  } else {
                    ctx.push(c)
                  }
              }

              loop(str, idx + 1)
            } else {
              ctx.push('"')
            }
          }

          loop(str, 0)
        }

        val in = new RingDeque[String](depth + 1)
        val inFlags = new RingDeque[Boolean](depth + 1)

        @inline
        def pushIn(str: String, flag: Boolean): Unit = {
          in.pushBack(str)
          inFlags.pushBack(flag)
        }

        @inline
        def popIn(): Unit = {
          in.popBack()
          inFlags.popBack()
        }

        @inline
        @tailrec
        def flushIn(): Unit = {
          if (!in.isEmpty) {
            val str = in.popFront()
            val flag = inFlags.popFront()

            if (flag)
              ctx.renderString(str)
            else
              ctx.pushStr(str)

            flushIn()
          }
        }

        def traverseSchema(row: Int, schema: SchemaNode): Boolean = schema match {
          case obj: SchemaNode.Obj =>
            val keys   = obj.keys
            val values = obj.values

            @inline
            @tailrec
            def loop(idx: Int, done: Boolean): Boolean = {
              if (idx < keys.length) {
                val key   = keys(idx)
                val value = values(idx)

                if (done) {
                  pushIn(",", false)
                }

                pushIn(key, true)
                pushIn(":", false)

                val emitted = traverseSchema(row, value)

                if (!emitted) { // less efficient
                  popIn()
                  popIn()

                  if (done) {
                    popIn()
                  }
                }

                loop(idx + 1, done || emitted)
              } else {
                done
              }
            }

            pushIn("{", false)
            val done = loop(0, false)

            if (done)
              ctx.push('}')
            else
              popIn()

            done

          case arr: SchemaNode.Arr =>
            val values = arr.nodes

            @inline
            @tailrec
            def loop(idx: Int, done: Boolean): Boolean = {
              if (idx < values.length) {
                val value = values(idx)

                if (done) {
                  pushIn(",", false)
                }

                val emitted = traverseSchema(row, value)

                if (!emitted && done) { // less efficient
                  popIn()
                }

                loop(idx + 1, done || emitted)
              } else {
                done
              }
            }

            pushIn("[", false)
            val done = loop(0, false)

            if (done)
              ctx.push(']')
            else
              popIn()

            done

          case union: SchemaNode.Union =>
            val pos = union.possibilities

            @inline
            @tailrec
            def loop(idx: Int): Boolean = {
              if (idx < pos.length) {
                traverseSchema(row, pos(idx)) || loop(idx + 1)
              } else {
                false
              }
            }

            loop(0)

          case SchemaNode.Leaf(tpe, col) =>
            tpe match {
              case CString => {
                val specCol = col.asInstanceOf[StrColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  ctx.renderString(specCol(row))
                  true
                } else {
                  false
                }
              }

              case CBoolean => {
                val specCol = col.asInstanceOf[BoolColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  ctx.renderBoolean(specCol(row))
                  true
                } else {
                  false
                }
              }

              case CLong =>
                val specCol = col.asInstanceOf[LongColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  ctx.renderLong(specCol(row))
                  true
                } else {
                  false
                }

              case CDouble =>
                val specCol = col.asInstanceOf[DoubleColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  ctx.renderDouble(specCol(row))
                  true
                } else {
                  false
                }

              case CNum =>
                val specCol = col.asInstanceOf[NumColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  ctx.renderNum(specCol(row))
                  true
                } else {
                  false
                }

              case CNull =>
                val specCol = col.asInstanceOf[NullColumn]
                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  ctx.renderNull()
                  true
                } else {
                  false
                }

              case CEmptyObject =>
                val specCol = col.asInstanceOf[EmptyObjectColumn]
                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  ctx.renderEmptyObject()
                  true
                } else {
                  false
                }

              case CEmptyArray =>
                val specCol = col.asInstanceOf[EmptyArrayColumn]
                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  ctx.renderEmptyArray()
                  true
                } else {
                  false
                }

              case COffsetDateTime =>
                val specCol = col.asInstanceOf[OffsetDateTimeColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  ctx.renderOffsetDateTime(specCol(row))
                  true
                } else {
                  false
                }

              case COffsetTime =>
                val specCol = col.asInstanceOf[OffsetTimeColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  ctx.renderOffsetTime(specCol(row))
                  true
                } else {
                  false
                }

              case COffsetDate =>
                val specCol = col.asInstanceOf[OffsetDateColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  ctx.renderOffsetDate(specCol(row))
                  true
                } else {
                  false
                }

              case CLocalDateTime =>
                val specCol = col.asInstanceOf[LocalDateTimeColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  ctx.renderLocalDateTime(specCol(row))
                  true
                } else {
                  false
                }

              case CLocalTime =>
                val specCol = col.asInstanceOf[LocalTimeColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  ctx.renderLocalTime(specCol(row))
                  true
                } else {
                  false
                }

              case CLocalDate =>
                val specCol = col.asInstanceOf[LocalDateColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  ctx.renderLocalDate(specCol(row))
                  true
                } else {
                  false
                }

              case CInterval =>
                val specCol = col.asInstanceOf[IntervalColumn]

                if (specCol.isDefinedAt(row)) {
                  flushIn()
                  ctx.renderInterval(specCol(row))
                  true
                } else {
                  false
                }

              // NB: I removed this implementation because it was horrible and broken
              case CArrayType(_) => errorImpossible

              case CUndefined => false
            }
        }

        @tailrec
        def render(row: Int, delimit: Boolean): Boolean = {
          if (row < size) {
            if (delimit) {
              pushIn(delimiter, false)
            }

            val rowRendered = traverseSchema(row, schema)

            if (delimit && !rowRendered) {
              popIn()
            }

            render(row + 1, delimit || rowRendered)
          } else {
            delimit
          }
        }

        val rendered = render(0, false)

        ctx.finish()

        // it's safe for us to expose this mutable Seq as "immutable" since we wont' be touching it anymore
        (ctx.toSeq(), rendered)

      case None =>
        Seq.empty[CharBuffer] -> false
    }
  }

  def toRValue(row: Int): RValue = {
    columns.foldLeft[RValue](CUndefined) {
      case (rv, (ColumnRef(selector, _), col)) if col.isDefinedAt(row) =>
        CPathUtils.cPathToJPaths(selector, col.cValue(row)).foldLeft(rv) {
          case (rv, (path, value)) => rv.unsafeInsert(CPath(path), value)
        }

      case (rv, _) => rv
    }
  }

  def toRValues: List[RValue] = {
    @tailrec
    def loop(idx: Int, rvalues: List[RValue]): List[RValue] =
      if (idx >= 0)
        toRValue(idx) match {
          case CUndefined => loop(idx - 1, rvalues)
          case rv => loop(idx - 1, rv :: rvalues)
        }
      else
        rvalues

    loop(source.size - 1, Nil)
  }

  def toJValue(row: Int) = {
    columns.foldLeft[JValue](JUndefined) {
      case (jv, (ColumnRef(selector, _), col)) if col.isDefinedAt(row) =>
        CPathUtils.cPathToJPaths(selector, col.cValue(row)).foldLeft(jv) {
          case (jv, (path, value)) => jv.unsafeInsert(path, value.toJValueRaw)
        }

      case (jv, _) => jv
    }
  }

  def toJson(row: Int): Option[JValue] = {
    toJValue(row) match {
      case JUndefined => None
      case jv => Some(jv)
    }
  }

  def toJsonElements: Vector[JValue] = {
    @tailrec def rec(i: Int, acc: Vector[JValue]): Vector[JValue] = {
      if (i < source.size) {
        toJValue(i) match {
          case JUndefined => rec(i + 1, acc)
          case jv => rec(i + 1, acc :+ jv)
        }
      } else acc
    }

    rec(0, Vector())
  }

  def toRJsonElements: Vector[RValue] = {
    @tailrec def rec(i: Int, acc: Vector[RValue]): Vector[RValue] = {
      if (i < source.size) {
        toRValue(i) match {
          case CUndefined => rec(i + 1, acc)
          case jv => rec(i + 1, acc :+ jv)
        }
      } else acc
    }

    rec(0, Vector())
  }

  def toString(row: Int): Option[String] = {
    (columns.toList.sortBy(_._1) map { case (ref, col) => ref.toString + ": " + (if (col.isDefinedAt(row)) col.strValue(row) else "(undefined)") }) match {
      case Nil => None
      case l => Some(l.mkString("[", ", ", "]"))
    }
  }

  def toJsonString(prefix: String = ""): String = {
    (0 until size).map(i => prefix + " " + toJson(i)).mkString("\n")
  }

  override def toString = (0 until size).map(toString(_).getOrElse("")).mkString("\n", "\n", "\n")

  private[this] def transformSchemaPrecise(precise: Boolean)(node: SchemaNode): SchemaNode = {
    import DataCodec.PreciseKeys._

    def inner(node: SchemaNode): SchemaNode = node match {
      case obj @ SchemaNode.Obj(nodes) if obj.keys.exists(_.contains("$")) =>
        SchemaNode.Obj(Map(ObjKey -> SchemaNode.Obj(nodes.mapValues(inner))))

      case SchemaNode.Obj(nodes) =>
        SchemaNode.Obj(nodes.mapValues(inner))

      case SchemaNode.Arr(map) =>
        SchemaNode.Arr(map.mapValues(inner))

      case SchemaNode.Union(nodes) =>
        SchemaNode.Union(nodes.map(inner))

      case lf @ SchemaNode.Leaf(CLocalDateTime, col) =>
        SchemaNode.Obj(Map(LocalDateTimeKey -> lf))

      case lf @ SchemaNode.Leaf(CLocalDate, col) =>
        SchemaNode.Obj(Map(LocalDateKey -> lf))

      case lf @ SchemaNode.Leaf(CLocalTime, col) =>
        SchemaNode.Obj(Map(LocalTimeKey -> lf))

      case lf @ SchemaNode.Leaf(COffsetDateTime, col) =>
        SchemaNode.Obj(Map(OffsetDateTimeKey -> lf))

      case lf @ SchemaNode.Leaf(COffsetDate, col) =>
        SchemaNode.Obj(Map(OffsetDateKey -> lf))

      case lf @ SchemaNode.Leaf(COffsetTime, col) =>
        SchemaNode.Obj(Map(OffsetTimeKey -> lf))

      case lf @ SchemaNode.Leaf(CInterval, col) =>
        SchemaNode.Obj(Map(IntervalKey -> lf))

      case lf @ SchemaNode.Leaf(_, _) => lf
    }

    if (precise)
      normalizeSchema(inner(node)).getOrElse(sys.error("weird failure in precise transform"))
    else
      node
  }

  private[this] def sliceSchema: Option[SchemaNode] = {
    if (columns.isEmpty) {
      None
    } else {
      def insert(target: SchemaNode, ref: ColumnRef, col: Column): SchemaNode = {
        val ColumnRef(selector, ctype) = ref

        selector.nodes match {
          case CPathField(name) :: tail =>
            target match {
              case SchemaNode.Obj(nodes) => {
                val subTarget = nodes get name getOrElse SchemaNode.Union(Set())
                val result    = insert(subTarget, ColumnRef(CPath(tail), ctype), col)
                SchemaNode.Obj(nodes + (name -> result))
              }

              case SchemaNode.Union(nodes) => {
                val objNode = nodes find {
                  case _: SchemaNode.Obj => true
                  case _ => false
                }

                val subTarget = objNode getOrElse SchemaNode.Obj(Map())
                SchemaNode.Union(nodes - subTarget + insert(subTarget, ref, col))
              }

              case node =>
                SchemaNode.Union(Set(node, insert(SchemaNode.Obj(Map()), ref, col)))
            }

          case CPathIndex(idx) :: tail =>
            target match {
              case SchemaNode.Arr(map) => {
                val subTarget = map get idx getOrElse SchemaNode.Union(Set())
                val result    = insert(subTarget, ColumnRef(CPath(tail), ctype), col)
                SchemaNode.Arr(map + (idx -> result))
              }

              case SchemaNode.Union(nodes) => {
                val objNode = nodes find {
                  case _: SchemaNode.Arr => true
                  case _ => false
                }

                val subTarget = objNode getOrElse SchemaNode.Arr(Map())
                SchemaNode.Union(nodes - subTarget + insert(subTarget, ref, col))
              }

              case node =>
                SchemaNode.Union(Set(node, insert(SchemaNode.Arr(Map()), ref, col)))
            }

          case CPathMeta(_) :: _ => target

          case CPathArray :: _ => sys.error("todo")

          case Nil =>
            val node = SchemaNode.Leaf(ctype, col)

            target match {
              case SchemaNode.Union(nodes) => SchemaNode.Union(nodes + node)
              case oldNode => SchemaNode.Union(Set(oldNode, node))
            }
        }
      }

      val schema = columns.foldLeft(SchemaNode.Union(Set()): SchemaNode) {
        case (acc, (ref, col)) => insert(acc, ref, col)
      }

      normalizeSchema(schema)
    }
  }

  private[this] def normalizeSchema(schema: SchemaNode): Option[SchemaNode] = schema match {
    case SchemaNode.Obj(nodes) =>
      val nodes2 = nodes flatMap {
        case (key, value) => normalizeSchema(value) map { key -> _ }
      }

      val back =
        if (nodes2.isEmpty)
          None
        else
          Some(SchemaNode.Obj(nodes2))

      back foreach { obj =>
        obj.keys = new Array[String](nodes2.size)
        obj.values = new Array[SchemaNode](nodes2.size)
      }

      var i = 0
      back foreach { obj =>
        for ((key, value) <- nodes2) {
          obj.keys(i) = key
          obj.values(i) = value
          i += 1
        }
      }

      back


    case SchemaNode.Arr(map) =>
      val map2 = map flatMap {
        case (idx, value) => normalizeSchema(value) map { idx -> _ }
      }

      val back =
        if (map2.isEmpty)
          None
        else
          Some(SchemaNode.Arr(map2))

      back foreach { arr =>
        arr.nodes = new Array[SchemaNode](map2.size)
      }

      var i = 0
      back foreach { arr =>
        val values = map2.toSeq sortBy { _._1 } map { _._2 }

        for (value <- values) {
          arr.nodes(i) = value
          i += 1
        }
      }

      back

    case SchemaNode.Union(nodes) =>
      val nodes2 = nodes.flatMap(normalizeSchema)

      if (nodes2.isEmpty)
        None
      else if (nodes2.size == 1)
        nodes2.headOption
      else {
        val union = SchemaNode.Union(nodes2)
        union.possibilities = nodes2.toArray
        Some(union)
      }

    case lf: SchemaNode.Leaf => Some(lf)
  }
}

object Slice {

  private def replaceColumnImpl(size: Int, cols: Map[ColumnRef, Column]): Map[ColumnRef, Column] = {
    def step(acc: Map[ColumnRef, Column], cref: ColumnRef, col: Column): Map[ColumnRef, Column] =
      if (col.isDefinedAt(0)) {
        val c = cref.ctype match {
          case CBoolean => SingletonBoolColumn(col.asInstanceOf[BoolColumn](0))
          case CLong => SingletonLongColumn(col.asInstanceOf[LongColumn](0))
          case CDouble => SingletonDoubleColumn(col.asInstanceOf[DoubleColumn](0))
          case CNum => SingletonNumColumn(col.asInstanceOf[NumColumn](0))
          case CString => SingletonStrColumn(col.asInstanceOf[StrColumn](0))
          case COffsetDateTime => SingletonOffsetDateTimeColumn(col.asInstanceOf[OffsetDateTimeColumn](0))
          case COffsetDate => SingletonOffsetDateColumn(col.asInstanceOf[OffsetDateColumn](0))
          case COffsetTime => SingletonOffsetTimeColumn(col.asInstanceOf[OffsetTimeColumn](0))
          case CLocalDateTime => SingletonLocalDateTimeColumn(col.asInstanceOf[LocalDateTimeColumn](0))
          case CLocalDate => SingletonLocalDateColumn(col.asInstanceOf[LocalDateColumn](0))
          case CLocalTime => SingletonLocalTimeColumn(col.asInstanceOf[LocalTimeColumn](0))
          case CInterval => SingletonIntervalColumn(col.asInstanceOf[IntervalColumn](0))
          case CNull => col
          case CUndefined => col
          case CEmptyArray => col
          case CEmptyObject => col
          case CArrayType(_) => col
        }
        acc + (cref -> c)
      } else
        acc

    if (size == 1)
      cols.foldLeft[Map[ColumnRef, Column]](Map.empty) {
        case (acc, (cref, col)) => step(acc, cref, col)
      }
    else
      cols
  }

  def empty: Slice = Slice(0, Map.empty)

  def apply(dataSize: Int, columns0: Map[ColumnRef, Column]): Slice =
    new Slice {
      val size = dataSize
      val columns = replaceColumnImpl(dataSize, columns0)
    }

  def updateRefs(rv: List[(CPath, CValue)], into: Map[ColumnRef, ArrayColumn[_]], sliceIndex: Int, sliceSize: Int): Map[ColumnRef, ArrayColumn[_]] = {
    var acc = into
    var cursor = rv
    while (cursor.isInstanceOf[::[(CPath, CValue)]]) {
      cursor.head match {
        case (cpath, CUndefined) =>
        case (cpath, cvalue) =>
          val ref = ColumnRef(cpath, cvalue.cType)

          val updatedColumn: ArrayColumn[_] = cvalue match {
            case CBoolean(b) =>
              val c = acc.getOrElse(ref, ArrayBoolColumn.empty(sliceSize)).asInstanceOf[ArrayBoolColumn]
              c.update(sliceIndex, b)

            case CLong(d) =>
              val c = acc.getOrElse(ref, ArrayLongColumn.empty(sliceSize)).asInstanceOf[ArrayLongColumn]
              c.update(sliceIndex, d.toLong)

            case CDouble(d) =>
              val c = acc.getOrElse(ref, ArrayDoubleColumn.empty(sliceSize)).asInstanceOf[ArrayDoubleColumn]
              c.update(sliceIndex, d.toDouble)

            case CNum(d) =>
              val c = acc.getOrElse(ref, ArrayNumColumn.empty(sliceSize)).asInstanceOf[ArrayNumColumn]
              c.update(sliceIndex, d)

            case CString(s) =>
              val c = acc.getOrElse(ref, ArrayStrColumn.empty(sliceSize)).asInstanceOf[ArrayStrColumn]
              c.update(sliceIndex, s)

            case COffsetDateTime(d) =>
              val c = acc.getOrElse(ref, ArrayOffsetDateTimeColumn.empty(sliceSize)).asInstanceOf[ArrayOffsetDateTimeColumn]
              c.update(sliceIndex, d)

            case COffsetTime(d) =>
              val c = acc.getOrElse(ref, ArrayOffsetTimeColumn.empty(sliceSize)).asInstanceOf[ArrayOffsetTimeColumn]
              c.update(sliceIndex, d)

            case COffsetDate(d) =>
              val c = acc.getOrElse(ref, ArrayOffsetDateColumn.empty(sliceSize)).asInstanceOf[ArrayOffsetDateColumn]
              c.update(sliceIndex, d)

            case CLocalDateTime(d) =>
              val c = acc.getOrElse(ref, ArrayLocalDateTimeColumn.empty(sliceSize)).asInstanceOf[ArrayLocalDateTimeColumn]
              c.update(sliceIndex, d)

            case CLocalTime(d) =>
              val c = acc.getOrElse(ref, ArrayLocalTimeColumn.empty(sliceSize)).asInstanceOf[ArrayLocalTimeColumn]
              c.update(sliceIndex, d)

            case CLocalDate(d) =>
              val c = acc.getOrElse(ref, ArrayLocalDateColumn.empty(sliceSize)).asInstanceOf[ArrayLocalDateColumn]
              c.update(sliceIndex, d)

            case CInterval(p) =>
              val c = acc.getOrElse(ref, ArrayIntervalColumn.empty(sliceSize)).asInstanceOf[ArrayIntervalColumn]
              c.update(sliceIndex, p)

            case CArray(arr, cType) =>
              val c = acc.getOrElse(ref, ArrayHomogeneousArrayColumn.empty(sliceSize)(cType)).asInstanceOf[ArrayHomogeneousArrayColumn[cType.tpe]]
              c.update(sliceIndex, arr)

            case CEmptyArray =>
              val c = acc.getOrElse(ref, MutableEmptyArrayColumn.empty()).asInstanceOf[MutableEmptyArrayColumn]
              c.update(sliceIndex, true)

            case CEmptyObject =>
              val c = acc.getOrElse(ref, MutableEmptyObjectColumn.empty()).asInstanceOf[MutableEmptyObjectColumn]
              c.update(sliceIndex, true)

            case CNull =>
              val c = acc.getOrElse(ref, MutableNullColumn.empty()).asInstanceOf[MutableNullColumn]
              c.update(sliceIndex, true)

            case x =>
              sys.error(s"Unexpected arg $x")
          }

          acc = acc.updated(ref, updatedColumn)
      }
      cursor = cursor.tail
    }
    acc
  }

  // doesn't return the next power of two
  // when the input is already a power of two
  private[table] def nextPowerOfTwo(i: Int): Int = {
    val highestOneBit = Integer.highestOneBit(i)
    if (i == highestOneBit)
      i
    else
      highestOneBit << 1
  }

  private[table] def fromRValuesStep(
    values: ArraySliced[RValue], maxRows: Int, maxColumns: Int, startingSize: Int
  ): (Slice, ArraySliced[RValue]) = {
    @tailrec
    def inner(
      next: ArraySliced[RValue], rows: Int, colsOverflowed: Boolean,
      acc: Map[ColumnRef, ArrayColumn[_]], allocatedColSize: Int
    ): (Slice, ArraySliced[RValue]) = {
      if (next.size == 0) {
        // there's no more data to make slices of.
        // we'll have to emit whatever data we have already as a slice.
        if (acc.isEmpty) {
          // println("no more data to use, emitting slice with no data")
          (Slice.empty, ArraySliced.noRValues)
        } else {
          // println("no more data to use, emitting slice with data")
          (Slice(rows, acc), ArraySliced.noRValues)
        }
      } else {
        // we have some data.
        if (rows >= maxRows) {
          // we'd have more rows than `maxRows` if we added
          // this row to the slice. so we have to emit the
          // data we have already as a slice, and pass
          // the row back to the caller.
          // println(s"we have the maximum ($maxRows) rows, cutting slice early")
          (Slice(rows, acc), next)
        } else if (rows >= allocatedColSize && !colsOverflowed) {
          // we'd have more rows than `allocatedColSize` if we added this
          // row to the slice. `allocatedColSize` is the size in rows of
          // all of the columns we've accumulated, so we'll
          // have to resize those columns to add further data.
          // `allocatedColSize` should always be a power of two, so we multiply
          // by two.
          val newSize =
            if (allocatedColSize == 2 && startingSize != 2) startingSize
            else allocatedColSize * 2
          // println(s"we're resizing the columns from $allocatedColSize to $newSize because we have too many rows")
          inner(next, rows, false, acc.map { case (k, c) => (k, c.resize(newSize)) }.toMap, newSize)
        } else {
          // we already have too many columns, so there's no need to check
          // if the next row would put us over the limit.
          if (colsOverflowed) {
          // println(s"we already have too many columns (${acc.size}), cutting slice early at $rows (allocated size $allocatedColSize)")
            (Slice(rows, acc), next)
          } else {
            // we *may* have enough space in this slice for the
            // next `RValue`. we're going to flatten the `RValue`
            // out to find out how many columns we would have
            // if we added that `RValue` to the current slice.
            // if we have no other rows, we'll start with a two-row
            // slice, just in case we have too many columns immediately.
            val flattened = next.head.flattenWithPath
            val newAcc = updateRefs(flattened, acc, rows, allocatedColSize)
            val newCols = newAcc.size
            if (newCols > maxColumns && rows > 0) {
              // we would have too many columns in this slice if we added this
              // `RValue`. so we're going to pass it back to the caller and
              // return a slice with the data we have already. we don't have to
              // clear the new `RValue`'s data out of the accumulated data,
              // because we've already made sure to set `size` correctly.
              // println(s"we would have too many columns with this new value ($newCols), cutting slice early at $rows")
              (Slice(rows, acc), next)
            } else {
              // we're okay with adding this RValue to the slice!
              // we already have the slice's data including RValue in `newAcc`,
              // so we pass that on and advance the data cursor.
              // println("we're adding an RValue to the slice")
              inner(next.tail, rows + 1, newCols > maxColumns, newAcc, allocatedColSize)
            }
          }
        }
      }
    }

    if (values.size == 0) {
      (Slice.empty, ArraySliced.noRValues)
    } else {
      val size = Math.min(maxRows, startingSize)
      inner(values, 0, false, Map.empty, size)
    }
  }

  def allFromRValues[F[_]](
      values: fs2.Stream[F, RValue],
      maxRows: Option[Int] = None,
      maxColumns: Option[Int] = None )
      : fs2.Stream[F, Slice] = {

    val maxRowsC = maxRows.getOrElse(Config.maxSliceRows)
    val maxColumnsC = maxColumns.getOrElse(Config.maxSliceColumns)
    // println(s"maxRows: $maxRowsC, maxCols: $maxColumnsC")

    def rec(next: ArraySliced[RValue], values: fs2.Stream[F, RValue])
        : fs2.Pull[F, Slice, Unit] =
      if (next.size == 0) {
        values.pull.unconsChunk flatMap {
          case Some((chunk, next)) =>
            val chunkArr = chunk.toArray
            rec(ArraySliced(chunkArr, 0, chunkArr.length), next)
          case None =>
            // println("Finished top-level loop")
            fs2.Pull.done
        }
      } else {
        // println(s"extracting slice from data with size ${next.size}")
        val (nextSlice, remainingData) =
          Slice.fromRValuesStep(next, maxRowsC, maxColumnsC, Config.defaultMinRows)

        fs2.Pull.output1(nextSlice) >> rec(remainingData, values)
      }

    rec(ArraySliced(new Array[RValue](0), 0, 0), values).stream
  }

  @deprecated("use allFromRValues", "52.0.2")
  def fromJValues(values: Stream[JValue]): Slice =
    fromRValues(values.map(RValue.fromJValueRaw))

  // This doesn't limit slice size properly.
  @deprecated("use allFromRValues", "52.0.2")
  def fromRValues(values: Stream[RValue]): Slice = {
    val sliceSize = values.size

    @tailrec def buildColArrays(from: Stream[RValue], into: Map[ColumnRef, ArrayColumn[_]], sliceIndex: Int)
        : (Map[ColumnRef, ArrayColumn[_]], Int) =
      from match {
        case jv #:: xs =>
          val refs = updateRefs(jv.flattenWithPath, into, sliceIndex, sliceSize)
          buildColArrays(xs, refs, sliceIndex + 1)
        case _ =>
          (into, sliceIndex)
      }

    val (columns, size) = buildColArrays(values, Map.empty[ColumnRef, ArrayColumn[_]], 0)
    Slice(size, columns)
  }

  /**
    * Concatenate multiple slices into 1 big slice. The slices will be
    * concatenated in the order they appear in `slices`.
    */
  def concat(slices: Seq[Slice]): Slice = {
    val (_columns, _size) = slices.foldLeft((Map.empty[ColumnRef, List[(Int, Column)]], 0)) {
      case ((cols, offset), slice) if slice.size > 0 =>
        (slice.columns.foldLeft(cols) {
          case (acc, (ref, col)) =>
            acc + (ref -> ((offset, col) :: acc.getOrElse(ref, Nil)))
        }, offset + slice.size)

      case ((cols, offset), _) => (cols, offset)
    }

    Slice(
      _size,
      _columns.flatMap {
        case (ref, parts) =>
          cf.util.NConcat(parts) map ((ref, _))
      })
  }

  def rowComparatorFor(s1: Slice, s2: Slice)(keyf: Slice => Iterable[CPath]): RowComparator = {
    val paths     = (keyf(s1) ++ keyf(s2)).toList
    val traversal = CPathTraversal(paths)
    val lCols     = s1.columns groupBy (_._1.selector) map { case (path, m) => path -> m.values.toSet }
    val rCols     = s2.columns groupBy (_._1.selector) map { case (path, m) => path -> m.values.toSet }
    val allPaths  = (lCols.keys ++ rCols.keys).toList
    val order     = traversal.rowOrder(allPaths, lCols, Some(rCols))
    new RowComparator {
      def compare(r1: Int, r2: Int): Ordering = scalaz.Ordering.fromInt(order.compare(r1, r2))
    }
  }

  /**
    * Given a JValue, an existing map of columnrefs to column data,
    * a sliceIndex, and a sliceSize, return an updated map.
    */
  def withIdsAndValues(jv: JValue,
                       into: Map[ColumnRef, ArrayColumn[_]],
                       sliceIndex: Int,
                       sliceSize: Int,
                       remapPath: Option[JPath => CPath] = None): Map[ColumnRef, ArrayColumn[_]] = {
    jv.flattenWithPath.foldLeft(into) {
      case (acc, (jpath, JUndefined)) => acc
      case (acc, (jpath, v)) =>
        val ctype = CType.forJValueRaw(v) getOrElse { sys.error("Cannot determine ctype for " + v + " at " + jpath + " in " + jv) }
        val ref   = ColumnRef(remapPath.map(_ (jpath)).getOrElse(CPath(jpath)), ctype)

        val updatedColumn: ArrayColumn[_] = v match {
          case JBool(b) =>
            val c = acc.getOrElse(ref, ArrayBoolColumn.empty(sliceSize)).asInstanceOf[ArrayBoolColumn]
            c.update(sliceIndex, b)

            c

          case JNum(d) =>
            ctype match {
              case CLong =>
                val c = acc.getOrElse(ref, ArrayLongColumn.empty(sliceSize)).asInstanceOf[ArrayLongColumn]
                c.update(sliceIndex, d.toLong)

                c

              case CDouble =>
                val c = acc.getOrElse(ref, ArrayDoubleColumn.empty(sliceSize)).asInstanceOf[ArrayDoubleColumn]
                c.update(sliceIndex, d.toDouble)

                c

              case CNum =>
                val c = acc.getOrElse(ref, ArrayNumColumn.empty(sliceSize)).asInstanceOf[ArrayNumColumn]
                c.update(sliceIndex, d)

                c

              case _ => sys.error("non-numeric type reached")
            }

          case JString(s) =>
            val c = acc.getOrElse(ref, ArrayStrColumn.empty(sliceSize)).asInstanceOf[ArrayStrColumn]
            c.update(sliceIndex, s)

            c

          case JArray(Nil) =>
            val c = acc.getOrElse(ref, MutableEmptyArrayColumn.empty()).asInstanceOf[MutableEmptyArrayColumn]
            c.update(sliceIndex, true)

            c

          case JObject.empty =>
            val c = acc.getOrElse(ref, MutableEmptyObjectColumn.empty()).asInstanceOf[MutableEmptyObjectColumn]
            c.update(sliceIndex, true)

            c

          case JNull =>
            val c = acc.getOrElse(ref, MutableNullColumn.empty()).asInstanceOf[MutableNullColumn]
            c.update(sliceIndex, true)

            c

          case _ => sys.error("non-flattened value reached")
        }

        acc + (ref -> updatedColumn)
    }
  }

  private sealed trait SchemaNode

  private object SchemaNode {
    final case class Obj(nodes: Map[String, SchemaNode]) extends SchemaNode {
      final var keys: Array[String]       = _
      final var values: Array[SchemaNode] = _
    }

    final case class Arr(map: Map[Int, SchemaNode]) extends SchemaNode {
      final var nodes: Array[SchemaNode] = _
    }

    final case class Union(nodes: Set[SchemaNode]) extends SchemaNode {
      final var possibilities: Array[SchemaNode] = _
    }

    final case class Leaf(tpe: CType, col: Column) extends SchemaNode
  }

  /*
   * We manually encode a bimorphic delegating to these function values
   * because we're probably going to have over 10k rows in a given dataset.
   * Having over 10k rows would mean that we would over-specialize to
   * monomorphism on the very first rendering call, then subsequently
   * despecialize *entirely* when subsequent renders come in. By manually
   * encoding the bimorphism, we can ensure that both call-sites get
   * specialized.
   */
  private final class BufferContext(
      size: Int,
      _renderJsonString: (BufferContext, String) => Unit,   // maybe null
      _renderCsvString: (BufferContext, String) => Unit) {  // maybe null

    val RenderBufferSize = 1024 * 10 // 10 KB

    private[this] var buffer = CharBuffer.allocate(RenderBufferSize)
    private[this] val vector = new mutable.ArrayBuffer[CharBuffer](math.max(1, size / 10))

    def checkPush(length: Int): Unit = {
      if (buffer.remaining < length) {
        buffer.flip()
        vector += buffer

        buffer = CharBuffer.allocate(RenderBufferSize)
      }
    }

    def push(c: Char): Unit = {
      checkPush(1)
      buffer.put(c)
    }

    def pushStr(str: String): Unit = {
      checkPush(str.length)
      buffer.put(str)
    }

    def renderString(str: String): Unit = {
      if (_renderJsonString eq null)
        _renderCsvString(this, str)
      else
        _renderJsonString(this, str)
    }

    def renderLong(ln: Long): Unit = {
      @tailrec
      def power10(ln: Long, seed: Long = 1): Long = {
        // note: we could be doing binary search here

        if (seed * 10 < 0) // overflow
          seed
        else if (seed * 10 > ln)
          seed
        else
          power10(ln, seed * 10)
      }

      @tailrec
      def renderPositive(ln: Long, power: Long): Unit = {
        if (power > 0) {
          val c = Character.forDigit((ln / power % 10).toInt, 10)
          push(c)
          renderPositive(ln, power / 10)
        }
      }

      if (ln == Long.MinValue) {
        pushStr("-9223372036854775808")
      } else if (ln == 0) {
        push('0')
      } else if (ln < 0) {
        push('-')

        val ln2 = ln * -1
        renderPositive(ln2, power10(ln2))
      } else {
        renderPositive(ln, power10(ln))
      }
    }

    // TODO is this a problem?
    def renderDouble(d: Double): Unit = pushStr(d.toString)

    // TODO is this a problem?
    def renderNum(d: BigDecimal): Unit = pushStr(d.toString)

    def renderBoolean(b: Boolean): Unit = {
      if (b)
        pushStr("true")
      else
        pushStr("false")
    }

    def renderNull(): Unit = pushStr("null")

    def renderEmptyObject(): Unit = pushStr("{}")

    def renderEmptyArray(): Unit = pushStr("[]")

    def renderOffsetDateTime(time: OffsetDateTime): Unit = renderString(time.toString)

    def renderOffsetTime(time: OffsetTime): Unit = renderString(time.toString)

    def renderOffsetDate(date: OffsetDate): Unit = renderString(date.toString)

    def renderLocalDateTime(time: LocalDateTime): Unit = renderString(time.toString)

    def renderLocalTime(time: LocalTime): Unit = renderString(time.toString)

    def renderLocalDate(date: LocalDate): Unit = renderString(date.toString)

    def renderInterval(duration: DateTimeInterval): Unit = renderString(duration.toString)

    def finish(): Unit = {
      buffer.flip()
      vector += buffer
    }

    def toSeq(): Seq[CharBuffer] = {
      buffer = null    // invalidate the state so we throw exceptions later
      vector
    }
  }

  private object BufferContext {

    @inline
    def json(size: Int)(renderString: (BufferContext, String) => Unit): BufferContext =
      new BufferContext(size, renderString, null)

    @inline
    def csv(size: Int)(renderString: (BufferContext, String) => Unit): BufferContext =
      new BufferContext(size, null, renderString)
  }
}
