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

import quasar.blueeyes._
import quasar.precog.BitSet
import quasar.precog.common._
import quasar.yggdrasil.bytecode._
import quasar.yggdrasil.table._

object Schema {
  def ctypes(jtype: JType): Set[CType] = jtype match {
    case JArrayFixedT(indices) if indices.isEmpty => Set(CEmptyArray)
    case JObjectFixedT(fields) if fields.isEmpty  => Set(CEmptyObject)
    case JArrayFixedT(indices) =>
      indices.values.toSet.flatMap { tpe: JType =>
        ctypes(tpe)
      }
    case JObjectFixedT(fields) =>
      fields.values.toSet.flatMap { tpe: JType =>
        ctypes(tpe)
      }
    case JArrayHomogeneousT(elemType) =>
      ctypes(elemType) collect {
        case cType: CValueType[_] => CArrayType(cType)
      }
    case JNumberT         => Set(CLong, CDouble, CNum)
    case JTextT           => Set(CString)
    case JBooleanT        => Set(CBoolean)
    case JNullT           => Set(CNull)
    case JOffsetDateTimeT => Set(COffsetDateTime)
    case JOffsetTimeT     => Set(COffsetTime)
    case JOffsetDateT     => Set(COffsetDate)
    case JLocalDateTimeT  => Set(CLocalDateTime)
    case JLocalTimeT      => Set(CLocalTime)
    case JLocalDateT      => Set(CLocalDate)
    case JIntervalT       => Set(CInterval)
    case _                => Set.empty
  }

  def cpath(jtype: JType): Seq[CPath] = {
    val cpaths = jtype match {
      case JArrayFixedT(indices)                           => indices flatMap { case (idx, tpe) => CPath(CPathIndex(idx)) combine cpath(tpe) } toSeq
      case JObjectFixedT(fields)                           => fields flatMap { case (name, tpe) => CPath(CPathField(name)) combine cpath(tpe) } toSeq
      case JArrayHomogeneousT(elemType)                    => Seq(CPath(CPathArray))
      case _                                               => Nil
    }

    cpaths.sorted
  }

  def sample(jtype: JType, size: Int): Option[JType] = {
    val paths                        = flatten(jtype, Nil) groupBy { _.selector } toSeq
    val sampledPaths: Seq[ColumnRef] = scala.util.Random.shuffle(paths).take(size) flatMap { _._2 }

    mkType(sampledPaths)
  }

  def flatten(jtype: JType, refsOriginal: List[ColumnRef]): Set[ColumnRef] = {
    def buildPath(nodes: List[CPathNode], refs: List[ColumnRef], jType: JType): List[ColumnRef] = jType match {
      case JArrayFixedT(indices) if indices.isEmpty =>
        ColumnRef(CPath(nodes.reverse), CEmptyArray) :: Nil

      case JObjectFixedT(fields) if fields.isEmpty =>
        ColumnRef(CPath(nodes.reverse), CEmptyObject) :: Nil

      case JArrayFixedT(indices) =>
        indices.toList.flatMap {
          case (idx, tpe) =>
            val refs0 = refs collect {
              case ColumnRef(CPath(CPathIndex(`idx`), rest @ _ *), ctype) =>
                ColumnRef(CPath(rest: _*), ctype)
            }
            buildPath(CPathIndex(idx) :: nodes, refs0, tpe)
        }

      case JObjectFixedT(fields) => {
        fields.toList.flatMap {
          case (field, tpe) =>
            val refs0 = refs collect {
              case ColumnRef(CPath(CPathField(`field`), rest @ _ *), ctype) =>
                ColumnRef(CPath(rest: _*), ctype)
            }
            buildPath(CPathField(field) :: nodes, refs0, tpe)
        }
      }

      case JArrayUnfixedT =>
        refs collect {
          case ColumnRef(p @ CPath(CPathIndex(i), rest @ _ *), ctype) =>
            ColumnRef(CPath(nodes.reverse) \ p, ctype)
          case ColumnRef(CPath(), CEmptyArray) =>
            ColumnRef(CPath(nodes.reverse), CEmptyArray)
        }

      case JObjectUnfixedT =>
        refs collect {
          case ColumnRef(p @ CPath(CPathField(i), rest @ _ *), ctype) =>
            ColumnRef(CPath(nodes.reverse) \ p, ctype)
          case ColumnRef(CPath(), CEmptyObject) =>
            ColumnRef(CPath(nodes.reverse), CEmptyObject)
        }

      case JArrayHomogeneousT(tpe) =>
        val refs0 = refs collect {
          case ColumnRef(CPath(CPathArray, rest @ _ *), ctype) =>
            ColumnRef(CPath(rest: _*), ctype)
        }
        buildPath(CPathArray :: nodes, refs0, tpe)

      case JNumberT =>
        val path = CPath(nodes.reverse)
        ColumnRef(path, CLong: CType) :: ColumnRef(path, CDouble) :: ColumnRef(path, CNum) :: Nil

      case JUnionT(ltpe, rtpe) =>
        buildPath(nodes, refs, ltpe) ++ buildPath(nodes, refs, rtpe)

      case JTextT =>
        ColumnRef(CPath(nodes.reverse), CString) :: Nil

      case JBooleanT =>
        ColumnRef(CPath(nodes.reverse), CBoolean) :: Nil

      case JOffsetDateTimeT =>
        ColumnRef(CPath(nodes.reverse), COffsetDateTime) :: Nil

      case JOffsetTimeT =>
        ColumnRef(CPath(nodes.reverse), COffsetTime) :: Nil

      case JOffsetDateT =>
        ColumnRef(CPath(nodes.reverse), COffsetDate) :: Nil

      case JLocalDateTimeT =>
        ColumnRef(CPath(nodes.reverse), CLocalDateTime) :: Nil

      case JLocalTimeT =>
        ColumnRef(CPath(nodes.reverse), CLocalTime) :: Nil

      case JLocalDateT =>
        ColumnRef(CPath(nodes.reverse), CLocalDate) :: Nil

      case JIntervalT =>
        ColumnRef(CPath(nodes.reverse), CInterval) :: Nil

      case JNullT =>
        ColumnRef(CPath(nodes.reverse), CNull) :: Nil
    }

    buildPath(Nil, refsOriginal, jtype).toSet
  }

  private def fromCValueType(t: CValueType[_]): Option[JType] = t match {
    case CBoolean               => Some(JBooleanT)
    case CString                => Some(JTextT)
    case CLong | CDouble | CNum => Some(JNumberT)
    case CArrayType(elemType)   => fromCValueType(elemType) map JArrayHomogeneousT
    case COffsetDateTime        => Some(JOffsetDateTimeT)
    case COffsetTime            => Some(JOffsetTimeT)
    case COffsetDate            => Some(JOffsetDateT)
    case CLocalDateTime         => Some(JLocalDateTimeT)
    case CLocalTime             => Some(JLocalTimeT)
    case CLocalDate             => Some(JLocalDateT)
    case CInterval              => Some(JIntervalT)
    case _                      => None
  }

  /**
    * replaces all leaves in `jtype` with `leaf`
    */
  def replaceLeaf(jtype: JType)(leaf: JType): JType = {
    def inner(jtype: JType): JType = jtype match {
      case JNumberT | JTextT | JBooleanT | JNullT |
           JLocalDateTimeT | JLocalTimeT | JLocalDateT |
           JOffsetDateTimeT | JOffsetTimeT | JOffsetDateT |
           JIntervalT              => leaf
      case JArrayFixedT(elements)  => JArrayFixedT(elements.mapValues(inner))
      case JObjectFixedT(fields)   => JObjectFixedT(fields.mapValues(inner))
      case JUnionT(left, right)    => JUnionT(inner(left), inner(right))
      case JArrayHomogeneousT(tpe) => JArrayHomogeneousT(inner(tpe))
      case JArrayUnfixedT          => JArrayUnfixedT
      case JObjectUnfixedT         => JObjectUnfixedT
    }

    inner(jtype)
  }

  /**
    * returns a function that, for a given (row: Int), produces a Boolean
    * value is true if the given row subsumes the provided `jtpe`
    */
  def findTypes(jtpe: JType, seenPath: CPath, cols: Map[ColumnRef, Column], size: Int): Int => Boolean = {
    def handleRoot(providedCTypes: Seq[CType], cols: Map[ColumnRef, Column]) = {
      val filteredCols = cols filter {
        case (ColumnRef(path, ctpe), _) =>
          path == seenPath && providedCTypes.contains(ctpe)
      }
      val bits = filteredCols.values map {
        _.definedAt(0, size)
      } reduceOption { _ | _ } getOrElse new BitSet

      (row: Int) =>
        bits(row)
    }

    def handleUnfixed(emptyCType: CType, checkNode: CPathNode => Boolean, cols: Map[ColumnRef, Column]) = {
      val objCols = cols filter {
        case (ColumnRef(path, ctpe), _) =>
          val emptyCrit = path == seenPath && ctpe == emptyCType

          lazy val seenPathLength = seenPath.nodes.length
          lazy val nonemptyCrit = {
            if (seenPathLength + 1 <= path.nodes.length) {
              val pathToCompare = path.nodes.take(seenPathLength)
              pathToCompare == seenPath.nodes && checkNode(path.nodes(seenPathLength))
            } else {
              false
            }
          }

          emptyCrit || nonemptyCrit
      }
      val objBits = objCols.values map {
        _.definedAt(0, size)
      } reduceOption { _ | _ } getOrElse new BitSet

      (row: Int) =>
        objBits(row)
    }

    def handleEmpty(emptyCType: CType, cols: Map[ColumnRef, Column]) = {
      val emptyCols = cols filter {
        case (ColumnRef(path, ctpe), _) =>
          path == seenPath && ctpe == emptyCType
      }
      val emptyBits = emptyCols.values map {
        _.definedAt(0, size)
      } reduceOption { _ | _ } getOrElse new BitSet

      (row: Int) =>
        emptyBits(row)
    }

    def combineFixedResults(results: Seq[Int => Boolean]): Int => Boolean = { (row: Int) =>
      results.foldLeft(true) { case (bool, fcn) => bool && fcn(row) }
    }

    jtpe match {
      case JNumberT  => handleRoot(Seq(CDouble, CLong, CNum), cols)
      case JBooleanT => handleRoot(Seq(CBoolean), cols)
      case JTextT    => handleRoot(Seq(CString), cols)
      case JNullT    => handleRoot(Seq(CNull), cols)

      case JOffsetDateTimeT => handleRoot(Seq(COffsetDateTime), cols)
      case JOffsetTimeT     => handleRoot(Seq(COffsetTime), cols)
      case JOffsetDateT     => handleRoot(Seq(COffsetDate), cols)
      case JLocalDateTimeT  => handleRoot(Seq(CLocalDateTime), cols)
      case JLocalTimeT      => handleRoot(Seq(CLocalTime), cols)
      case JLocalDateT      => handleRoot(Seq(CLocalDate), cols)
      case JIntervalT       => handleRoot(Seq(CInterval), cols)

      case JObjectUnfixedT => handleUnfixed(CEmptyObject, _.isInstanceOf[CPathField], cols)
      case JArrayUnfixedT  => handleUnfixed(CEmptyArray, _.isInstanceOf[CPathIndex], cols)

      case JObjectFixedT(fields) =>
        if (fields.isEmpty) {
          handleEmpty(CEmptyObject, cols)
        } else {
          val results: Seq[Int => Boolean] = fields.toSeq map {
            case (field, tpe) =>
              val seenPath0 = CPath(seenPath.nodes :+ CPathField(field))
              findTypes(tpe, seenPath0, cols, size)
          }

          combineFixedResults(results)
        }

      case JArrayFixedT(elements) =>
        if (elements.isEmpty) {
          handleEmpty(CEmptyArray, cols)
        } else {
          val results: Seq[Int => Boolean] = elements.toSeq map {
            case (idx, tpe) =>
              val seenPath0 = CPath(seenPath.nodes :+ CPathIndex(idx))
              findTypes(tpe, seenPath0, cols, size)
          }

          combineFixedResults(results)
        }

      case JUnionT(left, right) =>
        val leftTypes  = findTypes(left, seenPath, cols, size)
        val rightTypes = findTypes(right, seenPath, cols, size)

        (row: Int) =>
          leftTypes(row) || rightTypes(row)

        case JArrayHomogeneousT(jtpe) =>
        findTypes(jtpe, CPath(seenPath.nodes :+ CPathArray), cols, size)
    }
  }

  /**
    * Constructs a JType corresponding to the supplied sequence of ColumnRefs. Returns None if the
    * supplied sequence is empty.
    */
  def mkType(ctpes: Seq[ColumnRef]): Option[JType] = {

    val primitives = ctpes flatMap {
      case ColumnRef(CPath.Identity, t: CValueType[_]) => fromCValueType(t)
      case ColumnRef(CPath.Identity, CNull)            => Some(JNullT)
      case ColumnRef(CPath.Identity, CEmptyArray)      => Some(JArrayFixedT(Map()))
      case ColumnRef(CPath.Identity, CEmptyObject)     => Some(JObjectFixedT(Map()))
      case _                                           => None
    }

    val elements = ctpes.collect {
      case ColumnRef(CPath(CPathIndex(i), _ *), _) => i
    }.toSet.flatMap { (i: Int) =>
      mkType(ctpes.collect {
        case ColumnRef(CPath(CPathIndex(`i`), tail @ _ *), ctpe) => ColumnRef(CPath(tail: _*), ctpe)
      }).map(i -> _)
    }
    val array = if (elements.isEmpty) Nil else List(JArrayFixedT(elements.toMap))

    val keys = ctpes.foldLeft(Set.empty[String]) {
      case (acc, ColumnRef(CPath(CPathField(key), _ *), _)) => acc + key
      case (acc, _)                                         => acc
    }

    val members = keys.flatMap { key =>
      mkType(ctpes.collect {
        case ColumnRef(CPath(CPathField(`key`), tail @ _ *), ctpe) => ColumnRef(CPath(tail: _*), ctpe)
      }).map(key -> _)
    }
    val obj = if (members.isEmpty) Nil else List(JObjectFixedT(members.toMap))

    (primitives ++ array ++ obj).reduceOption(JUnionT)
  }

  /**
    * This is a less-strict version of `includes`. Instead of returning true iff
    * the `(CPath, CType)` is included in the `JType`, it returns `true` if the
    * `(CPath, CType)` pair may be required to satisify some requirement of the
    * `JType`, even if the `(CPath, CType)` may contain more than necessary (eg.
    * in the case of homogeneous arrays when only need a few elements).
    */
  def requiredBy(jtpe: JType, path: CPath, ctpe: CType): Boolean =
    includes(jtpe, path, ctpe) || ((jtpe, path, ctpe) match {
          case (JArrayFixedT(elements), CPath(CPathArray, tail @ _ *), CArrayType(elemType)) =>
            elements.values exists (requiredBy(_, CPath(tail: _*), elemType))
          case _ => false
        })

  /**
    * Tests whether the supplied JType includes the supplied CPath and CType.
    *
    * This is strict, so a JArrayFixedT(_) cannot include a CPathArray/CArrayType(_).
    */
  def includes(jtpe: JType, path: CPath, ctpe: CType): Boolean = (jtpe, (path, ctpe)) match {
    case (JNumberT, (CPath.Identity, CLong | CDouble | CNum)) => true

    case (JTextT, (CPath.Identity, CString)) => true

    case (JBooleanT, (CPath.Identity, CBoolean)) => true

    case (JNullT, (CPath.Identity, CNull)) => true

    case (JOffsetDateTimeT, (CPath.Identity, COffsetDateTime)) => true
    case (JOffsetTimeT, (CPath.Identity, COffsetTime))         => true
    case (JOffsetDateT, (CPath.Identity, COffsetDate))         => true
    case (JLocalDateTimeT, (CPath.Identity, CLocalDateTime))   => true
    case (JLocalTimeT, (CPath.Identity, CLocalTime))           => true
    case (JLocalDateT, (CPath.Identity, CLocalDate))           => true
    case (JIntervalT,  (CPath.Identity, CInterval))            => true

    case (JObjectUnfixedT, (CPath.Identity, CEmptyObject))                         => true
    case (JObjectUnfixedT, (CPath(CPathField(_), _ *), _))                         => true
    case (JObjectFixedT(fields), (CPath.Identity, CEmptyObject)) if fields.isEmpty => true

    case (JObjectFixedT(fields), (CPath(CPathField(head), tail @ _ *), ctpe)) => {
      fields.get(head).map(includes(_, CPath(tail: _*), ctpe)).getOrElse(false)
    }

    case (JArrayUnfixedT, (CPath.Identity, CEmptyArray))                             => true
    case (JArrayUnfixedT, (CPath(CPathArray, _ *), CArrayType(_)))                   => true
    case (JArrayUnfixedT, (CPath(CPathIndex(_), _ *), _))                            => true
    case (JArrayFixedT(elements), (CPath.Identity, CEmptyArray)) if elements.isEmpty => true
    case (JArrayFixedT(elements), (CPath(CPathIndex(i), tail @ _ *), ctpe)) =>
      elements.get(i).map(includes(_, CPath(tail: _*), ctpe)).getOrElse(false)
    case (JArrayHomogeneousT(jElemType), (CPath(CPathArray, _ *), CArrayType(cElemType))) =>
      fromCValueType(cElemType) == Some(jElemType)

    // TODO This is a bit contentious, as this situation will need to be dealt
    // with at a higher level if we let parts of a heterogeneous array fall
    // through, posing as a homogeneous array. Especially since, eg, someone
    // should be expecting that if a[1] exists, therefore a[0] exists.
    case (JArrayHomogeneousT(jElemType), (CPath(CPathIndex(i), tail @ _ *), ctpe)) =>
      ctypes(jElemType) contains ctpe

    case (JUnionT(ljtpe, rjtpe), (path, ctpe)) => includes(ljtpe, path, ctpe) || includes(rjtpe, path, ctpe)

    case _ => false
  }

  /**
    * Tests whether the supplied sequence contains all the (CPath, CType) pairs that are
    * included by the supplied JType.
    */
  def subsumes(ctpes: Seq[(CPath, CType)], jtpe: JType): Boolean = jtpe match {
    case JNumberT =>
      ctpes.exists {
        case (CPath.Identity, CLong | CDouble | CNum) => true
        case _                                        => false
      }

    case JTextT => ctpes.contains(CPath.Identity -> CString)

    case JBooleanT => ctpes.contains(CPath.Identity -> CBoolean)

    case JNullT => ctpes.contains(CPath.Identity -> CNull)

    case JOffsetDateTimeT => ctpes.contains(CPath.Identity -> COffsetDateTime)
    case JOffsetTimeT     => ctpes.contains(CPath.Identity -> COffsetTime)
    case JOffsetDateT     => ctpes.contains(CPath.Identity -> COffsetDate)
    case JLocalDateTimeT  => ctpes.contains(CPath.Identity -> CLocalDateTime)
    case JLocalTimeT      => ctpes.contains(CPath.Identity -> CLocalTime)
    case JLocalDateT      => ctpes.contains(CPath.Identity -> CLocalDate)
    case JIntervalT       => ctpes.contains(CPath.Identity -> CInterval)

    case JObjectUnfixedT if ctpes.contains(CPath.Identity -> CEmptyObject) => true
    case JObjectUnfixedT =>
      ctpes.exists {
        case (CPath(CPathField(_), _ *), _) => true
        case _                              => false
      }
    case JObjectFixedT(fields) if fields.isEmpty => ctpes.contains(CPath.Identity -> CEmptyObject)
    case JObjectFixedT(fields) => {
      val keys = fields.keySet
      keys.forall { key =>
        subsumes(ctpes.collect { case (CPath(CPathField(`key`), tail @ _ *), ctpe) => (CPath(tail: _*), ctpe) }, fields(key))
      }
    }

    case JArrayUnfixedT if ctpes.contains(CPath.Identity -> CEmptyArray) => true
    case JArrayUnfixedT =>
      ctpes.exists {
        case (CPath(CPathArray, _ *), _)    => true
        case (CPath(CPathIndex(_), _ *), _) => true
        case _                              => false
      }
    case JArrayFixedT(elements) if elements.isEmpty => ctpes.contains(CPath.Identity -> CEmptyArray)
    case JArrayFixedT(elements) => {
      val indices = elements.keySet
      indices.forall { i =>
        subsumes(ctpes.collect {
          case (CPath(CPathArray, tail @ _ *), CArrayType(elemType)) => (CPath(tail: _*), elemType)
          case (CPath(CPathIndex(`i`), tail @ _ *), ctpe)            => (CPath(tail: _*), ctpe)
        }, elements(i))
      }
    }
    case JArrayHomogeneousT(jElemType) =>
      ctpes.exists {
        case (CPath(CPathArray, _ *), CArrayType(cElemType)) =>
          ctypes(jElemType) contains cElemType
        case _ => false
      }

    case JUnionT(ljtpe, rjtpe) => subsumes(ctpes, ljtpe) || subsumes(ctpes, rjtpe)

    case _ => false
  }
}
