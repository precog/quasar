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

package quasar.niflheim

import quasar.blueeyes.json._
import quasar.precog.BitSet
import quasar.precog.common._

import scala.collection.mutable

case class CTree(path: CPath, fields: mutable.Map[String, CTree], indices: mutable.ArrayBuffer[CTree], types: mutable.Map[CType, Int]) {
  def getField(s: String): CTree = fields.getOrElseUpdate(s, CTree.empty(CPath(path.nodes :+ CPathField(s))))
  def getIndex(n: Int): CTree = {
    var i = indices.length
    while (i <= n) {
      indices.append(CTree.empty(CPath(path.nodes :+ CPathIndex(i))))
      i += 1
    }
    indices(n)
  }
  def getType(ctype: CType): Int = types.getOrElse(ctype, -1)
  def setType(ctype: CType, n: Int): Unit = types(ctype) = n

  override def equals(that: Any): Boolean = that match {
    case CTree(`path`, fields2, indices2, types2) =>
      fields == fields2 && indices == indices2 && types == types2
    case _ =>
      false
  }
}

object CTree {
  def empty(path: CPath) = CTree(path, mutable.Map.empty[String, CTree], mutable.ArrayBuffer.empty[CTree], mutable.Map.empty[CType, Int])
}

object Segments {
  def empty(id: Long): Segments =
    Segments(id, 0, CTree.empty(CPath.Identity), mutable.ArrayBuffer.empty[Segment])
}

case class Segments(id: Long, var length: Int, t: CTree, a: mutable.ArrayBuffer[Segment]) {

  override def equals(that: Any): Boolean = that match {
    case Segments(`id`, length2, t2, a2) =>
      length == length2 && t == t2 && a.toSet == a2.toSet
    case _ =>
      false
  }

  def addNullType(row: Int, tree: CTree, ct: CNullType) {
    val n = tree.getType(ct)
    if (n >= 0) {
      a(n).defined.set(row)
    } else {
      tree.setType(ct, a.length)
      val d = new BitSet()
      d.set(row)
      a.append(NullSegment(id, tree.path, ct, d, length))
    }
  }

  def addNull(row: Int, tree: CTree): Unit = addNullType(row, tree, CNull)

  def addEmptyArray(row: Int, tree: CTree): Unit = addNullType(row, tree, CEmptyArray)

  def addEmptyObject(row: Int, tree: CTree): Unit = addNullType(row, tree, CEmptyObject)

  def addTrue(row: Int, tree: CTree) {
    val n = tree.getType(CBoolean)
    if (n >= 0) {
      val seg = a(n).asInstanceOf[BooleanSegment]
      seg.defined.set(row)
      seg.values.set(row)
    } else {
      tree.setType(CBoolean, a.length)
      val d = new BitSet()
      val v = new BitSet()
      d.set(row)
      v.set(row)
      a.append(BooleanSegment(id, tree.path, d, v, length))
    }
  }

  def addFalse(row: Int, tree: CTree) {
    val n = tree.getType(CBoolean)
    if (n >= 0) {
      a(n).defined.set(row)
    } else {
      tree.setType(CBoolean, a.length)
      val d = new BitSet()
      val v = new BitSet()
      d.set(row)
      a.append(BooleanSegment(id, tree.path, d, v, length))
    }
  }

  def addString(row: Int, tree: CTree, s: String) {
    val n = tree.getType(CString)
    if (n >= 0) {
      val seg = a(n).asInstanceOf[ArraySegment[String]]
      seg.defined.set(row)
      seg.values(row) = s
    } else {
      tree.setType(CString, a.length)
      val d = new BitSet()
      d.set(row)
      val v = new Array[String](length)
      v(row) = s
      a.append(ArraySegment[String](id, tree.path, CString, d, v))
    }
  }

  def addLong(row: Int, tree: CTree, x: Long) {
    val n = tree.getType(CLong)
    if (n >= 0) {
      val seg = a(n).asInstanceOf[ArraySegment[Long]]
      seg.defined.set(row)
      seg.values(row) = x
    } else {
      tree.setType(CLong, a.length)
      val d = new BitSet()
      d.set(row)
      val v = new Array[Long](length)
      v(row) = x
      a.append(ArraySegment[Long](id, tree.path, CLong, d, v))
    }
  }

  def addDouble(row: Int, tree: CTree, x: Double) {
    val n = tree.getType(CDouble)
    if (n >= 0) {
      val seg = a(n).asInstanceOf[ArraySegment[Double]]
      seg.defined.set(row)
      seg.values(row) = x
    } else {
      tree.setType(CDouble, a.length)
      val d = new BitSet()
      d.set(row)
      val v = new Array[Double](length)
      v(row) = x
      a.append(ArraySegment[Double](id, tree.path, CDouble, d, v))
    }
  }

  def addBigDecimal(row: Int, tree: CTree, x: BigDecimal) {
    val j = x.toLong
    if (false && BigDecimal(j) == x) {
      addLong(row, tree, j)
    } else {
      val n = tree.getType(CNum)

      if (n >= 0) {
        val seg = a(n).asInstanceOf[ArraySegment[BigDecimal]]
        seg.defined.set(row)
        seg.values(row) = x
      } else {
        tree.setType(CNum, a.length)
        val d = new BitSet()
        d.set(row)
        val v = new Array[BigDecimal](length)
        v(row) = x
        a.append(ArraySegment[BigDecimal](id, tree.path, CNum, d, v))
      }
    }
  }

  // TODO: more principled number handling
  def addNum(row: Int, tree: CTree, s: String): Unit = {
    addBigDecimal(row, tree, BigDecimal(s))
  }

  def extendWithRows(rows: Seq[JValue]) {
    var i = 0

    val rlen = rows.length

    val alen = a.length
    while (i < alen) {
      a(i) = a(i).extend(rlen)
      i += 1
    }

    i = length
    length += rlen

    rows.foreach { j =>
      initializeSegments(i, j, t)
      i += 1
    }
  }

  def initializeSegments(row: Int, j: JValue, tree: CTree): Unit = {
    j match {
      case JNull => addNull(row, tree)
      case JTrue => addTrue(row, tree)
      case JFalse => addFalse(row, tree)

      case JString(s) => addString(row, tree, s)
      case JNumLong(n) => addLong(row, tree, n)
      case JNumDouble(n) => addDouble(row, tree, n)
      case JNumBigDec(n) => addBigDecimal(row, tree, n)
      case JNumStr(s) => addNum(row, tree, s)

      case JObject(m) =>
        if (m.isEmpty) {
          addEmptyObject(row, tree)
        } else {
          m.foreach {
            case (key, j) =>
              initializeSegments(row, j, tree.getField(key))
          }
        }

      case JArray(js) =>
        if (js.isEmpty) {
          addEmptyArray(row, tree)
        } else {
          var i = 0
          js.foreach { j =>
            initializeSegments(row, j, tree.getIndex(i))
            i += 1
          }
        }

      case JUndefined => ()
    }
  }
}
