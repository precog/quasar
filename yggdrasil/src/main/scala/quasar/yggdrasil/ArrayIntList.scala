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

class ArrayIntList(initialCapacity: Int) {
  private[this] var _size: Int        = 0
  private[this] var _data: Array[Int] = new Array[Int](initialCapacity)

  def this()             = this(8)
  def size(): Int        = _size
  def get(row: Int): Int = _data(row)
  def toArray(): Array[Int] = {
    val arr = new Array[Int](size)
    System.arraycopy(_data, 0, arr, 0, size)
    arr
  }
  def isEmpty: Boolean                    = size == 0
  def add(index: Int, element: Int): Unit = {
    checkRangeIncludingEndpoint(index)
    ensureCapacity(_size + 1)
    val numtomove = _size - index
    System.arraycopy(_data, index, _data, index+1, numtomove)
    _data(index) = element
    _size += 1
  }
  def add(element: Int): Boolean = {
    add(size(), element)
    true
  }
  private def checkRangeIncludingEndpoint(index: Int): Unit = {
    if (index < 0 || index > _size)
      throw new IndexOutOfBoundsException(s"Should be at least 0 and at most ${_size}, found $index")
  }
  def ensureCapacity(mincap: Int): Unit = {
    if (mincap > _data.length) {
      val newcap = (_data.length * 3) / 2 + 1
      val olddata = _data
      val newlen = math.max(mincap, newcap)
      _data = new Array[Int](newlen)
      System.arraycopy(olddata, 0, _data, 0, _size)
    }
  }
}
