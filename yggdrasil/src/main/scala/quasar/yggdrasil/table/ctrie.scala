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

import quasar.contrib.radixtree.ListKey._
import quasar.contrib.std.errorNotImplemented
import quasar.precog.common._

import com.rklaehn.radixtree.RadixTree
import shims._

object ctrie {

  implicit def toCMap[V](r: RadixTree[List[CPathNode], Map[CType, V]]): Map[ColumnRef, V] =
    new ToCMap[V](r)

  class ToCMap[+V](r: RadixTree[List[CPathNode], Map[CType, V]]) extends Map[ColumnRef, V] {
    def this() = this(RadixTree.empty[List[CPathNode], Map[CType, V]])

    def +[V1 >: V](kv: (ColumnRef, V1)): Map[ColumnRef,V1] = {
      toCMap[V1](r.asInstanceOf[RadixTree[List[CPathNode], Map[CType, V1]]]
        .mergeWith(
          RadixTree(kv._1.selector.nodes -> Map[CType, V1](kv._1.ctype -> kv._2)),
          { case (v1, v2) => v1 ++ v2 }))
    }

    def -(key: ColumnRef): scala.collection.immutable.Map[ColumnRef, V] =
      errorNotImplemented

    def get(key: ColumnRef): Option[V] = {
      r.get(key.selector.nodes).flatMap(_.get(key.ctype))
    }

    def iterator: Iterator[(ColumnRef, V)] = {

      def toIterator(pathNodes: List[CPathNode], map: Map[CType, V]): Iterator[(ColumnRef, V)] =
        map.toIterator.map { case (tp, c) => (ColumnRef(CPath(pathNodes), tp), c) }

      r.entries.map(e => toIterator(e._1, e._2)).flatten.toIterator
    }
  }

  def apply[K: RadixTree.Key, V](kvs: (K, V)*) = RadixTree(kvs: _*)

  type CTrie = RadixTree[ColumnRef, Column]

  object CTrie {
    def emptyCol: Map[ColumnRef, Column] = empty[Column]
    def empty[V]: Map[ColumnRef, V] = toCMap(RadixTree.empty[List[CPathNode], Map[CType, V]])

    def apply(kvs: (ColumnRef, Column)*): Map[ColumnRef, Column] = {
      val l: Seq[(List[CPathNode], CType, Column)] =
        kvs.map(p => (p._1.selector.nodes, p._1.ctype, p._2))
      val m: Map[List[CPathNode], Map[CType, Column]] =
        l.groupBy(_._1).mapValues(ts => ts.map(t => (t._2, t._3)).toMap)
      toCMap(RadixTree(m.toList :_*))
    }

    def mk[V](kvs: (ColumnRef, V)*): Map[ColumnRef, V] = {
      val l: Seq[(List[CPathNode], CType, V)] =
        kvs.map(p => (p._1.selector.nodes, p._1.ctype, p._2))
      val m: Map[List[CPathNode], Map[CType, V]] =
        l.groupBy(_._1).mapValues(ts => ts.map(t => (t._2, t._3)).toMap)
      toCMap(RadixTree(m.toList :_*))
    }
  }

  // temp. function to check that we don't have a HashMap impl anymore
  def assertNoHashMap(m: Map[_, _]) =
    if (m.isInstanceOf[scala.collection.immutable.HashMap[_, _]]) {
      throw new java.lang.RuntimeException(s"$m is a HashMap")
    }

}
