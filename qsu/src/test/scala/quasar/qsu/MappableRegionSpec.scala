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

package quasar.qsu

import slamdata.Predef.{Boolean, String, Symbol}
import quasar.{Qspec, TreeMatchers}
import quasar.contrib.pathy.AFile
import quasar.ejson.{EJson, Fixed}
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.ski.κ
import quasar.qscript.{construction, Hole, MapFuncsCore, SrcHole}

import matryoshka._
import matryoshka.data._
import pathy.Path
import scalaz.syntax.equal._

object MappableRegionSpec extends Qspec with TreeMatchers with QSUTTypes[Fix] {
  import MapFuncsCore._

  type QSU[A] = QScriptUniform[A]

  val ejs = Fixed[Fix[EJson]]
  val QSU = QScriptUniform.Optics[Fix]
  val qsu = QScriptUniform.DslT[Fix]
  val func = construction.Func[Fix]
  val recFunc = construction.RecFunc[Fix]
  val hole = (SrcHole: Hole)

  val orders: AFile = Path.rootDir </> Path.dir("client") </> Path.file("orders")

  def mr(tree: Fix[QSU]): FreeMapA[QSUGraph] =
    MappableRegion.maximal(QSUGraph.fromTree(tree))

  def mr1(tree: Fix[QSU]): FreeMap =
    mr(tree) map κ(hole)

  def projectStrKey(key: String): FreeMap =
    func.ProjectKeyS(func.Hole, key)

  def recProjectStrKey(key: String): RecFreeMap =
    recFunc.ProjectKeyS(recFunc.Hole, key)

  "finding mappable region" should {
    "convert autojoin2" >> {
      val tree =
        qsu.autojoin2((
          qsu.map(qsu.read(orders), recProjectStrKey("foo")),
          qsu.map(qsu.read(orders), recProjectStrKey("bar")),
          _(ConcatMaps(_, _))))

      val exp =
        func.ConcatMaps(projectStrKey("foo"), projectStrKey("bar"))

      mr1(tree) must beTreeEqual(exp)
    }

    "convert autojoin3" >> {
      val tree =
        qsu.autojoin3((
          qsu.autojoin2((
            qsu.map(qsu.read(orders), recProjectStrKey("baz")),
            qsu.cint(42),
            _(Gt(_, _)))),
          qsu.map(qsu.read(orders), recProjectStrKey("quux")),
          qsu.cdec(32.56),
          _(Cond(_, _, _))))

      val exp =
        func.Cond(
          func.Gt(projectStrKey("baz"), func.Constant(ejs.int(42))),
          projectStrKey("quux"),
          func.Constant(ejs.dec(32.56)))

      mr1(tree) must beTreeEqual(exp)
    }

    "convert map" >> {
      val tree =
        qsu.map(
          qsu.autojoin2((
            qsu.read(orders),
            qsu.cstr("foo"),
            _(ProjectKey(_, _)))),
          recFunc.MakeArray(recFunc.Hole))

      val exp =
        func.MakeArray(projectStrKey("foo"))

      mr1(tree) must beTreeEqual(exp)
    }

    "stop at non-mapping nodes" >> {
      val tree =
        qsu.autojoin2((
          qsu.lpFilter(
            qsu.read(orders),
            qsu.map(qsu.read(orders), recProjectStrKey("isOpen"))),
          qsu.map(qsu.read(orders), recProjectStrKey("clientName")),
          _(MakeMap(_, _))))

      val exp =
        func.MakeMap(func.Hole, projectStrKey("clientName"))

      mr1(tree) must beTreeEqual(exp)
    }

    "halt when predicate returns true" >> {
      val prjIdx =
        recFunc.ProjectIndex(recFunc.Hole, recFunc.Constant(ejs.int(3)))

      val tree =
        qsu.autojoin2((
          qsu.map(
            qsu.lpFilter(
              qsu.read(orders),
              qsu.map(qsu.read(orders), recProjectStrKey("isClosed"))),
            prjIdx),
          qsu.cstr("closed"),
          _(MakeMap(_, _))))

      val exp =
        func.MakeMap(func.Hole, func.Constant(ejs.str("closed")))

      val graph =
        QSUGraph.fromTree(tree)

      val pred: Symbol => Boolean =
        s => graph.vertices.get(s).exists {
          case QScriptUniform.Map(_, f) => f ≟ prjIdx
          case _ => false
        }

      (MappableRegion(pred, graph) map κ(hole)) must beTreeEqual(exp)
    }
  }
}
