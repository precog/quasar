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
import quasar.precog.common.accounts._
import quasar.yggdrasil._
import quasar.yggdrasil.execution.EvaluationContext
import quasar.yggdrasil.table._
import quasar.yggdrasil.vfs._

import quasar.blueeyes._, json._
import scalaz._, Scalaz._
import quasar.precog.TestSupport._

trait FSLibSpecs extends Specification with FSLibModule[Need] with TestColumnarTableModule[Need] {
  implicit def M = Need.need

  import trans._
  import constants._

  val library = new FSLib {}
  import library._

  lazy val projectionMetadata: Map[Path, Map[ColumnRef, Long]] = Map(
    Path("/foo/bar1/baz/quux1")   -> Map(ColumnRef(CPath.Identity, CString) -> 10L),
    Path("/foo/bar2/baz/quux1")   -> Map(ColumnRef(CPath.Identity, CString) -> 20L),
    Path("/foo/bar2/baz/quux2")   -> Map(ColumnRef(CPath.Identity, CString) -> 30L),
    Path("/foo2/bar1/baz/quux1" ) -> Map(ColumnRef(CPath.Identity, CString) -> 40L)
  )

  val vfs = new StubVFSMetadata[Need](projectionMetadata)

  def pathTable(path: String) = {
    Table.constString(Set(path)).transform(WrapObject(Leaf(Source), TransSpecModule.paths.Value.name))
  }

  val testAPIKey = "testAPIKey"
  def testAccount = AccountDetails("00001", "test@email.com", dateTime.now, "testAPIKey", Path.Root, AccountPlan.Free)
  val defaultEvaluationContext = EvaluationContext(testAPIKey, testAccount, Path.Root, Path.Root, dateTime.now)
  val defaultMorphContext = MorphContext(defaultEvaluationContext, new MorphLogger {
    def info(msg: String): Need[Unit]  = M.point(())
    def warn(msg: String): Need[Unit]  = M.point(())
    def error(msg: String): Need[Unit] = M.point(())
    def die(): Need[Unit]              = M.point(sys.error("MorphContext#die()"))
  })

  def runExpansion(table: Table): List[JValue] = {
    expandGlob(table, defaultMorphContext).map(_.transform(SourceValue.Single)).flatMap(_.toJson).copoint.toList
  }

  "path globbing" should {
    "not alter un-globbed paths" in {
      val table = pathTable("/foo/bar/baz/")
      val expected: List[JValue] = List(JString("/foo/bar/baz/"))
      runExpansion(table) must_== expected
    }

    "not alter un-globbed relative paths" in {
      val table = pathTable("foo")
      val expected: List[JValue] = List(JString("/foo/"))
      runExpansion(table) mustEqual expected
    }

    "expand a leading glob" in {
      val table = pathTable("/*/bar1")
      val expected: List[JValue] = List(JString("/foo/bar1/"), JString("/foo2/bar1/"))
      runExpansion(table) must_== expected
    }

    "expand a trailing glob" in {
      val table = pathTable("/foo/*")
      val expected: List[JValue] = List(JString("/foo/bar1/"), JString("/foo/bar2/"))
      runExpansion(table) must_== expected
    }

    "expand an internal glob and filter" in {
      val table = pathTable("/foo/*/baz/quux1")
      val expected: List[JValue] = List(JString("/foo/bar1/baz/quux1/"), JString("/foo/bar2/baz/quux1/"))
      runExpansion(table) must_== expected
    }

    "expand multiple globbed segments" in {
      val table = pathTable("/foo/*/baz/*")
      val expected: List[JValue] = List(JString("/foo/bar1/baz/quux1/"), JString("/foo/bar2/baz/quux1/"), JString("/foo/bar2/baz/quux2/"))
      runExpansion(table) must_== expected
    }
  }
}

object FSLibSpecs extends FSLibSpecs
