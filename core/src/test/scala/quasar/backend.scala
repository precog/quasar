/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar

import quasar.Predef._

import org.specs2.mutable._
import org.specs2.scalaz._

class BackendSpecs extends Specification with DisjunctionMatchers {
  import quasar.sql._

  "interpretPaths" should {
    import quasar.fs.{Path}

    "make simple table name relative to base path" in {
      val q = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(TableRelationAST("bar", None)),
        None, None, None)
      val basePath = Path("foo/")
      val exp = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(TableRelationAST("./foo/bar", None)),
        None, None, None)

      relativizePaths(q, basePath) must beRightDisjunction(exp)
    }

    "make sub-query table names relative to base path" in {
      val q = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(ExprRelationAST(
          Select(SelectAll,
            Proj(Splice(None), None) :: Nil,
            Some(TableRelationAST("bar", None)),
            None, None, None), "t")),
        None, None, None)
      val basePath = Path("foo/")
      val exp = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(ExprRelationAST(
          Select(SelectAll,
            Proj(Splice(None), None) :: Nil,
            Some(TableRelationAST("./foo/bar", None)),
            None, None, None), "t")),
        None, None, None)

      relativizePaths(q, basePath) must beRightDisjunction(exp)
    }

    "make join table names relative to base path" in {
      val q = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(JoinRelation(
          TableRelationAST("bar", None),
          TableRelationAST("baz", None),
          LeftJoin,
          Ident("id")
        )),
        None, None, None)
      val basePath = Path("foo/")
      val exp = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(JoinRelation(
          TableRelationAST("./foo/bar", None),
          TableRelationAST("./foo/baz", None),
          LeftJoin,
          Ident("id")
        )),
        None, None, None)

      relativizePaths(q, basePath) must beRightDisjunction(exp)
    }

    "make cross table names relative to base path" in {
      val q = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(CrossRelation(
          TableRelationAST("bar", None),
          TableRelationAST("baz", None))),
        None, None, None)
      val basePath = Path("foo/")
      val exp = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(CrossRelation(
          TableRelationAST("./foo/bar", None),
          TableRelationAST("./foo/baz", None))),
        None, None, None)

      relativizePaths(q, basePath) must beRightDisjunction(exp)
    }

    "make sub-select table names relative to base path" in {
      val q = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(TableRelationAST("bar", None)),
        Some(Binop(
          Ident("widgetId"),
          Select(SelectAll,
            Proj(Ident("id"), None) :: Nil,
            Some(TableRelationAST("widget", None)),
            None, None, None),
          In)),
        None, None)
      val basePath = Path("foo/")
      val exp = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(TableRelationAST("./foo/bar", None)),
        Some(Binop(
          Ident("widgetId"),
          Select(SelectAll,
            Proj(Ident("id"), None) :: Nil,
            Some(TableRelationAST("./foo/widget", None)),
            None, None, None),
          In)),
        None, None)

      relativizePaths(q, basePath) must beRightDisjunction(exp)
    }
  }

}
