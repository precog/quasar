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

package quasar.sql

import quasar.Predef._
import quasar.TreeMatchers
import quasar.sql.SemanticAnalysis._
import quasar.sql.fixpoint._

import matryoshka._, FunctorT.ops._
import pathy.Path._

class SemanticsSpec extends quasar.Qspec with TreeMatchers {

  "TransformSelect" should {
    val compiler = Compiler.trampoline

    def transform[T[_[_]]: Recursive: Corecursive](q: T[Sql]): T[Sql] =
      q.transCata(orOriginal(projectSortKeysƒ))

    "add single field for order by" in {
      val q = SelectR(SelectAll,
                     Proj(IdentR("name"), None) :: Nil,
                     Some(TableRelationAST(file("person"), None)),
                     None,
                     None,
                     Some(OrderBy((ASC, IdentR("height")) :: Nil)))
      transform(q) must beTree(
               SelectR(SelectAll,
                      Proj(IdentR("name"), None) :: Proj(IdentR("height"), Some("__sd__0")) :: Nil,
                      Some(TableRelationAST(file("person"), None)),
                      None,
                      None,
                      Some(OrderBy((ASC, IdentR("__sd__0")) :: Nil)))
               )
    }

    "not add a field that appears in the projections" in {
      val q = SelectR(SelectAll,
                     Proj(IdentR("name"), None) :: Nil,
                     Some(TableRelationAST(file("person"), None)),
                     None,
                     None,
                     Some(OrderBy((ASC, IdentR("name")) :: Nil)))
      transform(q) must beTree(q)
    }

    "not add a field that appears as an alias in the projections" in {
      val q = SelectR(SelectAll,
                     Proj(IdentR("foo"), Some("name")) :: Nil,
                     Some(TableRelationAST(file("person"), None)),
                     None,
                     None,
                     Some(OrderBy((ASC, IdentR("name")) :: Nil)))
      transform(q) must beTree(q)
    }

    "not add a field with wildcard present" in {
      val q = SelectR(SelectAll,
                     Proj(SpliceR(None), None) :: Nil,
                     Some(TableRelationAST(file("person"), None)),
                     None,
                     None,
                     Some(OrderBy((ASC, IdentR("height")) :: Nil)))
      transform(q) must beTree(q)
    }

    "add single field for order by" in {
      val q = SelectR(SelectAll,
                     Proj(IdentR("name"), None) :: Nil,
                     Some(TableRelationAST(file("person"), None)),
                     None,
                     None,
                     Some(OrderBy((ASC, IdentR("height")) ::
                                  (ASC, IdentR("name")) ::
                                  Nil)))
      transform(q) must beTree(
               SelectR(SelectAll,
                      Proj(IdentR("name"), None) ::
                        Proj(IdentR("height"), Some("__sd__0")) ::
                        Nil,
                      Some(TableRelationAST(file("person"), None)),
                      None,
                      None,
                      Some(OrderBy((ASC, IdentR("__sd__0")) ::
                                   (ASC, IdentR("name")) ::
                                   Nil))))
    }

    "transform sub-select" in {
      val q = SelectR(SelectAll,
                     Proj(SpliceR(None), None) :: Nil,
                     Some(TableRelationAST(file("foo"), None)),
                     Some(
                       BinopR(
                         IdentR("a"),
                         SelectR(SelectAll,
                                Proj(IdentR("a"), None) :: Nil,
                                Some(TableRelationAST(file("bar"), None)),
                                None,
                                None,
                                Some(OrderBy((ASC, IdentR("b")) :: Nil))),
                         In)),
                     None,
                     None)
      transform(q) must beTree(
              SelectR(SelectAll,
                     Proj(SpliceR(None), None) :: Nil,
                     Some(TableRelationAST(file("foo"), None)),
                     Some(
                       BinopR(
                         IdentR("a"),
                         SelectR(SelectAll,
                                Proj(IdentR("a"), None) ::
                                  Proj(IdentR("b"), Some("__sd__0")) ::
                                  Nil,
                                Some(TableRelationAST(file("bar"), None)),
                                None,
                                None,
                                Some(OrderBy((ASC, IdentR("__sd__0")) :: Nil))),
                         In)),
                     None,
                     None))
    }

  }
}
