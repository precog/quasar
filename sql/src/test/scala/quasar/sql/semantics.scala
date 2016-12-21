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

import matryoshka._
import matryoshka.implicits._
import pathy.Path._
import scalaz._, Scalaz._

class SemanticsSpec extends quasar.Qspec with TreeMatchers {

  "TransformSelect" should {
    val compiler = Compiler.trampoline
    val asc: OrderType = ASC

    def transform[T]
      (q: T)
      (implicit TR: Recursive.Aux[T, Sql], TC: Corecursive.Aux[T, Sql])
        : T =
      q.transCata[T](orOriginal(projectSortKeysƒ))

    "add single field for order by" in {
      val q = SelectR(SelectAll,
                     Proj(IdentR("name"), None) :: Nil,
                     Some(TableRelationAST(file("person"), None)),
                     None,
                     None,
                     Some(OrderBy((asc, IdentR("height")).wrapNel)))
      transform(q) must beTree(
               SelectR(SelectAll,
                      Proj(IdentR("name"), None) :: Proj(IdentR("height"), Some("__sd__0")) :: Nil,
                      Some(TableRelationAST(file("person"), None)),
                      None,
                      None,
                      Some(OrderBy((asc, IdentR("__sd__0")).wrapNel)))
               )
    }

    "not add a field that appears in the projections" in {
      val q = SelectR(SelectAll,
                     Proj(IdentR("name"), None) :: Nil,
                     Some(TableRelationAST(file("person"), None)),
                     None,
                     None,
                     Some(OrderBy((asc, IdentR("name")).wrapNel)))
      transform(q) must beTree(q)
    }

    "not add a field that appears as an alias in the projections" in {
      val q = SelectR(SelectAll,
                     Proj(IdentR("foo"), Some("name")) :: Nil,
                     Some(TableRelationAST(file("person"), None)),
                     None,
                     None,
                     Some(OrderBy((asc, IdentR("name")).wrapNel)))
      transform(q) must beTree(q)
    }

    "not add a field with wildcard present" in {
      val q = SelectR(SelectAll,
                     Proj(SpliceR(None), None) :: Nil,
                     Some(TableRelationAST(file("person"), None)),
                     None,
                     None,
                     Some(OrderBy((asc, IdentR("height")).wrapNel)))
      transform(q) must beTree(q)
    }

    "add single field for order by" in {
      val q = SelectR(SelectAll,
                     Proj(IdentR("name"), None) :: Nil,
                     Some(TableRelationAST(file("person"), None)),
                     None,
                     None,
                     Some(OrderBy(NonEmptyList(
                       (asc, IdentR("height")),
                       (asc, IdentR("name"))))))
      transform(q) must beTree(
               SelectR(SelectAll,
                      Proj(IdentR("name"), None) ::
                        Proj(IdentR("height"), Some("__sd__0")) ::
                        Nil,
                      Some(TableRelationAST(file("person"), None)),
                      None,
                      None,
                      Some(OrderBy(NonEmptyList(
                        (asc, IdentR("__sd__0")),
                        (asc, IdentR("name")))))))
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
                                Some(OrderBy((asc, IdentR("b")).wrapNel))),
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
                                Some(OrderBy((asc, IdentR("__sd__0")).wrapNel))),
                         In)),
                     None,
                     None))
    }

  }
}
