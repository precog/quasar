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

package quasar.sql

import slamdata.Predef._
import quasar.TreeMatchers
import quasar.sql.fixpoint._

import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._

class SemanticsSpec extends quasar.Qspec with TreeMatchers {

  val asc: OrderType = ASC

  "normalize projections" should {
    "remove projection from filter when table exists" in  {
      val query = SelectR(SelectAll,
        Proj(IdentR("name"), None) :: Nil,
        Some(TableRelationAST(file("person"), None)),
        Some(BinopR(IdentR("person"), StringLiteralR("age"), FieldDeref)),
        None,
        None)

      SemanticAnalysis.normalizeProjections(query) must beTree(
        SelectR(SelectAll,
          Proj(IdentR("name"), None) :: Nil,
          Some(TableRelationAST(file("person"), None)),
          Some(IdentR("age")),
          None,
          None))
    }

    "remove projection from group by that already exists" in  {
      val query = SelectR(SelectAll,
        Proj(IdentR("name"), None) :: Nil,
        Some(TableRelationAST(file("person"), None)),
        Some(BinopR(IdentR("person"), StringLiteralR("age"), FieldDeref)),
        Some(GroupBy(
          BinopR(IdentR("person"), StringLiteralR("height"), FieldDeref) :: Nil,
          Some(BinopR(IdentR("person"), StringLiteralR("initials"), FieldDeref)))),
        None)

      SemanticAnalysis.normalizeProjections(query) must beTree(
        SelectR(SelectAll,
          Proj(IdentR("name"), None) :: Nil,
          Some(TableRelationAST(file("person"), None)),
          Some(IdentR("age")),
          Some(GroupBy(
            IdentR("height") :: Nil,
            Some(IdentR("initials")))),
          None))
    }

    "remove projection from order by that already exists" in  {
      val query = SelectR(SelectDistinct,
        Proj(IdentR("name"), None) :: Nil,
        Some(TableRelationAST(file("person"), None)),
        Some(BinopR(IdentR("person"), StringLiteralR("age"), FieldDeref)),
        Some(GroupBy(BinopR(IdentR("person"), StringLiteralR("height"), FieldDeref) :: Nil, None)),
        Some(OrderBy((asc, BinopR(IdentR("person"), StringLiteralR("shoe size"), FieldDeref)).wrapNel)))

      SemanticAnalysis.normalizeProjections(query) must beTree(
        SelectR(SelectDistinct,
          Proj(IdentR("name"), None) :: Nil,
          Some(TableRelationAST(file("person"), None)),
          Some(IdentR("age")),
          Some(GroupBy(IdentR("height") :: Nil, None)),
          Some(OrderBy((asc, IdentR("shoe size")).wrapNel))))
    }

    "not remove projection that doesn't exist" in  {
      val query = SelectR(SelectAll,
        Proj(IdentR("name"), None) :: Nil,
        Some(TableRelationAST(file("person"), None)),
        Some(BinopR(IdentR("animal"), StringLiteralR("name"), FieldDeref)),
        None,
        None)

      SemanticAnalysis.normalizeProjections(query) must beTree(query)
    }

    "remove projection using table alias" in  {
      val query = SelectR(SelectAll,
        Proj(IdentR("name"), None) :: Nil,
        Some(TableRelationAST(file("person"), Some("Full Name"))),
        Some(BinopR(IdentR("Full Name"), StringLiteralR("first"), FieldDeref)),
        None,
        None)

      SemanticAnalysis.normalizeProjections(query) must beTree(
        SelectR(SelectAll,
          Proj(IdentR("name"), None) :: Nil,
          Some(TableRelationAST(file("person"), Some("Full Name"))),
          Some(IdentR("first")),
          None,
          None))

    }
  }

  "sort key projection" should {
    "add single field for order by" in {
      val q = SelectR(SelectAll,
                     Proj(IdentR("name"), None) :: Nil,
                     Some(TableRelationAST(file("person"), None)),
                     None,
                     None,
                     Some(OrderBy((asc, IdentR("height")).wrapNel)))
      SemanticAnalysis.projectSortKeys(q) must beTree(
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
      SemanticAnalysis.projectSortKeys(q) must beTree(q)
    }

    "not add a field that appears as an alias in the projections" in {
      val q = SelectR(SelectAll,
                     Proj(IdentR("foo"), Some("name")) :: Nil,
                     Some(TableRelationAST(file("person"), None)),
                     None,
                     None,
                     Some(OrderBy((asc, IdentR("name")).wrapNel)))
      SemanticAnalysis.projectSortKeys(q) must beTree(q)
    }

    "not add a field with wildcard present" in {
      val q = SelectR(SelectAll,
                     Proj(SpliceR(None), None) :: Nil,
                     Some(TableRelationAST(file("person"), None)),
                     None,
                     None,
                     Some(OrderBy((asc, IdentR("height")).wrapNel)))
      SemanticAnalysis.projectSortKeys(q) must beTree(q)
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
      SemanticAnalysis.projectSortKeys(q) must beTree(
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

    "SemanticAnalysis.projectSortKeys sub-select" in {
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
      SemanticAnalysis.projectSortKeys(q) must beTree(
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
