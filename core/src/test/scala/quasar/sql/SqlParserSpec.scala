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
import quasar.RenderTree.ops._
import quasar.fp._
import quasar.specs2._

import org.specs2.mutable._
import org.specs2.ScalaCheck
import scalaz._, Scalaz._

class SQLParserSpec extends Specification with ScalaCheck with DisjunctionMatchers {
  import SqlQueries._, ExprArbitrary._

  implicit def stringToQuery(s: String): Query = Query(s)

  val parser = new SQLParser

  "SQLParser" should {
    "parse query1" in {
      val r = parser.parse(q1).toOption
      r should beSome
    }

    "parse query2" in {
      val r = parser.parse(q2).toOption
      r should beSome
    }

    "parse query3" in {
      val r = parser.parse(q3).toOption
      r should beSome
    }

    "parse query4" in {
      val r = parser.parse(q4).toOption
      r should beSome
    }

    "parse query5" in {
      val r = parser.parse(q5).toOption
      r should beSome
    }

    "parse query6" in {
      val r = parser.parse(q6).toOption
      r should beSome
    }

    "parse query7" in {
      val r = parser.parse(q7).toOption
      r should beSome
    }

    "parse query8" in {
      val r = parser.parse(q8).toOption
      r should beSome
    }

    "parse query9" in {
      val r = parser.parse(q9).toOption
      r should beSome
    }

    "parse query10" in {
      val r = parser.parse(q10).toOption
      r should beSome
    }

    "parse query11" in {
      val r = parser.parse(q11).toOption
      r should beSome
    }

    "parse query12" in {
      val r = parser.parse(q12).toOption
      r should beSome
    }

    "parse query13" in {
      val r = parser.parse(q13).toOption
      r should beSome
    }

    "parse query14" in {
      val r = parser.parse(q14).toOption
      r should beSome
    }

    "parse query16" in {
      val r = parser.parse(q16).toOption
      r should beSome
    }

    "parse query17" in {
      val r = parser.parse(q17).toOption
      r should beSome
    }

    "parse query18" in {
      val r = parser.parse(q18).toOption
      r should beSome
    }

    "parse query19" in {
      val r = parser.parse(q19).toOption
      r should beSome
    }

    "parse query20" in {
      val r = parser.parse(q20).toOption
      r should beSome
    }

    "parse query21" in {
      val r = parser.parse(q21).toOption
      r should beSome
    }

    "parse query22" in {
      val r = parser.parse(q22).toOption
      r should beSome
    }

    "parse keywords as identifiers" in {
      parser.parse("select as as as from from as from where where group by group order by order") should
        beRightDisjOrDiff(
          Select(
            SelectAll,
            List(Proj(Ident("as"), "as".some)),
            TableRelationAST("from", "from".some).some,
            Ident("where").some,
            GroupBy(List(Ident("group")), None).some,
            OrderBy(List((ASC, Ident("order")))).some))
    }

    "parse quoted literal" in {
      parser.parse("select * from foo where bar = 'abc'").toOption should beSome
    }

    "parse quoted literal with escaped quote" in {
      parser.parse("select * from foo where bar = 'that''s it!'").toOption should beSome
    }

    "parse literal that’s too big for an Int" in {
      parser.parse("select * from users where add_date > 1425460451000") should
        beRightDisjOrDiff(
          Select(
            SelectAll,
            List(Proj(Splice(None), None)),
            Some(TableRelationAST("users",None)),
            Some(Binop(Ident("add_date"),IntLiteral(1425460451000L), Gt)),
            None,None))
    }

    "parse quoted identifier" in {
      parser.parse("""select * from "tmp/foo" """).toOption should beSome
    }

    "parse quoted identifier with escaped quote" in {
      parser.parse("""select * from "tmp/foo[""bar""]" """).toOption should beSome
    }

    "parse simple query with two variables" in {
      parser.parse("""SELECT * FROM zips WHERE zips.dt > :start_time AND zips.dt <= :end_time """).toOption should beSome
    }

    "parse true and false literals" in {
      parser.parse("""SELECT * FROM zips WHERE zips.isNormalized = TRUE AND zips.isFruityFlavored = FALSE""").toOption should beSome
    }

    "parse numeric literals" in {
      parser.parse("select 1, 2.0, 3000000, 2.998e8, -1.602E-19, 1e+6") should beRightDisjunction
    }

    "parse date, time, timestamp, and id literals" in {
      val q = """select * from foo
                  where dt < date('2014-11-16')
                  and tm < time('03:00:00')
                  and ts < timestamp('2014-11-16T03:00:00Z') + interval('PT1H')
                  and _id != oid('abc123')"""

      parser.parse(q) must beRightDisjunction
    }

    "parse IS and IS NOT" in {
      val q = """select * from foo
                  where a IS NULL
                  and b IS NOT NULL
                  and c IS TRUE
                  and d IS NOT FALSE"""

      parser.parse(q) must beRightDisjunction
    }

    "parse nested joins left to right" in {
      val q1 = "select * from a cross join b cross join c"
      val q2 = "select * from (a cross join b) cross join c"
      parser.parse(q1) must_== parser.parse(q2)
    }

    "parse nested joins with parens" in {
      val q = "select * from a cross join (b cross join c)"
      parser.parse(q) must beRightDisjunction(
        Select(
          SelectAll,
          List(Proj(Splice(None), None)),
          Some(
            CrossRelation(
              TableRelationAST("a", None),
              CrossRelation(
                TableRelationAST("b", None),
                TableRelationAST("c", None)))),
          None, None, None))
    }

    "parse array constructor and concat op" in {
      parser.parse("select loc || [ pop ] from zips") must beRightDisjunction(
        Select(SelectAll,
          List(
            Proj(
              Binop(Ident("loc"),
                ArrayLiteral(List(
                  Ident("pop"))),
                Concat),
              None)),
          Some(TableRelationAST("zips", None)),
          None, None, None))
    }

    val expectedSelect = Select(SelectAll,
      List(Proj(Ident("loc"), None)),
      Some(TableRelationAST("places", None)),
      None,
      None,
      None
    )
    val selectString = "select loc from places"

    "parse offset" in {
      val q = s"$selectString offset 6"
      parser.parse(q) must beRightDisjunction(
        Offset(expectedSelect, IntLiteral(6))
      )
    }

    "parse limit" in {
      "normal" in {
        val q = s"$selectString limit 6"
        parser.parse(q) must beRightDisjunction(
          Limit(expectedSelect, IntLiteral(6))
        )
      }
      "multiple limits" in {
        val q = s"$selectString limit 6 limit 3"
        parser.parse(q) must beRightDisjunction(
          Limit(Limit(expectedSelect, IntLiteral(6)), IntLiteral(3))
        )
      }
      "should not allow single limit" in {
        val q = "limit 6"
        parser.parse(q) must beLeftDisjunction
      }
    }

    "parse limit and offset" in {
      "limit before" in {
        val q = s"$selectString limit 6 offset 3"
        parser.parse(q) must beRightDisjunction(
          Offset(Limit(expectedSelect, IntLiteral(6)), IntLiteral(3))
        )
      }
      "limit after" in {
        val q = s"$selectString offset 6 limit 3"
        parser.parse(q) must beRightDisjunction(
          Limit(Offset(expectedSelect, IntLiteral(6)), IntLiteral(3))
        )
      }
    }

    "should refuse a semicolon not at the end" in {
      import shapeless.contrib.scalaz._
      val q = "select foo from (select 5 as foo;) where foo = 7"
      parser.parse(q) must beLeftDisjunction(
        GenericParsingError("operator ')' expected; `;'")
      )
    }

    "should not parse multiple expressions seperated incorrectly" in {
      val q = "select foo from bar limit 6 select biz from baz"
      parser.parse(q) must beLeftDisjunction
    }

    "parse array literal at top level" in {
      parser.parse("['X', 'Y']") must beRightDisjunction(
        ArrayLiteral(List(StringLiteral("X"), StringLiteral("Y"))))
    }

    "parse empty set literal" in {
      parser.parse("()") must beRightDisjunction(
        SetLiteral(Nil))
    }

    "parse parenthesized simple expression (which is syntactically identical to a 1-element set literal)" in {
      parser.parse("(a)") must beRightDisjunction(
        Ident("a"))
    }

    "parse 2-element set literal" in {
      parser.parse("(a, b)") must beRightDisjunction(
        SetLiteral(List(Ident("a"), Ident("b"))))
    }

    "parse deeply nested parens" in {
      // NB: Just a stress-test that the parser can handle a deeply
      // left-recursive expression with many unneeded parenes, which
      // happens to be exactly what pprint produces.
      val q = """(select distinct topArr, topObj from "/demo/demo/nested" where (((((((((((((((search((((topArr)[:*])[:*])[:*], '^.*$', true)) or (search((((topArr)[:*])[:*]).a, '^.*$', true))) or (search((((topArr)[:*])[:*]).b, '^.*$', true))) or (search((((topArr)[:*])[:*]).c, '^.*$', true))) or (search((((topArr)[:*]).botObj).a, '^.*$', true))) or (search((((topArr)[:*]).botObj).b, '^.*$', true))) or (search((((topArr)[:*]).botObj).c, '^.*$', true))) or (search((((topArr)[:*]).botArr)[:*], '^.*$', true))) or (search((((topObj).midArr)[:*])[:*], '^.*$', true))) or (search((((topObj).midArr)[:*]).a, '^.*$', true))) or (search((((topObj).midArr)[:*]).b, '^.*$', true))) or (search((((topObj).midArr)[:*]).c, '^.*$', true))) or (search((((topObj).midObj).botArr)[:*], '^.*$', true))) or (search((((topObj).midObj).botObj).a, '^.*$', true))) or (search((((topObj).midObj).botObj).b, '^.*$', true))) or (search((((topObj).midObj).botObj).c, '^.*$', true)))"""
      parser.parse(q).map(pprint) must beRightDisjunction(q)
    }

    "round-trip to SQL and back" ! prop { (node: Expr) =>
      val parsed = parser.parse(pprint(node))

      parsed.fold(
        _ => println(node.shows + "\n" + pprint(node)),
        p => if (p != node) println(pprint(p) + "\n" + (node.render diff p.render).show))

      parsed must beRightDisjOrDiff(node)
    }
  }
}
