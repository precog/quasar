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
import pathy.Path._

class SQLParserSpec extends Specification with ScalaCheck with DisjunctionMatchers {
  import SqlQueries._, ExprArbitrary._

  implicit def stringToQuery(s: String): Query = Query(s)

  def parse(query: Query): ParsingError \/ Expr =
    (new SQLParser).parse(query).map(_.makeTables(Nil))

  "SQLParser" should {
    "parse query1" in {
      val r = parse(q1).toOption
      r should beSome
    }

    "parse query2" in {
      val r = parse(q2).toOption
      r should beSome
    }

    "parse query3" in {
      val r = parse(q3).toOption
      r should beSome
    }

    "parse query4" in {
      val r = parse(q4).toOption
      r should beSome
    }

    "parse query5" in {
      val r = parse(q5).toOption
      r should beSome
    }

    "parse query6" in {
      val r = parse(q6).toOption
      r should beSome
    }

    "parse query7" in {
      val r = parse(q7).toOption
      r should beSome
    }

    "parse query8" in {
      val r = parse(q8).toOption
      r should beSome
    }

    "parse query9" in {
      val r = parse(q9).toOption
      r should beSome
    }

    "parse query10" in {
      val r = parse(q10).toOption
      r should beSome
    }

    "parse query11" in {
      val r = parse(q11).toOption
      r should beSome
    }

    "parse query12" in {
      val r = parse(q12).toOption
      r should beSome
    }

    "parse query13" in {
      val r = parse(q13).toOption
      r should beSome
    }

    "parse query14" in {
      val r = parse(q14).toOption
      r should beSome
    }

    "parse query16" in {
      val r = parse(q16).toOption
      r should beSome
    }

    "parse query17" in {
      val r = parse(q17).toOption
      r should beSome
    }

    "parse query18" in {
      val r = parse(q18).toOption
      r should beSome
    }

    "parse query19" in {
      val r = parse(q19).toOption
      r should beSome
    }

    "parse query20" in {
      val r = parse(q20).toOption
      r should beSome
    }

    "parse query21" in {
      val r = parse(q21).toOption
      r should beSome
    }

    "parse query22" in {
      val r = parse(q22).toOption
      r should beSome
    }

    "parse basic select" in {
      parse("select foo from bar") must
        beRightDisjunction(
          Select(
            SelectAll,
            List(Proj(Ident("foo"), None)),
            Some(TableRelationAST(file("bar"), None)),
            None, None, None))
    }

    "parse keywords as identifiers" in {
      parse("select as as as from from as from where where group by group order by order") should
        beRightDisjOrDiff(
          Select(
            SelectAll,
            List(Proj(Ident("as"), "as".some)),
            TableRelationAST(file("from"), "from".some).some,
            Ident("where").some,
            GroupBy(List(Ident("group")), None).some,
            OrderBy(List((ASC, Ident("order")))).some))
    }

    "parse ambiguous keyword as identifier" in {
      parse("""select `false` from zips""") should
        beRightDisjOrDiff(
          Select(
            SelectAll,
            List(Proj(Ident("false"), None)),
            TableRelationAST(file("zips"), None).some,
            None, None, None))
    }

    "parse ambiguous expression as expression" in {
      parse("""select case from when where then and end""") should
        beRightDisjOrDiff(
          Select(
            SelectAll,
            List(Proj(Match(Ident("from"), List(Case(Ident("where"), Ident("and"))), None), None)),
            None, None, None, None))
    }

    "parse partially-disambiguated expression" in {
      parse("""select `case` from when where then and end""") should
        beRightDisjOrDiff(
          Select(
            SelectAll,
            List(Proj(Ident("case"), None)),
            TableRelationAST(file("when"), None).some,
            Binop(Ident("then"), Ident("end"), And).some,
            None, None))
    }
    "parse quoted literal" in {
      parse("""select * from foo where bar = "abc" """).toOption should beSome
    }

    "parse quoted literal with escaped quote" in {
      parse(raw"""select * from foo where bar = "that\"s it!" """).toOption should beSome
    }

    "don’t parse multi-character char literal" in {
      parse("""select * from foo where bar = 'it!'""").toOption should beNone
    }

    "parse literal that’s too big for an Int" in {
      parse("select * from users where add_date > 1425460451000") should
        beRightDisjOrDiff(
          Select(
            SelectAll,
            List(Proj(Splice(None), None)),
            Some(TableRelationAST(file("users"),None)),
            Some(Binop(Ident("add_date"),IntLiteral(1425460451000L), Gt)),
            None,None))
    }

    "parse quoted identifier" in {
      parse("""select * from `tmp/foo` """).toOption should beSome
    }

    "parse quoted identifier with escaped quote" in {
      parse(raw"select * from `tmp/foo[\`bar\`]` ").toOption should beSome
    }

    "parse simple query with two variables" in {
      parse("""SELECT * FROM zips WHERE zips.dt > :start_time AND zips.dt <= :end_time """).toOption should beSome
    }

    "parse true and false literals" in {
      parse("""SELECT * FROM zips WHERE zips.isNormalized = TRUE AND zips.isFruityFlavored = FALSE""").toOption should beSome
    }

    "parse “full-value” insert expression" in {
      parse("insert into zips values 1, 2, 3") should
      beRightDisjOrDiff(
        Union(
          SetLiteral(List(IntLiteral(1), IntLiteral(2), IntLiteral(3))),
          Select(
            SelectAll,
            List(Proj(Splice(None), None)),
            Some(TableRelationAST(file("zips"),None)),
            None, None, None)))
    }

    "parse “keyed” insert expression" in {
      parse("insert into zips ('a', 'b') values (1, 2), (3, 4)") should
      beRightDisjOrDiff(
        Union(
          SetLiteral(List(
            MapLiteral(List(StringLiteral("a") -> IntLiteral(1), StringLiteral("b") -> IntLiteral(2))),
            MapLiteral(List(StringLiteral("a") -> IntLiteral(3), StringLiteral("b") -> IntLiteral(4))))),
          Select(
            SelectAll,
            List(Proj(Splice(None), None)),
            Some(TableRelationAST(file("zips"),None)),
            None, None, None)))
    }

    "parse numeric literals" in {
      parse("select 1, 2.0, 3000000, 2.998e8, -1.602E-19, 1e+6") should beRightDisjunction
    }

    "parse date, time, timestamp, and id literals" in {
      val q = """select * from foo
                  where dt < date("2014-11-16")
                  and tm < time("03:00:00")
                  and ts < timestamp("2014-11-16T03:00:00Z") + interval("PT1H")
                  and _id != oid("abc123")"""

      parse(q) must beRightDisjunction
    }

    "parse IS and IS NOT" in {
      val q = """select * from foo
                  where a IS NULL
                  and b IS NOT NULL
                  and c IS TRUE
                  and d IS NOT FALSE"""

      parse(q) must beRightDisjunction
    }

    "parse is (not) as (!)=" in {
      val q1 = "select * from zips where pop is 1000 and city is not \"BOULDER\""
      val q2 = "select * from zips where pop = 1000 and city != \"BOULDER\""
      parse(q1) must_== parse(q2)
    }

    "parse `in` and `like` with optional `is`" in {
      val q1 = "select * from zips where pop is in (1000, 2000) and city is like \"BOU%\""
      val q2 = "select * from zips where pop in (1000, 2000) and city like \"BOU%\""
      parse(q1) must_== parse(q2)
    }

    "parse `not in` and `not like` with optional `is`" in {
      val q1 = "select * from zips where pop is not in (1000, 2000) and city is not like \"BOU%\""
      val q2 = "select * from zips where pop not in (1000, 2000) and city not like \"BOU%\""
      parse(q1) must_== parse(q2)
    }

    "parse nested joins left to right" in {
      val q1 = "select * from a cross join b cross join c"
      val q2 = "select * from (a cross join b) cross join c"
      parse(q1) must_== parse(q2)
    }

    "parse nested joins with parens" in {
      val q = "select * from a cross join (b cross join c)"
      parse(q) must beRightDisjunction(
        Select(
          SelectAll,
          List(Proj(Splice(None), None)),
          Some(
            CrossRelation(
              TableRelationAST(file("a"), None),
              CrossRelation(
                TableRelationAST(file("b"), None),
                TableRelationAST(file("c"), None)))),
          None, None, None))
    }

    "parse array constructor and concat op" in {
      parse("select loc || [ pop ] from zips") must beRightDisjunction(
        Select(SelectAll,
          List(
            Proj(
              Binop(Ident("loc"),
                ArrayLiteral(List(
                  Ident("pop"))),
                Concat),
              None)),
          Some(TableRelationAST(file("zips"), None)),
          None, None, None))
    }

    val expectedSelect = Select(SelectAll,
      List(Proj(Ident("loc"), None)),
      Some(TableRelationAST(file("places"), None)),
      None,
      None,
      None
    )
    val selectString = "select loc from places"

    "parse offset" in {
      val q = s"$selectString offset 6"
      parse(q) must beRightDisjunction(
        Offset(expectedSelect, IntLiteral(6))
      )
    }

    "parse limit" in {
      "normal" in {
        val q = s"$selectString limit 6"
        parse(q) must beRightDisjunction(
          Limit(expectedSelect, IntLiteral(6))
        )
      }
      "multiple limits" in {
        val q = s"$selectString limit 6 limit 3"
        parse(q) must beRightDisjunction(
          Limit(Limit(expectedSelect, IntLiteral(6)), IntLiteral(3))
        )
      }
      "should not allow single limit" in {
        val q = "limit 6"
        parse(q) must beLeftDisjunction
      }
    }

    "parse limit and offset" in {
      "limit before" in {
        val q = s"$selectString limit 6 offset 3"
        parse(q) must beRightDisjunction(
          Offset(Limit(expectedSelect, IntLiteral(6)), IntLiteral(3))
        )
      }
      "limit after" in {
        val q = s"$selectString offset 6 limit 3"
        parse(q) must beRightDisjunction(
          Limit(Offset(expectedSelect, IntLiteral(6)), IntLiteral(3))
        )
      }
    }

    "should refuse a semicolon not at the end" in {
      import shapeless.contrib.scalaz._
      val q = "select foo from (select 5 as foo;) where foo = 7"
      parse(q) must beLeftDisjunction(
        GenericParsingError("operator ')' expected; `;'")
      )
    }

    "parse basic let" in {
      parse("""foo := 5; foo""") must
        beRightDisjunction(
          Let("foo", IntLiteral(5), Ident("foo")))
    }

    "parse nested lets" in {
      parse("""foo := 5; bar := "hello"; bar + foo""") must
        beRightDisjunction(
          Let(
            "foo",
            IntLiteral(5),
            Let(
              "bar",
              StringLiteral("hello"),
              Binop(Ident("bar"), Ident("foo"), Plus))))
    }

    "parse let inside select" in {
      parse("""select foo from (bar := 12; baz) as quag""") must
        beRightDisjunction(
          Select(
            SelectAll,
            List(Proj(Ident("foo"), None)),
            Some(ExprRelationAST(
              Let(
                "bar",
                IntLiteral(12),
                Ident("baz")),
              "quag")),
            None,
            None,
            None))
    }

    "parse select inside body of let" in {
      parse("""foo := (1,2,3); select * from foo""") must
        beRightDisjunction(
          Let(
            "foo",
            SetLiteral(
              List(IntLiteral(1), IntLiteral(2), IntLiteral(3))),
            Select(
              SelectAll,
              List(Proj(Splice(None), None)),
              Some(IdentRelationAST("foo", None)),
              None,
              None,
              None)))
    }

    "parse select inside body of let" in {
      parse("""foo := (1,2,3); select foo from bar""") must
        beRightDisjunction(
          Let(
            "foo",
            SetLiteral(
              List(IntLiteral(1), IntLiteral(2), IntLiteral(3))),
            Select(
              SelectAll,
              // TODO this should be IdentRelationAST not Ident
              List(Proj(Ident("foo"), None)),
              Some(TableRelationAST(file("bar"), None)),
              None,
              None,
              None)))
    }

    "parse select inside body of let inside select" in {
      val innerLet =
        Let(
          "foo",
          SetLiteral(
            List(IntLiteral(1), IntLiteral(2), IntLiteral(3))),
          Select(
            SelectAll,
            List(Proj(Splice(None), None)),
            Some(IdentRelationAST("foo", None)),
            None,
            None,
            None))

      parse("""select (foo := (1,2,3); select * from foo) from baz""") must
        beRightDisjunction(
          Select(
            SelectAll,
            List(Proj(innerLet, None)),
            Some(TableRelationAST(file("baz"), None)),
            None,
            None,
            None))
    }

    "should parse a single-quoted character" in {
      val q = "'c'"
      parse(q) must beRightDisjunction(StringLiteral("c"))
    }

    "should parse escaped characters" in {
      val q = raw"select '\'', '\\', '\u1234'"
      parse(q) must beRightDisjunction(
        Select(SelectAll, List(
          Proj(StringLiteral("'"), None),
          Proj(StringLiteral(raw"\"), None),
          Proj(StringLiteral("ሴ"), None)),
          None, None, None, None))
    }
    "should parse escaped characters in a string" in {
      val q = raw""""'\\\u1234""""
      parse(q) must beRightDisjunction(StringLiteral(raw"'\ሴ"))
    }

    "should not parse multiple expressions seperated incorrectly" in {
      val q = "select foo from bar limit 6 select biz from baz"
      parse(q) must beLeftDisjunction
    }

    "parse array literal at top level" in {
      parse("""["X", "Y"]""") must beRightDisjunction(
        ArrayLiteral(List(StringLiteral("X"), StringLiteral("Y"))))
    }

    "parse empty set literal" in {
      parse("()") must beRightDisjunction(
        SetLiteral(Nil))
    }

    "parse parenthesized simple expression (which is syntactically identical to a 1-element set literal)" in {
      parse("(a)") must beRightDisjunction(
        Ident("a"))
    }

    "parse 2-element set literal" in {
      parse("(a, b)") must beRightDisjunction(
        SetLiteral(List(Ident("a"), Ident("b"))))
    }

    "parse deeply nested parens" in {
      // NB: Just a stress-test that the parser can handle a deeply
      // left-recursive expression with many unneeded parenes, which
      // happens to be exactly what pprint produces.
      val q = """(select distinct topArr, topObj from `/demo/demo/nested` where (((((((((((((((search((((topArr)[:*])[:*])[:*], "^.*$", true)) or (search((((topArr)[:*])[:*]).a, "^.*$", true))) or (search((((topArr)[:*])[:*]).b, "^.*$", true))) or (search((((topArr)[:*])[:*]).c, "^.*$", true))) or (search((((topArr)[:*]).botObj).a, "^.*$", true))) or (search((((topArr)[:*]).botObj).b, "^.*$", true))) or (search((((topArr)[:*]).botObj).c, "^.*$", true))) or (search((((topArr)[:*]).botArr)[:*], "^.*$", true))) or (search((((topObj).midArr)[:*])[:*], "^.*$", true))) or (search((((topObj).midArr)[:*]).a, "^.*$", true))) or (search((((topObj).midArr)[:*]).b, "^.*$", true))) or (search((((topObj).midArr)[:*]).c, "^.*$", true))) or (search((((topObj).midObj).botArr)[:*], "^.*$", true))) or (search((((topObj).midObj).botObj).a, "^.*$", true))) or (search((((topObj).midObj).botObj).b, "^.*$", true))) or (search((((topObj).midObj).botObj).c, "^.*$", true)))"""
      parse(q).map(pprint) must beRightDisjunction(q)
    }

    "should not parse query with a single backslash in an identifier" >> {
      "in table relation" in {
        parse(raw"select * from `\bar`") should beLeftDisjunction
      }.pendingUntilFixed("SD-1536")
      "in identifier" in {
        parse(raw"`\bar`") should beLeftDisjunction
      }.pendingUntilFixed("SD-1536")
    }

    "round-trip to SQL and back" ! prop { (node: Expr) =>
      val parsed = parse(pprint(node))

      parsed.fold(
        _ => println(node.shows + "\n" + pprint(node)),
        p => if (p != node) println(pprint(p) + "\n" + (node.render diff p.render).show))

      parsed must beRightDisjOrDiff(node)
    }
  }
}
