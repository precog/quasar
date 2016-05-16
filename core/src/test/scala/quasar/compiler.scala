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
import quasar.std._
import quasar.specs2.PendingWithAccurateCoverage

import matryoshka.Fix
import org.specs2.mutable._
import org.specs2.scalaz._

class CompilerSpec extends Specification with CompilerHelpers with PendingWithAccurateCoverage with DisjunctionMatchers {
  import StdLib._
  import agg._
  import array._
  import date._
  import identity._
  import math._
  import relations._
  import set._
  import string._
  import structural._

  import LogicalPlan._

  "compiler" should {
    "compile simple constant example 1" in {
      testLogicalPlanCompile(
        "select 1",
        makeObj("0" -> Constant(Data.Int(1))))
    }

    "compile simple boolean literal (true)" in {
      testLogicalPlanCompile(
        "select true",
        makeObj("0" -> Constant(Data.Bool(true))))
    }

    "compile simple boolean literal (false)" in {
      testLogicalPlanCompile(
        "select false",
        makeObj("0" -> Constant(Data.Bool(false))))
    }

    "compile simple constant with multiple named projections" in {
      testLogicalPlanCompile(
        "select 1.0 as a, \"abc\" as b",
        makeObj(
          "a" -> Constant(Data.Dec(1.0)),
          "b" -> Constant(Data.Str("abc"))))
    }

    "compile select substring" in {
      testLogicalPlanCompile(
        "select substring(bar, 2, 3) from foo",
        Squash.apply0(
          makeObj(
            "0" ->
              Substring.apply0[FLP](
                ObjectProject.apply0(read("foo"), Constant(Data.Str("bar"))),
                Constant(Data.Int(2)),
                Constant(Data.Int(3))))))
    }

    "compile select length" in {
      testLogicalPlanCompile(
        "select length(bar) from foo",
        Squash.apply0(
          makeObj(
            "0" -> Length.apply0[FLP](ObjectProject.apply0(read("foo"), Constant(Data.Str("bar")))))))
    }

    "compile simple select *" in {
      testLogicalPlanCompile("select * from foo", Squash.apply0(read("foo")))
    }

    "compile qualified select *" in {
      testLogicalPlanCompile("select foo.* from foo", Squash.apply0(read("foo")))
    }

    "compile qualified select * with additional fields" in {
      testLogicalPlanCompile(
        "select foo.*, bar.address from foo, bar",
        Let('__tmp0,
          InnerJoin.apply0(read("foo"), read("bar"), Constant(Data.Bool(true))),
          Squash.apply0[FLP](
            ObjectConcat.apply0[FLP](
              ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("left"))),
              makeObj(
                "address" ->
                  ObjectProject.apply0[FLP](
                    ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("right"))),
                    Constant(Data.Str("address"))))))))
    }

    "compile deeply-nested qualified select *" in {
      testLogicalPlanCompile(
        "select foo.bar.baz.*, bar.address from foo, bar",
        Let('__tmp0,
          InnerJoin.apply0(read("foo"), read("bar"), Constant(Data.Bool(true))),
          Squash.apply0[FLP](
            ObjectConcat.apply0[FLP](
              ObjectProject.apply0[FLP](
                ObjectProject.apply0[FLP](
                  ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("left"))),
                  Constant(Data.Str("bar"))),
                Constant(Data.Str("baz"))),
              makeObj(
                "address" ->
                  ObjectProject.apply0[FLP](
                    ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("right"))),
                    Constant(Data.Str("address"))))))))
    }

    "compile simple select with unnamed projection which is just an identifier" in {
      testLogicalPlanCompile(
        "select name from city",
        Squash.apply0(
          makeObj(
            "name" -> ObjectProject.apply0(read("city"), Constant(Data.Str("name"))))))
    }

    "compile basic let" in {
      testLogicalPlanCompile(
        "foo := 5; foo",
        Constant(Data.Int(5)))
    }

    "compile basic let, ignoring the form" in {
      testLogicalPlanCompile(
        "bar := 5; 7",
        Constant(Data.Int(7)))
    }

    "compile nested lets" in {
      testLogicalPlanCompile(
        """foo := 5; bar := 7; bar + foo""",
        Add.apply0[FLP](Constant(Data.Int(7)), Constant(Data.Int(5))))
    }

    "compile let with select in body from let binding ident" in {
      val query = """foo := (1,2,3); select * from foo"""
      val expectation =
        Squash.apply0[FLP](
          ShiftArray.apply0[FLP](
            ArrayConcat.apply0[FLP](
              ArrayConcat.apply0[FLP](
                MakeArrayN[Fix](Constant(Data.Int(1))),
                MakeArrayN[Fix](Constant(Data.Int(2)))),
              MakeArrayN[Fix](Constant(Data.Int(3))))))

      testLogicalPlanCompile(query, expectation)
    }

    "compile let with select in body selecting let binding ident" in {
      val query = """foo := 12; select foo from bar"""
      val expectation =
        Squash.apply0(
          makeObj(
            "0" ->
              Constant(Data.Int(12))))

      testLogicalPlanCompile(query, expectation)
    }

    "fail to compile let inside select with ambigious reference" in {
      compile("""select foo from (bar := 12; baz) as quag""") must
        beLeftDisjunction  // AmbiguousReference(baz)
    }

    "compile let inside select with table reference" in {
      val query = """select foo from (bar := 12; select * from baz) as quag"""
      val expectation =
        Squash.apply0(
          makeObj(
            "foo" ->
              ObjectProject.apply0[FLP](Squash.apply0(read("baz")), Constant(Data.Str("foo")))))

      testLogicalPlanCompile(query, expectation)
    }

    "compile let inside select with ident reference" in {
      val query = """select foo from (bar := 12; select * from bar) as quag"""
      val expectation =
        Squash.apply0(
          makeObj(
            "foo" ->
              ObjectProject.apply0[FLP](Squash.apply0(Constant(Data.Int(12))), Constant(Data.Str("foo")))))

      testLogicalPlanCompile(query, expectation)
    }

    "compile selection with same ident as nested let" in {
      val query = """select bar from (bar := 12; select * from bar) as quag"""
      val expectation =
        Squash.apply0(
          makeObj(
            "bar" ->
              ObjectProject.apply0[FLP](Squash.apply0(Constant(Data.Int(12))), Constant(Data.Str("bar")))))

      testLogicalPlanCompile(query, expectation)
    }

    "compile selection with same ident as nested let and alias" in {
      val query = """select bar from (bar := 12; select * from bar) as bar"""
      val expectation =
        Squash.apply0(
          makeObj(
            "0" ->
              Squash.apply0(Constant(Data.Int(12)))))

      testLogicalPlanCompile(query, expectation)
    }

    "compile let with select in form and body" in {
      val query = """foo := select * from bar; select * from foo"""
      val expectation = Squash.apply0[FLP](Squash.apply0[FLP](read("bar")))

      testLogicalPlanCompile(query, expectation)
    }

    "compile let with inner context that shares a table reference" in {
      val query = """select (foo := select * from bar; select * from foo) from foo"""
      val expectation =
        Squash.apply0[FLP](
          makeObj(
            "0" ->
              Squash.apply0[FLP](Squash.apply0[FLP](read("bar")))))

      testLogicalPlanCompile(query, expectation)
    }

    "compile let with an inner context of as that shares a binding name" in {
      val query = """foo := 4; select * from bar as foo"""
      val expectation = Squash.apply0(read("bar"))

      testLogicalPlanCompile(query, expectation)
    }

    "fail to compile let with an inner context of let that shares a binding name in expression context" in {
      val query = """foo := 4; select * from (foo := bar; foo) as quag"""

      compile(query) must beLeftDisjunction // ambiguous reference for `bar` - `4` or `foo`
    }

    "compile let with an inner context of as that shares a binding name in table context" in {
      val query = """foo := 4; select * from (foo := select * from bar; foo) as quag"""
      val expectation = Squash.apply0[FLP](Squash.apply0[FLP](read("bar")))

      testLogicalPlanCompile(query, expectation)
    }

    "compile simple 1-table projection when root identifier is also a projection" in {
      // 'foo' must be interpreted as a projection because only this interpretation is possible
      testLogicalPlanCompile(
        "select foo.bar from baz",
        Squash.apply0(
          makeObj(
            "bar" ->
              ObjectProject.apply0[FLP](
                ObjectProject.apply0(read("baz"), Constant(Data.Str("foo"))),
                Constant(Data.Str("bar"))))))
    }

    "compile simple 1-table projection when root identifier is also a table ref" in {
      // 'foo' must be interpreted as a table reference because this
      // interpretation is possible and consistent with ANSI SQL.
      testLogicalPlanCompile(
        "select foo.bar from foo",
        Squash.apply0(
          makeObj(
            "bar" -> ObjectProject.apply0(read("foo"), Constant(Data.Str("bar"))))))
    }

    "compile two term addition from one table" in {
      testLogicalPlanCompile(
        "select foo + bar from baz",
        Let('__tmp0, read("baz"),
          Squash.apply0(
            makeObj(
              "0" ->
                Add.apply0[FLP](
                  ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("foo"))),
                  ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("bar"))))))))
    }

    "compile negate" in {
      testLogicalPlanCompile(
        "select -foo from bar",
        Squash.apply0(
          makeObj(
            "0" ->
              Negate.apply0[FLP](ObjectProject.apply0(read("bar"), Constant(Data.Str("foo")))))))
    }

    "compile modulo" in {
      testLogicalPlanCompile(
        "select foo % baz from bar",
        Let('__tmp0, read("bar"),
          Squash.apply0(
            makeObj(
              "0" ->
                Modulo.apply0[FLP](
                  ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("foo"))),
                  ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("baz"))))))))
    }

    "compile coalesce" in {
      testLogicalPlanCompile(
        "select coalesce(bar, baz) from foo",
        Let('__tmp0, read("foo"),
          Squash.apply0(
            makeObj(
              "0" ->
                Coalesce.apply0[FLP](
                  ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("bar"))),
                  ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("baz"))))))))
    }

    "compile date field extraction" in {
      testLogicalPlanCompile(
        "select date_part(\"day\", baz) from foo",
        Squash.apply0(
          makeObj(
            "0" ->
              Extract.apply0[FLP](
                Constant(Data.Str("day")),
                ObjectProject.apply0(read("foo"), Constant(Data.Str("baz")))))))
    }

    "compile conditional" in {
      testLogicalPlanCompile(
        "select case when pop < 10000 then city else loc end from zips",
        Let('__tmp0, read("zips"),
          Squash.apply0(makeObj(
            "0" ->
              Cond.apply0[FLP](
                Lt.apply0[FLP](
                  ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("pop"))),
                  Constant(Data.Int(10000))),
                ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("city"))),
                ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("loc"))))))))
    }

    "compile conditional (match) without else" in {
      testLogicalPlanCompile(
                   "select case when pop = 0 then \"nobody\" end from zips",
        compileExp("select case when pop = 0 then \"nobody\" else null end from zips"))
    }

    "compile conditional (switch) without else" in {
      testLogicalPlanCompile(
                   "select case pop when 0 then \"nobody\" end from zips",
        compileExp("select case pop when 0 then \"nobody\" else null end from zips"))
    }

    "have ~~ as alias for LIKE" in {
      testLogicalPlanCompile(
                   "select pop from zips where city ~~ \"%BOU%\"",
        compileExp("select pop from zips where city LIKE \"%BOU%\""))
    }

    "have !~~ as alias for NOT LIKE" in {
      testLogicalPlanCompile(
                   "select pop from zips where city !~~ \"%BOU%\"",
        compileExp("select pop from zips where city NOT LIKE \"%BOU%\""))
    }

    "compile array length" in {
      testLogicalPlanCompile(
        "select array_length(bar, 1) from foo",
        Squash.apply0(
          makeObj(
            "0" ->
              ArrayLength.apply0[FLP](
                ObjectProject.apply0(read("foo"), Constant(Data.Str("bar"))),
                Constant(Data.Int(1))))))
    }

    "compile concat" in {
      testLogicalPlanCompile(
        "select concat(foo, concat(\" \", bar)) from baz",
        Let('__tmp0, read("baz"),
          Squash.apply0(
            makeObj(
              "0" ->
                Concat.apply0[FLP](
                  ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("foo"))),
                  Concat.apply0[FLP](
                    Constant(Data.Str(" ")),
                    ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("bar")))))))))
    }

    "compile between" in {
      testLogicalPlanCompile(
        "select * from foo where bar between 1 and 10",
        Let('__tmp0, read("foo"),
          Squash.apply0[FLP](
            Filter.apply0[FLP](
              Free('__tmp0),
              Between.apply0[FLP](
                ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("bar"))),
                Constant(Data.Int(1)),
                Constant(Data.Int(10)))))))
    }

    "compile not between" in {
      testLogicalPlanCompile(
        "select * from foo where bar not between 1 and 10",
        Let('__tmp0, read("foo"),
          Squash.apply0[FLP](
            Filter.apply0[FLP](
              Free('__tmp0),
              Not.apply0[FLP](
                Between.apply0[FLP](
                  ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("bar"))),
                  Constant(Data.Int(1)),
                  Constant(Data.Int(10))))))))
    }

    "compile like" in {
      testLogicalPlanCompile(
        "select bar from foo where bar like \"a%\"",
        Let('__tmp0, read("foo"),
          Squash.apply0(
            makeObj(
              "bar" ->
                ObjectProject.apply0[FLP](
                  Filter.apply0[FLP](
                    Free('__tmp0),
                    Search.apply0[FLP](
                      ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("bar"))),
                      Constant(Data.Str("^a.*$")),
                      Constant(Data.Bool(false)))),
                  Constant(Data.Str("bar")))))))
    }

    "compile like with escape char" in {
      testLogicalPlanCompile(
        "select bar from foo where bar like \"a=%\" escape \"=\"",
        Let('__tmp0, read("foo"),
          Squash.apply0(
            makeObj(
              "bar" ->
                ObjectProject.apply0[FLP](
                  Filter.apply0[FLP](
                    Free('__tmp0),
                    Search.apply0[FLP](
                      ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("bar"))),
                      Constant(Data.Str("^a%$")),
                      Constant(Data.Bool(false)))),
                  Constant(Data.Str("bar")))))))
    }

    "compile not like" in {
      testLogicalPlanCompile(
        "select bar from foo where bar not like \"a%\"",
        Let('__tmp0, read("foo"),
          Squash.apply0(makeObj("bar" -> ObjectProject.apply0[FLP](Filter.apply0[FLP](
            Free('__tmp0),
            Not.apply0[FLP](
              Search.apply0[FLP](
                ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("bar"))),
                Constant(Data.Str("^a.*$")),
                Constant(Data.Bool(false))))),
            Constant(Data.Str("bar")))))))
    }

    "compile ~" in {
      testLogicalPlanCompile(
        "select bar from foo where bar ~ \"a.$\"",
        Let('__tmp0, read("foo"),
          Squash.apply0(
            makeObj(
              "bar" ->
                ObjectProject.apply0[FLP](
                  Filter.apply0[FLP](
                    Free('__tmp0),
                    Search.apply0[FLP](
                      ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("bar"))),
                      Constant(Data.Str("a.$")),
                      Constant(Data.Bool(false)))),
                  Constant(Data.Str("bar")))))))
    }

    "compile complex expression" in {
      testLogicalPlanCompile(
        "select avgTemp*9/5 + 32 from cities",
        Squash.apply0(
          makeObj(
            "0" ->
              Add.apply0[FLP](
                Divide.apply0[FLP](
                  Multiply.apply0[FLP](
                    ObjectProject.apply0(read("cities"), Constant(Data.Str("avgTemp"))),
                    Constant(Data.Int(9))),
                  Constant(Data.Int(5))),
                Constant(Data.Int(32))))))
    }

    "compile parenthesized expression" in {
      testLogicalPlanCompile(
        "select (avgTemp + 32)/5 from cities",
        Squash.apply0(
          makeObj(
            "0" ->
              Divide.apply0[FLP](
                Add.apply0[FLP](
                  ObjectProject.apply0(read("cities"), Constant(Data.Str("avgTemp"))),
                  Constant(Data.Int(32))),
                Constant(Data.Int(5))))))
    }

    "compile cross select *" in {
      testLogicalPlanCompile(
        "select * from person, car",
        Let('__tmp0,
          InnerJoin.apply0(read("person"), read("car"), Constant(Data.Bool(true))),
          Squash.apply0[FLP](
            ObjectConcat.apply0[FLP](
              ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("left"))),
              ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("right")))))))
    }

    "compile two term multiplication from two tables" in {
      testLogicalPlanCompile(
        "select person.age * car.modelYear from person, car",
        Let('__tmp0,
          InnerJoin.apply0(read("person"), read("car"), Constant(Data.Bool(true))),
          Squash.apply0(
            makeObj(
              "0" ->
                Multiply.apply0[FLP](
                  ObjectProject.apply0[FLP](
                    ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("left"))),
                    Constant(Data.Str("age"))),
                  ObjectProject.apply0[FLP](
                    ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("right"))),
                    Constant(Data.Str("modelYear"))))))))
    }

    "compile simple where (with just a constant)" in {
      testLogicalPlanCompile(
        "select name from person where 1",
        Squash.apply0(
          makeObj(
            "name" ->
              ObjectProject.apply0[FLP](
                Filter.apply0(read("person"), Constant(Data.Int(1))),
                Constant(Data.Str("name"))))))
    }

    "compile simple where" in {
      testLogicalPlanCompile(
        "select name from person where age > 18",
        Let('__tmp0, read("person"),
          Squash.apply0(
            makeObj(
              "name" ->
                ObjectProject.apply0[FLP](
                  Filter.apply0[FLP](
                    Free('__tmp0),
                    Gt.apply0[FLP](
                      ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("age"))),
                      Constant(Data.Int(18)))),
                  Constant(Data.Str("name")))))))
    }

    "compile simple group by" in {
      testLogicalPlanCompile(
        "select count(*) from person group by name",
        Let('__tmp0, read("person"),
          Squash.apply0(
            makeObj(
              "0" ->
                Count.apply0[FLP](
                  GroupBy.apply0[FLP](
                    Free('__tmp0),
                    MakeArrayN[Fix](ObjectProject.apply0(
                      Free('__tmp0),
                      Constant(Data.Str("name"))))))))))
    }

    "compile group by with projected keys" in {
      testLogicalPlanCompile(
        "select lower(name), person.gender, avg(age) from person group by lower(person.name), gender",
        Let('__tmp0, read("person"),
          Let('__tmp1,
            GroupBy.apply0[FLP](
              Free('__tmp0),
              MakeArrayN[Fix](
                Lower.apply0[FLP](
                  ObjectProject.apply0(
                    Free('__tmp0),
                    Constant(Data.Str("name")))),
                ObjectProject.apply0(
                  Free('__tmp0),
                  Constant(Data.Str("gender"))))),
            Squash.apply0(
              makeObj(
                "0" ->
                  Arbitrary.apply0[FLP](
                    Lower.apply0[FLP](
                      ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("name"))))),
                "gender" ->
                  Arbitrary.apply0[FLP](
                    ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("gender")))),
                "2" ->
                  Avg.apply0[FLP](
                    ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("age")))))))))
    }

    "compile group by with perverse aggregated expression" in {
      testLogicalPlanCompile(
        "select count(name) from person group by name",
        Let('__tmp0, read("person"),
          Squash.apply0(
            makeObj(
              "0" ->
                Count.apply0[FLP](
                  ObjectProject.apply0[FLP](
                    GroupBy.apply0[FLP](
                      Free('__tmp0),
                      MakeArrayN[Fix](ObjectProject.apply0(
                        Free('__tmp0),
                        Constant(Data.Str("name"))))),
                    Constant(Data.Str("name"))))))))
    }

    "compile sum in expression" in {
      testLogicalPlanCompile(
        "select sum(pop) * 100 from zips",
        Squash.apply0(
          makeObj(
            "0" ->
              Multiply.apply0[FLP](
                Sum.apply0[FLP](ObjectProject.apply0(read("zips"), Constant(Data.Str("pop")))),
                Constant(Data.Int(100))))))
    }

    val setA =
      Let('__tmp0, read("zips"),
        Squash.apply0(makeObj(
          "loc" -> ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("loc"))),
          "pop" -> ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("pop"))))))
    val setB =
      Squash.apply0(makeObj(
        "city" -> ObjectProject.apply0(read("zips"), Constant(Data.Str("city")))))

    "compile union" in {
      testLogicalPlanCompile(
        "select loc, pop from zips union select city from zips",
          normalizeLets(normalizeLets(
            Distinct.apply0[FLP](Union.apply0[FLP](setA, setB)))))
    }

    "compile union all" in {
      testLogicalPlanCompile(
        "select loc, pop from zips union all select city from zips",
        normalizeLets(normalizeLets(
          Union.apply0[FLP](setA, setB))))
    }

    "compile intersect" in {
      testLogicalPlanCompile(
        "select loc, pop from zips intersect select city from zips",
        normalizeLets(normalizeLets(
          Distinct.apply0[FLP](Intersect.apply0[FLP](setA, setB)))))
    }

    "compile intersect all" in {
      testLogicalPlanCompile(
        "select loc, pop from zips intersect all select city from zips",
        normalizeLets(normalizeLets(
          Intersect.apply0[FLP](setA, setB))))
    }

    "compile except" in {
      testLogicalPlanCompile(
        "select loc, pop from zips except select city from zips",
        normalizeLets(normalizeLets(
          Except.apply0[FLP](setA, setB))))
    }

    "have {*} as alias for {:*}" in {
      testLogicalPlanCompile(
                   "SELECT bar{*} FROM foo",
        compileExp("SELECT bar{:*} FROM foo"))
    }

    "have [*] as alias for [:*]" in {
      testLogicalPlanCompile(
                   "SELECT foo[*] FROM foo",
        compileExp("SELECT foo[:*] FROM foo"))
    }

    "expand top-level map flatten" in {
      testLogicalPlanCompile(
                   "SELECT foo{:*} FROM foo",
        compileExp("SELECT Flatten_Map(foo) AS `0` FROM foo"))
    }

    "expand nested map flatten" in {
      testLogicalPlanCompile(
                   "SELECT foo.bar{:*} FROM foo",
        compileExp("SELECT Flatten_Map(foo.bar) AS `bar` FROM foo"))
    }

    "expand field map flatten" in {
      testLogicalPlanCompile(
                   "SELECT bar{:*} FROM foo",
        compileExp("SELECT Flatten_Map(foo.bar) AS `bar` FROM foo"))
    }

    "expand top-level array flatten" in {
      testLogicalPlanCompile(
                   "SELECT foo[:*] FROM foo",
        compileExp("SELECT Flatten_Array(foo) AS `0` FROM foo"))
    }

    "expand nested array flatten" in {
      testLogicalPlanCompile(
        "SELECT foo.bar[:*] FROM foo",
        compileExp("SELECT Flatten_Array(foo.bar) AS `bar` FROM foo"))
    }

    "expand field array flatten" in {
      testLogicalPlanCompile(
                   "SELECT bar[:*] FROM foo",
        compileExp("SELECT Flatten_Array(foo.bar) AS `bar` FROM foo"))
    }

    "compile top-level map flatten" in {
      testLogicalPlanCompile(
        "select zips{:*} from zips",
        Squash.apply0(makeObj("0" -> FlattenMap.apply0(read("zips")))))
    }

    "have {_} as alias for {:_}" in {
      testLogicalPlanCompile(
                   "select length(commit.author{_}) from slamengine_commits",
        compileExp("select length(commit.author{:_}) from slamengine_commits"))
    }

    "have [_] as alias for [:_]" in {
      testLogicalPlanCompile(
                   "select loc[_] / 10 from zips",
        compileExp("select loc[:_] / 10 from zips"))
    }

    "compile map shift / unshift" in {
      testLogicalPlanCompile(
        "select {length(commit.author{:_})...} from slamengine_commits",
        Squash.apply0(makeObj("0" ->
          UnshiftMap.apply0[FLP](
            Length.apply0[FLP](
              ShiftMap.apply0[FLP](ObjectProject.apply0[FLP](ObjectProject.apply0(read("slamengine_commits"), Constant(Data.Str("commit"))), Constant(Data.Str("author")))))))))
    }

    "compile map shift / unshift keys" in {
      testLogicalPlanCompile(
        "select {length(commit.author{_:})...} from slamengine_commits",
        Squash.apply0(makeObj("0" -> UnshiftMap.apply0[FLP](Length.apply0[FLP](ShiftMapKeys.apply0[FLP](ObjectProject.apply0[FLP](ObjectProject.apply0(read("slamengine_commits"), Constant(Data.Str("commit"))), Constant(Data.Str("author")))))))))
    }

    "compile array shift / unshift" in {
      testLogicalPlanCompile(
        "select [loc[:_] / 10 ...] from zips",
        Squash.apply0(
          makeObj(
            "0" ->
              UnshiftArray.apply0[FLP](
                Divide.apply0[FLP](
                  ShiftArray.apply0[FLP](ObjectProject.apply0(read("zips"), Constant(Data.Str("loc")))),
                  Constant(Data.Int(10)))))))
    }

    "compile array shift / unshift indices" in {
      testLogicalPlanCompile(
        "select [loc[_:] * 10 ...] from zips",
        Squash.apply0(
          makeObj(
            "0" ->
              UnshiftArray.apply0[FLP](
                Multiply.apply0[FLP](
                  ShiftArrayIndices.apply0[FLP](ObjectProject.apply0(read("zips"), Constant(Data.Str("loc")))),
                  Constant(Data.Int(10)))))))
    }

    "compile array flatten" in {
      testLogicalPlanCompile(
        "select loc[:*] from zips",
        Squash.apply0(
          makeObj(
            "loc" ->
              FlattenArray.apply0[FLP](ObjectProject.apply0(read("zips"), Constant(Data.Str("loc")))))))
    }

    "compile simple order by" in {
      testLogicalPlanCompile(
        "select name from person order by height",
        Let('__tmp0, read("person"),
          Let('__tmp1,
            Squash.apply0(
              makeObj(
                "name" -> ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("name"))),
                "__sd__0" -> ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("height"))))),
            DeleteField.apply0[FLP](
              OrderBy.apply0[FLP](
                Free('__tmp1),
                MakeArrayN[Fix](
                  ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("__sd__0")))),
                MakeArrayN(
                  Constant(Data.Str("ASC")))),
              Constant(Data.Str("__sd__0"))))))
    }

    "compile simple order by with filter" in {
      testLogicalPlanCompile(
        "select name from person where gender = \"male\" order by name, height",
        Let('__tmp0, read("person"),
          Let('__tmp1,
            Filter.apply0[FLP](
              Free('__tmp0),
              Eq.apply0[FLP](
                ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("gender"))),
                Constant(Data.Str("male")))),
            Let('__tmp2,
              Squash.apply0(
                makeObj(
                  "name"    -> ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("name"))),
                  "__sd__0" -> ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("height"))))),
              DeleteField.apply0[FLP](
                OrderBy.apply0[FLP](
                  Free('__tmp2),
                  MakeArrayN[Fix](
                    ObjectProject.apply0(Free('__tmp2), Constant(Data.Str("name"))),
                    ObjectProject.apply0(Free('__tmp2), Constant(Data.Str("__sd__0")))),
                  MakeArrayN(
                    Constant(Data.Str("ASC")),
                    Constant(Data.Str("ASC")))),
                Constant(Data.Str("__sd__0")))))))
    }

    "compile simple order by with wildcard" in {
      testLogicalPlanCompile(
        "select * from person order by height",
        Let('__tmp0, Squash.apply0(read("person")),
          OrderBy.apply0[FLP](
            Free('__tmp0),
            MakeArrayN[Fix](
              ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("height")))),
            MakeArrayN(
              Constant(Data.Str("ASC"))))))
    }

    "compile simple order by with ascending and descending" in {
      testLogicalPlanCompile(
        "select * from person order by height desc, name",
        Let('__tmp0, Squash.apply0(read("person")),
          OrderBy.apply0[FLP](
            Free('__tmp0),
            MakeArrayN[Fix](
              ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("height"))),
              ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("name")))),
            MakeArrayN(
              Constant(Data.Str("DESC")),
              Constant(Data.Str("ASC"))))))
    }

    "compile simple order by with expression" in {
      testLogicalPlanCompile(
        "select * from person order by height*2.54",
        Let('__tmp0, read("person"),
          Let('__tmp1,
            Squash.apply0[FLP](
              ObjectConcat.apply0(
                Free('__tmp0),
                makeObj(
                  "__sd__0" -> Multiply.apply0[FLP](
                    ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("height"))),
                    Constant(Data.Dec(2.54)))))),
            DeleteField.apply0[FLP](
              OrderBy.apply0[FLP](
                Free('__tmp1),
                MakeArrayN[Fix](
                  ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("__sd__0")))),
                MakeArrayN(
                  Constant(Data.Str("ASC")))),
              Constant(Data.Str("__sd__0"))))))
    }

    "compile order by with alias" in {
      testLogicalPlanCompile(
        "select firstName as name from person order by name",
        Let('__tmp0,
          Squash.apply0(
            makeObj(
              "name" -> ObjectProject.apply0(read("person"), Constant(Data.Str("firstName"))))),
          OrderBy.apply0[FLP](
            Free('__tmp0),
            MakeArrayN[Fix](ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("name")))),
            MakeArrayN(Constant(Data.Str("ASC"))))))
    }

    "compile simple order by with expression in synthetic field" in {
      testLogicalPlanCompile(
        "select name from person order by height*2.54",
        Let('__tmp0, read("person"),
          Let('__tmp1,
            Squash.apply0(
              makeObj(
                "name" -> ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("name"))),
                "__sd__0" ->
                  Multiply.apply0[FLP](
                    ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("height"))),
                    Constant(Data.Dec(2.54))))),
            DeleteField.apply0[FLP](
              OrderBy.apply0[FLP](
                Free('__tmp1),
                MakeArrayN[Fix](
                  ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("__sd__0")))),
                MakeArrayN(
                  Constant(Data.Str("ASC")))),
              Constant(Data.Str("__sd__0"))))))
    }

    "compile order by with nested projection" in {
      testLogicalPlanCompile(
        "select bar from foo order by foo.bar.baz.quux/3",
        Let('__tmp0, read("foo"),
          Let('__tmp1,
            Squash.apply0(
              makeObj(
                "bar" -> ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("bar"))),
                "__sd__0" -> Divide.apply0[FLP](
                  ObjectProject.apply0[FLP](
                    ObjectProject.apply0[FLP](
                      ObjectProject.apply0(Free('__tmp0),
                        Constant(Data.Str("bar"))),
                      Constant(Data.Str("baz"))),
                    Constant(Data.Str("quux"))),
                  Constant(Data.Int(3))))),
            DeleteField.apply0[FLP](
              OrderBy.apply0[FLP](
                Free('__tmp1),
                MakeArrayN[Fix](
                  ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("__sd__0")))),
                MakeArrayN(
                  Constant(Data.Str("ASC")))),
              Constant(Data.Str("__sd__0"))))))
    }

    "compile order by with root projection a table ref" in {
      // Note: not using wildcard here because the simple case is optimized
      //       differently
      testLogicalPlanCompile(
                   "select foo from bar order by bar.baz",
        compileExp("select foo from bar order by baz"))
    }

    "compile order by with root projection a table ref with alias" in {
      // Note: not using wildcard here because the simple case is optimized
      //       differently
      testLogicalPlanCompile(
                   "select foo from bar as b order by b.baz",
        compileExp("select foo from bar as b order by baz"))
    }

    "compile order by with root projection a table ref with alias, mismatched" in {
      testLogicalPlanCompile(
                   "select * from bar as b order by bar.baz",
        compileExp("select * from bar as b order by b.bar.baz"))
    }

    "compile order by with root projection a table ref, embedded in expr" in {
      testLogicalPlanCompile(
                   "select * from bar order by bar.baz/10",
        compileExp("select * from bar order by baz/10"))
    }

    "compile order by with root projection a table ref, embedded in complex expr" in {
      testLogicalPlanCompile(
                   "select * from bar order by bar.baz/10 - 3*bar.quux",
        compileExp("select * from bar order by baz/10 - 3*quux"))
    }

    "compile multiple stages" in {
      testLogicalPlanCompile(
        "select height*2.54 as cm" +
          " from person" +
          " where height > 60" +
          " group by gender, height" +
          " having count(*) > 10" +
          " order by cm" +
          " offset 10" +
          " limit 5",
        Let('__tmp0, read("person"), // from person
          Let('__tmp1,    // where height > 60
            Filter.apply0[FLP](
              Free('__tmp0),
              Gt.apply0[FLP](
                ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("height"))),
                Constant(Data.Int(60)))),
            Let('__tmp2,    // group by gender, height
              GroupBy.apply0[FLP](
                Free('__tmp1),
                MakeArrayN[Fix](
                  ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("gender"))),
                  ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("height"))))),
              Let('__tmp3,
                Squash.apply0(    // select height*2.54 as cm
                  makeObj(
                    "cm" ->
                      Multiply.apply0[FLP](
                        Arbitrary.apply0[FLP](
                          ObjectProject.apply0[FLP](
                            Filter.apply0[FLP](  // having count(*) > 10
                              Free('__tmp2),
                              Gt.apply0[FLP](Count.apply0(Free('__tmp2)), Constant(Data.Int(10)))),
                            Constant(Data.Str("height")))),
                        Constant(Data.Dec(2.54))))),
                Take.apply0[FLP](
                  Drop.apply0[FLP](
                    OrderBy.apply0[FLP](  // order by cm
                      Free('__tmp3),
                      MakeArrayN[Fix](
                        ObjectProject.apply0(Free('__tmp3), Constant(Data.Str("cm")))),
                      MakeArrayN(
                        Constant(Data.Str("ASC")))),
                  Constant(Data.Int(10))), // offset 10
                Constant(Data.Int(5))))))))    // limit 5
    }

    "compile simple sum" in {
      testLogicalPlanCompile(
        "select sum(height) from person",
        Squash.apply0(
          makeObj(
            "0" ->
              Sum.apply0[FLP](ObjectProject.apply0(read("person"), Constant(Data.Str("height")))))))
    }

    "compile simple inner equi-join" in {
      testLogicalPlanCompile(
        "select foo.name, bar.address from foo join bar on foo.id = bar.foo_id",
        Let('__tmp0, read("foo"),
          Let('__tmp1, read("bar"),
            Let('__tmp2,
              InnerJoin.apply0[FLP](Free('__tmp0), Free('__tmp1),
                relations.Eq.apply0[FLP](
                  ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("id"))),
                  ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("foo_id"))))),
              Squash.apply0(
                makeObj(
                  "name" ->
                    ObjectProject.apply0[FLP](
                      ObjectProject.apply0(Free('__tmp2), Constant(Data.Str("left"))),
                      Constant(Data.Str("name"))),
                  "address" ->
                    ObjectProject.apply0[FLP](
                      ObjectProject.apply0(Free('__tmp2), Constant(Data.Str("right"))),
                      Constant(Data.Str("address")))))))))
    }

    "compile cross join to the equivalent inner equi-join" in {
      val query = "select foo.name, bar.address from foo, bar where foo.id = bar.foo_id"
      val equiv = "select foo.name, bar.address from foo join bar on foo.id = bar.foo_id"

      testLogicalPlanCompile(query, compileExp(equiv))
    }

    "compile inner join with additional equi-condition to the equivalent inner equi-join" in {
      val query = "select foo.name, bar.address from foo join bar on foo.id = bar.foo_id where foo.x = bar.y"
      val equiv = "select foo.name, bar.address from foo join bar on foo.id = bar.foo_id and foo.x = bar.y"

      testLogicalPlanCompile(query, compileExp(equiv))
    }

    "compile inner non-equi join to the equivalent cross join" in {
      val query = "select foo.name, bar.address from foo join bar on foo.x < bar.y"
      val equiv = "select foo.name, bar.address from foo, bar where foo.x < bar.y"

      testLogicalPlanCompile(query, compileExp(equiv))
    }

    "compile nested cross join to the equivalent inner equi-join" in {
      val query = "select a.x, b.y, c.z from a, b, c where a.id = b.a_id and b._id = c.b_id"
      val equiv = "select a.x, b.y, c.z from (a join b on a.id = b.a_id) join c on b._id = c.b_id"

      testLogicalPlanCompile(query, compileExp(equiv))
    }.pendingUntilFixed("SD-1190")

    "compile filtered cross join with one-sided conditions" in {
      val query = "select foo.name, bar.address from foo, bar where foo.id = bar.foo_id and foo.x < 10 and bar.y = 20"

      // NB: this query produces what we want but currently doesn't match due to spurious SQUASHes
      // val equiv = "select foo.name, bar.address from (select * from foo where x < 10) foo join (select * from bar where y = 20) bar on foo.id = bar.foo_id"

      testLogicalPlanCompile(query,
        Let('__tmp0, read("foo"),
          Let('__tmp1,
             Fix(Filter.apply0(
               Free('__tmp0),
               Fix(Lt.apply0(
                 ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("x"))),
                 Constant(Data.Int(10)))))),
             Let('__tmp2, read("bar"),
               Let('__tmp3,
                 Fix(Filter.apply0(
                   Free('__tmp2),
                   Fix(Eq.apply0(
                     ObjectProject.apply0(Free('__tmp2), Constant(Data.Str("y"))),
                     Constant(Data.Int(20)))))),
                  Let('__tmp4,
                    Fix(InnerJoin.apply0(Free('__tmp1), Free('__tmp3),
                      Fix(Eq.apply0(
                        ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("id"))),
                        ObjectProject.apply0(Free('__tmp3), Constant(Data.Str("foo_id"))))))),
                    Fix(Squash.apply0(
                       makeObj(
                         "name" -> ObjectProject.apply0[FLP](
                           ObjectProject.apply0(Free('__tmp4), Constant(Data.Str("left"))),
                           Constant(Data.Str("name"))),
                         "address" -> ObjectProject.apply0[FLP](
                           ObjectProject.apply0(Free('__tmp4), Constant(Data.Str("right"))),
                           Constant(Data.Str("address"))))))))))))
    }

    "compile filtered join with one-sided conditions" in {
      val query = "select foo.name, bar.address from foo join bar on foo.id = bar.foo_id where foo.x < 10 and bar.y = 20"

      // NB: this should be identical to the same query written as a cross join
      // (but cannot be written as in "must compile to the equivalent ..." style
      // because both require optimization)

      testLogicalPlanCompile(query,
        Let('__tmp0, read("foo"),
          Let('__tmp1, read("bar"),
            Let('__tmp2,
              Fix(Filter.apply0(
                Free('__tmp0),
                Fix(Lt.apply0(
                  ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("x"))),
                  Constant(Data.Int(10)))))),
              Let('__tmp3,
                Fix(Filter.apply0(
                  Free('__tmp1),
                  Fix(Eq.apply0(
                    ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("y"))),
                    Constant(Data.Int(20)))))),
                Let('__tmp4,
                  Fix(InnerJoin.apply0(Free('__tmp2), Free('__tmp3),
                    Fix(Eq.apply0(
                      ObjectProject.apply0(Free('__tmp2), Constant(Data.Str("id"))),
                      ObjectProject.apply0(Free('__tmp3), Constant(Data.Str("foo_id"))))))),
                  Fix(Squash.apply0(
                    makeObj(
                      "name" -> ObjectProject.apply0[FLP](
                        ObjectProject.apply0(Free('__tmp4), Constant(Data.Str("left"))),
                        Constant(Data.Str("name"))),
                      "address" -> ObjectProject.apply0[FLP](
                        ObjectProject.apply0(Free('__tmp4), Constant(Data.Str("right"))),
                        Constant(Data.Str("address"))))))))))))
    }

    "compile simple left ineq-join" in {
      testLogicalPlanCompile(
        "select foo.name, bar.address " +
          "from foo left join bar on foo.id < bar.foo_id",
        Let('__tmp0, read("foo"),
          Let('__tmp1, read("bar"),
            Let('__tmp2,
              LeftOuterJoin.apply0[FLP](Free('__tmp0), Free('__tmp1),
                relations.Lt.apply0[FLP](
                  ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("id"))),
                  ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("foo_id"))))),
              Squash.apply0(
                makeObj(
                  "name" ->
                    ObjectProject.apply0[FLP](
                      ObjectProject.apply0(Free('__tmp2), Constant(Data.Str("left"))),
                      Constant(Data.Str("name"))),
                  "address" ->
                    ObjectProject.apply0[FLP](
                      ObjectProject.apply0(Free('__tmp2), Constant(Data.Str("right"))),
                      Constant(Data.Str("address")))))))))
    }

    "compile complex equi-join" in {
      testLogicalPlanCompile(
        "select foo.name, bar.address " +
          "from foo join bar on foo.id = bar.foo_id " +
          "join baz on baz.bar_id = bar.id",
        Let('__tmp0, read("foo"),
          Let('__tmp1, read("bar"),
            Let('__tmp2,
              InnerJoin.apply0[FLP](Free('__tmp0), Free('__tmp1),
                relations.Eq.apply0[FLP](
                  ObjectProject.apply0(
                    Free('__tmp0),
                    Constant(Data.Str("id"))),
                  ObjectProject.apply0(
                    Free('__tmp1),
                    Constant(Data.Str("foo_id"))))),
              Let('__tmp3, read("baz"),
                Let('__tmp4,
                  InnerJoin.apply0[FLP](Free('__tmp2), Free('__tmp3),
                    relations.Eq.apply0[FLP](
                      ObjectProject.apply0(Free('__tmp3),
                        Constant(Data.Str("bar_id"))),
                      ObjectProject.apply0[FLP](
                        ObjectProject.apply0(Free('__tmp2),
                          Constant(Data.Str("right"))),
                        Constant(Data.Str("id"))))),
                  Squash.apply0(
                    makeObj(
                      "name" ->
                        ObjectProject.apply0[FLP](
                          ObjectProject.apply0[FLP](
                            ObjectProject.apply0(Free('__tmp4), Constant(Data.Str("left"))),
                            Constant(Data.Str("left"))),
                          Constant(Data.Str("name"))),
                      "address" ->
                        ObjectProject.apply0[FLP](
                          ObjectProject.apply0[FLP](
                            ObjectProject.apply0(Free('__tmp4), Constant(Data.Str("left"))),
                            Constant(Data.Str("right"))),
                          Constant(Data.Str("address")))))))))))
    }

    "compile sub-select in filter" in {
      testLogicalPlanCompile(
        "select city, pop from zips where pop > (select avg(pop) from zips)",
        read("zips"))
    }.pendingUntilFixed

    "compile simple sub-select" in {
      testLogicalPlanCompile(
        "select temp.name, temp.size from (select zips.city as name, zips.pop as size from zips) as temp",
        Let('__tmp0, read("zips"),
          Let('__tmp1,
            Squash.apply0(
              makeObj(
                "name" -> ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("city"))),
                "size" -> ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("pop"))))),
            Squash.apply0(
              makeObj(
                "name" -> ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("name"))),
                "size" -> ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("size"))))))))
    }

    "compile sub-select with same un-qualified names" in {
      testLogicalPlanCompile(
        "select city, pop from (select city, pop from zips) as temp",
        Let('__tmp0, read("zips"),
          Let('__tmp1,
            Squash.apply0(
              makeObj(
                "city" -> ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("city"))),
                "pop" -> ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("pop"))))),
            Squash.apply0(
              makeObj(
                "city" -> ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("city"))),
                "pop" -> ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("pop"))))))))
    }

    "compile simple distinct" in {
      testLogicalPlanCompile(
        "select distinct city from zips",
        Distinct.apply0[FLP](
          Squash.apply0(
            makeObj(
              "city" -> ObjectProject.apply0(read("zips"), Constant(Data.Str("city")))))))
    }

    "compile simple distinct ordered" in {
      testLogicalPlanCompile(
        "select distinct city from zips order by city",
        Let('__tmp0,
          Squash.apply0(
            makeObj(
              "city" ->
                ObjectProject.apply0(read("zips"), Constant(Data.Str("city"))))),
          Distinct.apply0[FLP](
            OrderBy.apply0[FLP](
              Free('__tmp0),
              MakeArrayN[Fix](
                ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("city")))),
              MakeArrayN(
                Constant(Data.Str("ASC")))))))
    }

    "compile distinct with unrelated order by" in {
      testLogicalPlanCompile(
        "select distinct city from zips order by pop desc",
        Let('__tmp0,
          read("zips"),
          Let('__tmp1,
            Squash.apply0(
              makeObj(
                "city" -> ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("city"))),
                "__sd__0" -> ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("pop"))))),
            Let('__tmp2,
              OrderBy.apply0[FLP](
                Free('__tmp1),
                MakeArrayN[Fix](
                  ObjectProject.apply0(Free('__tmp1), Constant(Data.Str("__sd__0")))),
                MakeArrayN(
                  Constant(Data.Str("DESC")))),
              DeleteField.apply0[FLP](
                DistinctBy.apply0[FLP](Free('__tmp2),
                  DeleteField.apply0(Free('__tmp2), Constant(Data.Str("__sd__0")))),
                Constant(Data.Str("__sd__0")))))))
    }

    "compile count(distinct(...))" in {
      testLogicalPlanCompile(
        "select count(distinct(lower(city))) from zips",
        Squash.apply0(
          makeObj(
            "0" -> Count.apply0[FLP](Distinct.apply0[FLP](Lower.apply0[FLP](ObjectProject.apply0(read("zips"), Constant(Data.Str("city")))))))))
    }

    "compile simple distinct with two named projections" in {
      testLogicalPlanCompile(
        "select distinct city as CTY, state as ST from zips",
        Let('__tmp0, read("zips"),
          Distinct.apply0[FLP](
            Squash.apply0(
              makeObj(
                "CTY" -> ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("city"))),
                "ST" -> ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("state"))))))))
    }

    "compile count distinct with two exprs" in {
      testLogicalPlanCompile(
        "select count(distinct city, state) from zips",
        read("zips"))
    }.pendingUntilFixed

    "compile distinct as function" in {
      testLogicalPlanCompile(
        "select distinct(city, state) from zips",
        read("zips"))
    }.pendingUntilFixed

    "fail with ambiguous reference" in {
      compile("select foo from bar, baz") must beLeftDisjunction
    }

    "fail with ambiguous reference in cond" in {
      compile("select (case when a = 1 then 'ok' else 'reject' end) from bar, baz") must beLeftDisjunction
    }

    "fail with ambiguous reference in else" in {
      compile("select (case when bar.a = 1 then 'ok' else foo end) from bar, baz") must beLeftDisjunction
    }

    "fail with duplicate alias" in {
      compile("select car.name as name, owner.name as name from owners as owner join cars as car on car._id = owner.carId") must
        beLeftDisjunction("DuplicateAlias(name)")
    }

    "translate free variable" in {
      testLogicalPlanCompile("select name from zips where age < :age",
        Let('__tmp0, read("zips"),
          Squash.apply0(
            makeObj(
              "name" ->
                ObjectProject.apply0[FLP](
                  Filter.apply0[FLP](
                    Free('__tmp0),
                    Lt.apply0[FLP](
                      ObjectProject.apply0(Free('__tmp0), Constant(Data.Str("age"))),
                      Free('age))),
                  Constant(Data.Str("name")))))))
    }
  }

  "error when too few arguments passed to a function" in {
    fullCompile("""select substring("foo") from zips""")
      .toEither must beLeft(contain("3,1"))
  }

  "error when too many arguments passed to a function" in {
    fullCompile("select count(*, 1, 2, 4) from zips")
      .toEither must beLeft(contain("1,4"))
  }

  "reduceGroupKeys" should {
    import Compiler.reduceGroupKeys

    "insert ARBITRARY" in {
      val lp =
        Let('tmp0, read("zips"),
          Let('tmp1,
            GroupBy.apply0[FLP](
              Free('tmp0),
              MakeArrayN[Fix](ObjectProject.apply0(Free('tmp0), Constant(Data.Str("city"))))),
            ObjectProject.apply0(Free('tmp1), Constant(Data.Str("city")))))
      val exp =
        Let('tmp0, read("zips"),
          Arbitrary.apply0[FLP](
            ObjectProject.apply0[FLP](
              GroupBy.apply0[FLP](
                Free('tmp0),
                MakeArrayN[Fix](ObjectProject.apply0(Free('tmp0), Constant(Data.Str("city"))))), Constant(Data.Str("city")))))

      reduceGroupKeys(lp) must equalToPlan(exp)
    }

    "insert ARBITRARY with intervening filter" in {
      val lp =
        Let('tmp0, read("zips"),
          Let('tmp1,
            GroupBy.apply0[FLP](
              Free('tmp0),
              MakeArrayN[Fix](ObjectProject.apply0(Free('tmp0), Constant(Data.Str("city"))))),
            Let('tmp2,
              Filter.apply0[FLP](Free('tmp1), Gt.apply0[FLP](Count.apply0(Free('tmp1)), Constant(Data.Int(10)))),
              ObjectProject.apply0(Free('tmp2), Constant(Data.Str("city"))))))
      val exp =
        Let('tmp0, read("zips"),
          Let('tmp1,
            GroupBy.apply0[FLP](
              Free('tmp0),
              MakeArrayN[Fix](ObjectProject.apply0(Free('tmp0), Constant(Data.Str("city"))))),
            Arbitrary.apply0[FLP](
              ObjectProject.apply0[FLP](
                Filter.apply0[FLP](
                  Free('tmp1),
                  Gt.apply0[FLP](Count.apply0(Free('tmp1)), Constant(Data.Int(10)))),
                Constant(Data.Str("city"))))))

      reduceGroupKeys(lp) must equalToPlan(exp)
    }

    "not insert redundant Reduction" in {
      val lp =
        Let('tmp0, read("zips"),
          Count.apply0[FLP](
            ObjectProject.apply0[FLP](
              GroupBy.apply0[FLP](
                Free('tmp0),
                MakeArrayN[Fix](ObjectProject.apply0(Free('tmp0),
                  Constant(Data.Str("city"))))), Constant(Data.Str("city")))))

      reduceGroupKeys(lp) must equalToPlan(lp)
    }

    "insert ARBITRARY with multiple keys and mixed projections" in {
      val lp =
        Let('tmp0,
          read("zips"),
          Let('tmp1,
            GroupBy.apply0[FLP](
              Free('tmp0),
              MakeArrayN[Fix](
                ObjectProject.apply0(Free('tmp0), Constant(Data.Str("city"))),
                ObjectProject.apply0(Free('tmp0), Constant(Data.Str("state"))))),
            makeObj(
              "city" -> ObjectProject.apply0(Free('tmp1), Constant(Data.Str("city"))),
              "1"    -> Count.apply0[FLP](ObjectProject.apply0(Free('tmp1), Constant(Data.Str("state")))),
              "loc"  -> ObjectProject.apply0(Free('tmp1), Constant(Data.Str("loc"))),
              "2"    -> Sum.apply0[FLP](ObjectProject.apply0(Free('tmp1), Constant(Data.Str("pop")))))))
      val exp =
        Let('tmp0,
          read("zips"),
          Let('tmp1,
            GroupBy.apply0[FLP](
              Free('tmp0),
              MakeArrayN[Fix](
                ObjectProject.apply0(Free('tmp0), Constant(Data.Str("city"))),
                ObjectProject.apply0(Free('tmp0), Constant(Data.Str("state"))))),
            makeObj(
              "city" -> Arbitrary.apply0[FLP](ObjectProject.apply0(Free('tmp1), Constant(Data.Str("city")))),
              "1"    -> Count.apply0[FLP](ObjectProject.apply0(Free('tmp1), Constant(Data.Str("state")))),
              "loc"  -> ObjectProject.apply0(Free('tmp1), Constant(Data.Str("loc"))),
              "2"    -> Sum.apply0[FLP](ObjectProject.apply0(Free('tmp1), Constant(Data.Str("pop")))))))

      reduceGroupKeys(lp) must equalToPlan(exp)
    }
  }

  "constant folding" >> {
    def testFolding(name: String, query: String, expected: String) = {
      s"${name}" >> {
        testTypedLogicalPlanCompile(query, fullCompileExp(expected))
      }

      s"${name} with collection" >> {
        testTypedLogicalPlanCompile(s"$query from zips", fullCompileExp(expected))
      }
    }

    testFolding("ARBITRARY",
      "select arbitrary((3, 4, 5))",
      "select 3")

    testFolding("AVG",
      "select avg((0.5, 1.0, 4.5))",
      "select 2.0")

    testFolding("COUNT",
      """select count(("foo", "quux", "baz"))""",
      "select 3")

    testFolding("MAX",
      "select max((4, 2, 1001, 17))",
      "select 1001")

    testFolding("MIN",
      "select min((4, 2, 1001, 17))",
      "select 2")

    testFolding("SUM",
      "select sum((1, 1, 1, 1, 1, 3, 4))",
      "select 12")
  }

  List("avg", "sum") foreach { fn =>
    s"passing a literal set of the wrong type to '${fn.toUpperCase}' fails" >> {
      fullCompile(s"""select $fn(("one", "two", "three"))""") must beLeftDisjunction
    }
  }
}
