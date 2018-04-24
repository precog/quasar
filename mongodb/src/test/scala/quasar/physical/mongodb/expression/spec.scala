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

package quasar.physical.mongodb.expression

import scala.Predef.$conforms
import slamdata.Predef._
import quasar.physical.mongodb.{Bson, BsonField}

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.scalacheck._, Arbitrary.arbitrary
import scalaz._
import scalacheck.ScalazArbitrary._

object ArbitraryExprOp {

  val fp3_2: ExprOpCoreF.fixpoint[Fix[Expr3_2], Expr3_2] =
    new ExprOpCoreF.fixpoint[Fix[Expr3_2], Expr3_2](_.embed)
  import fp3_2._

  implicit val formatSpecifierArbitrary: Arbitrary[FormatSpecifier] = Arbitrary {
    import FormatSpecifier._
    Gen.oneOf(
      Year, Month, DayOfMonth,
      Hour, Minute, Second, Millisecond,
      DayOfYear, DayOfWeek, WeekOfYear)
  }

  implicit val formatStringArbitrary: Arbitrary[FormatString] = Arbitrary {
    arbitrary[List[String \/ FormatSpecifier]].map(FormatString(_))
  }

  lazy val genSimpleExpr: Gen[Fix[Expr3_2]] =
    Gen.oneOf(
      arbitrary[Int].map(x => $literal(Bson.Int32(x))),
      Gen.alphaChar.map(c => $var(DocField(BsonField.Name(c.toString)))),
      arbitrary[FormatString].map(fmt =>
        $dateToString(fmt, $var(DocField(BsonField.Name("date"))))))

  lazy val genExpr: Gen[Fix[Expr3_2]] = {
    Gen.oneOf(
      genSimpleExpr.flatMap(x => Gen.oneOf(
        $sqrt(x),
        $abs(x),
        $log10(x),
        $ln(x),
        $trunc(x),
        $ceil(x),
        $floor(x))),
      for {
        x <- genSimpleExpr
        y <- genSimpleExpr
        expr <- Gen.oneOf(
          $log(x, y),
          $pow(x, y))
      } yield expr)
  }

  lazy val genExpr3_4: Gen[Fix[Expr3_4]] = {
    def inj3_2(expr: Fix[Expr3_2]) = expr.transCata[Fix[Expr3_4]](Inject[Expr3_2, Expr3_4])
    genExpr.map(inj3_2)
  }

  lazy val genExpr3_4_4: Gen[Fix[Expr3_4_4]] = {
    def inj3_4(expr: Fix[Expr3_4]) = expr.transCata[Fix[Expr3_4_4]](Inject[Expr3_4, Expr3_4_4])
    genExpr3_4.map(inj3_4)
  }

  lazy val genExpr3_6: Gen[Fix[Expr3_6]] = {
    def inj3_4_4(expr: Fix[Expr3_4_4]) = expr.transCata[Fix[Expr3_6]](Inject[Expr3_4_4, Expr3_6])
    genExpr3_4_4.map(inj3_4_4)
  }
}

class ExpressionSpec extends quasar.Qspec {
  import fixExprOp._

  val ops = ExprOpOps[ExprOp]

  "Expression" should {
    def literal(value: Bson): Bson = $literal(value).cata(ops.bson)

    "escape literal string with $" in {
      val x = Bson.Text("$1")
      literal(x) must_== Bson.Doc(ListMap("$literal" -> x))
    }

    "escape literal string with no leading '$'" in {
      val x = Bson.Text("abc")
      literal(x) must_== Bson.Doc(ListMap("$literal" -> x))
    }

    "escape simple integer literal" in {
      val x = Bson.Int32(0)
      literal(x) must_== Bson.Doc(ListMap("$literal" -> x))
    }

    "escape simple array literal" in {
      val x = Bson.Arr(Bson.Text("abc") :: Bson.Int32(0) :: Nil)
      literal(x) must_== Bson.Doc(ListMap("$literal" -> x))
    }

    "escape string nested in array" in {
      val x = Bson.Arr(Bson.Text("$1") :: Nil)
      literal(x) must_== Bson.Doc(ListMap("$literal" -> x))
    }

    "escape simple doc literal" in {
      val x = Bson.Doc(ListMap("a" -> Bson.Text("b")))
      literal(x) must_== Bson.Doc(ListMap("$literal" -> x))
    }

    "escape string nested in doc" in {
      val x = Bson.Doc(ListMap("a" -> Bson.Text("$1")))
      literal(x) must_== Bson.Doc(ListMap("$literal" -> x))
    }

    "render $$ROOT" in {
      DocVar.ROOT().bson must_== Bson.Text("$$ROOT")
    }

    "treat DocField as alias for DocVar.ROOT()" in {
      DocField(BsonField.Name("foo")) must_== DocVar.ROOT(BsonField.Name("foo"))
    }

    "render $foo under $$ROOT" in {
      DocVar.ROOT(BsonField.Name("foo")).bson must_== Bson.Text("$foo")
    }

    "render $foo.bar under $$CURRENT" in {
      DocVar.CURRENT(BsonField.Name("foo") \ BsonField.Name("bar")).bson must_== Bson.Text("$$CURRENT.foo.bar")
    }
  }

  "FormatSpecifier" should {
    import FormatSpecifier._

    def toBson(fmt: FormatString): Bson =
      $dateToString(fmt, $var(DocField(BsonField.Name("date"))))
        .cata(ops.bson)

    def expected(str: String): Bson =
      Bson.Doc(ListMap(
        "$dateToString" -> Bson.Doc(ListMap(
          "format" -> Bson.Text(str),
          "date" -> Bson.Text("$date")))))

    "match first example from mongodb docs" in {
      toBson(Year :: "-" :: Month :: "-" :: DayOfMonth :: FormatString.empty) must_==
        expected("%Y-%m-%d")
    }

    "match second example from mongodb docs" in {
      toBson(Hour :: ":" :: Minute :: ":" :: Second :: ":" :: Millisecond :: FormatString.empty) must_==
        expected("%H:%M:%S:%L")
    }

    "escape `%`s" in {
      toBson(Hour :: "%" :: Minute :: "%" :: Second :: FormatString.empty) must_==
        expected("%H%%%M%%%S")
    }
  }
}
