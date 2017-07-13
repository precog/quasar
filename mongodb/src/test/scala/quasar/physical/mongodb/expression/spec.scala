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

package quasar.physical.mongodb.expression

import scala.Predef.$conforms
import slamdata.Predef._
import quasar.fp._
import quasar.physical.mongodb.{Bson, BsonField}

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.scalacheck._, Arbitrary.arbitrary
import scalaz._
import scalacheck.ScalazArbitrary._

object ArbitraryExprOp {

  val fpCore: ExprOpCoreF.fixpoint[Fix[Expr2_6], Expr2_6] =
    new ExprOpCoreF.fixpoint[Fix[Expr2_6], Expr2_6](_.embed)
  import fpCore._
  val fp3_0: ExprOp3_0F.fixpoint[Fix[Expr3_0], Expr3_0] =
    new ExprOp3_0F.fixpoint[Fix[Expr3_0], Expr3_0](_.embed)
  import fp3_0._
  val fp3_2: ExprOp3_2F.fixpoint[Fix[Expr3_2], Expr3_2] =
    new ExprOp3_2F.fixpoint[Fix[Expr3_2], Expr3_2](_.embed)
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

  lazy val genExpr: Gen[Fix[Expr2_6]] =
    Gen.oneOf(
      arbitrary[Int].map(x => $literal(Bson.Int32(x))),
      Gen.alphaChar.map(c => $var(DocField(BsonField.Name(c.toString)))))

  lazy val genExpr3_0: Gen[Fix[Expr3_0]] = {
    def inj(expr: Fix[Expr2_6]) = expr.transCata[Fix[Expr3_0]](Inject[Expr2_6, Expr3_0])
    Gen.oneOf(
      genExpr.map(inj),
      arbitrary[FormatString].map(fmt =>
        $dateToString(fmt, inj($var(DocField(BsonField.Name("date")))))))
  }

  lazy val genExpr3_2: Gen[Fix[Expr3_2]] = {
    def inj(expr: Fix[Expr2_6])  = expr.transCata[Fix[Expr3_2]](Inject[Expr2_6, Expr3_2])
    def inj3_0(expr: Fix[Expr3_0]) = expr.transCata[Fix[Expr3_2]](Inject[Expr3_0, Expr3_2])
    Gen.oneOf(
      genExpr3_0.map(inj3_0),
      genExpr.map(inj).flatMap(x => Gen.oneOf(
        $sqrt(x),
        $abs(x),
        $log10(x),
        $ln(x),
        $trunc(x),
        $ceil(x),
        $floor(x))),
      for {
        x <- genExpr.map(inj)
        y <- genExpr.map(inj)
        expr <- Gen.oneOf(
          $log(x, y),
          $pow(x, y))
      } yield expr)
  }
}

class ExpressionSpec extends quasar.Qspec {
  import fixExprOp._
  val fp3_0 = new ExprOp3_0F.fixpoint[Fix[ExprOp], ExprOp](_.embed)
  import fp3_0._

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

  "toJs" should {
    import quasar.jscore._

    "handle addition with epoch date literal" in {
      $add(
        $literal(Bson.Date(0)),
        $var(DocField(BsonField.Name("epoch")))).para(toJs[Fix[ExprOp], ExprOp]) must beRightDisjunction(
        JsFn(JsFn.defaultName, New(Name("Date"), List(Select(Ident(JsFn.defaultName), "epoch")))))
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
