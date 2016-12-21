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
import quasar.contrib.pathy._, PathArbitrary._
import quasar.sql.fixpoint._

import matryoshka.data.Fix
import org.scalacheck.{Arbitrary, Gen}
import java.time.{Duration, Instant}
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._

trait ExprArbitrary {
  implicit val exprArbitrary: Arbitrary[Fix[Sql]] = Arbitrary(selectGen(4))

  private def selectGen(depth: Int): Gen[Fix[Sql]] = for {
    isDistinct <- Gen.oneOf(SelectDistinct, SelectAll)
    projs      <- smallNonEmptyListOf(projGen)
    relations  <- Gen.option(relationGen(depth-1))
    filter     <- Gen.option(exprGen(depth-1))
    groupBy    <- Gen.option(groupByGen(depth-1))
    orderBy    <- Gen.option(orderByGen(depth-1))
  } yield SelectR(isDistinct, projs, relations, filter, groupBy, orderBy)

  private def projGen: Gen[Proj[Fix[Sql]]] =
    Gen.oneOf(
      Gen.const(Proj(SpliceR(None), None)),
      exprGen(1) >>= (x =>
        Gen.oneOf(
          Gen.const(Proj(x, None)),
          Gen.oneOf(
            Gen.alphaChar.map(_.toString),
            Gen.const("public enemy #1"),
            Gen.const("I quote: \"foo\"")) ∘
            (n => Proj(x, Some(n))))))

  private def relationGen(depth: Int): Gen[SqlRelation[Fix[Sql]]] = {
    val simple = for {
      n <- Arbitrary.arbitrary[FPath]
      a <- Gen.option(Gen.alphaChar.map(_.toString))
    } yield TableRelationAST[Fix[Sql]](pathy.Path.unsandbox(n), a)
    if (depth <= 0) simple
    else Gen.frequency(
      5 -> simple,
      1 -> (selectGen(2) ⊛ Gen.alphaChar)((s, c) =>
        ExprRelationAST(s, c.toString)),
      1 -> (relationGen(depth-1) ⊛ relationGen(depth-1))(CrossRelation(_, _)),
      1 -> (relationGen(depth-1) ⊛ relationGen(depth-1) ⊛
        Gen.oneOf(LeftJoin, RightJoin, InnerJoin, FullJoin) ⊛
        exprGen(1))(
        JoinRelation(_, _, _, _)))
  }

  private def groupByGen(depth: Int): Gen[GroupBy[Fix[Sql]]] =
    (smallNonEmptyListOf(exprGen(depth)) ⊛ Gen.option(exprGen(depth)))(
      GroupBy(_, _))

  private def orderByGen(depth: Int): Gen[OrderBy[Fix[Sql]]] = {
    val order = Gen.oneOf(ASC, DESC) tuple exprGen(depth)
    (order ⊛ smallNonEmptyListOf(order))((o, os) => OrderBy(NonEmptyList(o, os: _*)))
  }

  private def exprGen(depth: Int): Gen[Fix[Sql]] = Gen.lzy {
    if (depth <= 0) simpleExprGen
    else complexExprGen(depth-1)
  }

  private def simpleExprGen: Gen[Fix[Sql]] =
    Gen.frequency(
      2 -> (for {
        n <- Gen.alphaChar.map(_.toString)
      } yield VariR(n)),
      1 -> (for {
        n  <- Gen.chooseNum(2, 5) // Note: at least two, to be valid set syntax
        cs <- Gen.listOfN(n, constExprGen)
      } yield SetLiteralR(cs)),
      10 -> Gen.oneOf(
        Gen.alphaChar.map(_.toString),
        Gen.const("name, address"),
        Gen.const("q: \"a\"")) ∘
        (IdentR(_)),
      1 -> InvokeFunctionR("timestamp", List(StringLiteralR(Instant.now.toString))),
      1 -> Gen.choose(0L, 10000000000L).map(millis => InvokeFunctionR("interval", List(StringLiteralR(Duration.ofMillis(millis).toString)))),
      1 -> InvokeFunctionR("date", List(StringLiteralR("2014-11-17"))),
      1 -> InvokeFunctionR("time", List(StringLiteralR("12:00:00"))),
      1 -> InvokeFunctionR("oid", List(StringLiteralR("123456")))
    )

  private def complexExprGen(depth: Int): Gen[Fix[Sql]] =
    Gen.frequency(
      5 -> simpleExprGen,
      1 -> Gen.lzy(selectGen(depth-1)),
      1 -> exprGen(depth) ∘ (expr => SpliceR(Some(expr))),
      3 -> (exprGen(depth) ⊛ exprGen(depth) ⊛
        Gen.oneOf(
          Or, And, Eq, Neq, Ge, Gt, Le, Lt,
          Plus, Minus, Mult, Div, Mod, Pow,
          In))(
        BinopR(_, _, _)),
      1 -> (exprGen(depth) ⊛ exprGen(depth))(BinopR(_, _, FieldDeref)),
      1 -> (exprGen(depth) ⊛ exprGen(depth))(BinopR(_, _, IndexDeref)),
      2 -> (exprGen(depth) ⊛
        Gen.oneOf(
          Not, Exists, Positive, Negative, Distinct,
          FlattenMapKeys,   FlattenArrayIndices,
          FlattenMapValues, FlattenArrayValues,
          ShiftMapKeys,     ShiftArrayIndices,
          ShiftMapValues,   ShiftArrayValues))(
        UnopR(_, _)),
      2 -> (Gen.oneOf("sum", "count", "avg", "length", "make_array") ⊛ exprGen(depth))(
        (fn, arg) => InvokeFunctionR(fn, List(arg))),
      1 -> exprGen(depth) ∘
        (arg => InvokeFunctionR("like", List(arg, StringLiteralR("B%"), StringLiteralR("")))),
      1 -> (exprGen(depth) ⊛ casesGen(depth) ⊛ Gen.option(exprGen(depth)))(
        MatchR(_, _, _)),
      1 -> (casesGen(depth) ⊛ Gen.option(exprGen(depth)))(SwitchR(_, _))
    )

  private def casesGen(depth: Int): Gen[List[Case[Fix[Sql]]]] =
    smallNonEmptyListOf((exprGen(depth) ⊛ exprGen(depth))(Case (_, _)))

  def constExprGen: Gen[Fix[Sql]] =
    Gen.oneOf(
      // NB: negative numbers are parsed as UnopR(-, _)
      Gen.chooseNum(0, Long.MaxValue).flatMap(IntLiteralR(_)),
      Gen.chooseNum(0.0, 10.0).flatMap(FloatLiteralR(_)),
      Gen.alphaStr.flatMap(StringLiteralR(_)),
      // Note: only `"` gets special encoding; the rest should be accepted as is.
      for {
        s  <- Gen.choose(1, 5)
        cs <- Gen.listOfN(s, Gen.oneOf("\"", "\\", " ", "\n", "\t", "a", "b", "c"))
      } yield StringLiteralR(cs.mkString),
      Gen.const(NullLiteralR),
      Gen.const(BoolLiteralR(true)),
      Gen.const(BoolLiteralR(false)))

  /** Generates non-empty lists which grow based on the `size` parameter, but
    * slowly (log), so that trees built out of the lists don't get
    * exponentially big.
    */
  private def smallNonEmptyListOf[A](gen: Gen[A]): Gen[List[A]] = {
    def log2(x: Int): Int = (java.lang.Math.log(x.toDouble)/java.lang.Math.log(2)).toInt
    for {
      sz <- Gen.size
      n  <- Gen.choose(1, log2(sz))
      l  <- Gen.listOfN(n, gen)
    } yield l
  }
}

object ExprArbitrary extends ExprArbitrary
