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
import quasar.std.StdLib._

import org.scalacheck.{Arbitrary, Gen}
import org.threeten.bp.{Duration, Instant}

trait ExprArbitrary {
  implicit val exprArbitrary: Arbitrary[Expr] =
    Arbitrary(selectGen(4))

  private def selectGen(depth: Int): Gen[Expr] = for {
    isDistinct <- Gen.oneOf(SelectDistinct, SelectAll)
    projs      <- smallNonEmptyListOf(projGen)
    relations  <- Gen.option(relationGen(depth-1))
    filter     <- Gen.option(exprGen(depth-1))
    groupBy    <- Gen.option(groupByGen(depth-1))
    orderBy    <- Gen.option(orderByGen(depth-1))
  } yield Select(isDistinct, projs, relations, filter, groupBy, orderBy)

  private def projGen: Gen[Proj[Expr]] =
    Gen.oneOf(
      Gen.const(Proj(Splice(None), None)),
      exprGen(1).flatMap(x =>
        Gen.oneOf(
          Gen.const(Proj(x, None)),
          for {
            n <- Gen.oneOf(
              Gen.alphaChar.map(_.toString),
              Gen.const("public enemy #1"),
              Gen.const("I quote: \"foo\""))
          } yield Proj(x, Some(n)))))

  private def relationGen(depth: Int): Gen[SqlRelation[Expr]] = {
    val simple = for {
        p <- Gen.oneOf(Nil, "" :: Nil, "." :: Nil)
        s <- Gen.choose(1, 3)
        n <- Gen.listOfN(s, Gen.alphaChar.map(_.toString)).map(ns => (p ++ ns).mkString("/"))
        a <- Gen.option(Gen.alphaChar.map(_.toString))
      } yield TableRelationAST[Expr](n, a)
    if (depth <= 0) simple
    else Gen.frequency(
      5 -> simple,
      1 -> (for {
        s <- selectGen(2)
        c <- Gen.alphaChar
      } yield ExprRelationAST(s, c.toString)),
      1 -> (for {
        l <- relationGen(depth-1)
        r <- relationGen(depth-1)
      } yield CrossRelation(l, r)),
      1 -> (for {
        l <- relationGen(depth-1)
        r <- relationGen(depth-1)
        t <- Gen.oneOf(LeftJoin, RightJoin, InnerJoin, FullJoin)
        x <- exprGen(1)
      } yield JoinRelation(l, r, t, x))
    )
  }

  private def groupByGen(depth: Int): Gen[GroupBy[Expr]] = for {
    keys   <- smallNonEmptyListOf(exprGen(depth))
    having <- Gen.option(exprGen(depth))
  } yield GroupBy(keys, having)

  private def orderByGen(depth: Int): Gen[OrderBy[Expr]] = smallNonEmptyListOf(for {
    expr <- exprGen(depth)
    ot   <- Gen.oneOf(ASC, DESC)
  } yield (ot, expr)).map(OrderBy(_))

  private def exprGen(depth: Int): Gen[Expr] = Gen.lzy {
    if (depth <= 0) simpleExprGen
    else complexExprGen(depth-1)
  }

  private def simpleExprGen: Gen[Expr] =
    Gen.frequency(
      2 -> (for {
        n <- Gen.alphaChar.map(_.toString)
      } yield Vari(n)),
      1 -> (for {
        n  <- Gen.chooseNum(2, 5)  // Note: at least two, to be valid set syntax
        cs <- Gen.listOfN(n, constExprGen)
      } yield SetLiteral(cs)),
      10 -> (for {
        n <- Gen.oneOf(
          Gen.alphaChar.map(_.toString),
          Gen.const("name, address"),
          Gen.const("q: \"a\""))
      } yield Ident(n)),
      1 -> Unop(StringLiteral(Instant.now.toString), ToTimestamp),
      1 -> Gen.choose(0L, 10000000000L).map(millis => Unop(StringLiteral(Duration.ofMillis(millis).toString), ToInterval)),
      1 -> Unop(StringLiteral("2014-11-17"), ToDate),
      1 -> Unop(StringLiteral("12:00:00"), ToTime),
      1 -> Unop(StringLiteral("123456"), ToId)
    )

  private def complexExprGen(depth: Int): Gen[Expr] =
    Gen.frequency(
      5 -> simpleExprGen,
      1 -> Gen.lzy(selectGen(depth-1)),
      1 -> (for {
        expr <- exprGen(depth)
      } yield Splice(Some(expr))),
      3 -> (for {
        l  <- exprGen(depth)
        r  <- exprGen(depth)
        op <- Gen.oneOf(
          Or, And, Eq, Neq, Ge, Gt, Le, Lt,
          Plus, Minus, Mult, Div, Mod, Pow,
          In)
      } yield Binop(l, r, op)),
      1 -> (for {
        l <- exprGen(depth)
        n <- exprGen(depth)
      } yield Binop(l, n, FieldDeref)),
      1 -> (for {
        l <- exprGen(depth)
        i <- exprGen(depth)
      } yield Binop(l, i, IndexDeref)),
      2 -> (for {
        x  <- exprGen(depth)
        op <- Gen.oneOf(
          Not, Exists, Positive, Negative, Distinct,
          ToDate, ToInterval,
          FlattenMapKeys,   FlattenArrayIndices,
          FlattenMapValues, FlattenArrayValues,
          ShiftMapKeys,     ShiftArrayIndices,
          ShiftMapValues,   ShiftArrayValues,
          IsNull)
      } yield Unop(x, op)),
      2 -> (for {
        fn  <- Gen.oneOf(agg.Sum, agg.Count, agg.Avg, string.Length, structural.MakeArray)
        arg <- exprGen(depth)
      } yield InvokeFunction(fn.name, List(arg))),
      1 -> (for {
        arg <- exprGen(depth)
      } yield InvokeFunction(string.Like.name, List(arg, StringLiteral("B%"), StringLiteral("")))),
      1 -> (for {
        expr  <- exprGen(depth)
        cases <- casesGen(depth)
        dflt  <- Gen.option(exprGen(depth))
      } yield Match(expr, cases, dflt)),
      1 -> (for {
        cases <- casesGen(depth)
        dflt  <- Gen.option(exprGen(depth))
      } yield Switch(cases, dflt))
    )

  private def casesGen(depth: Int): Gen[List[Case[Expr]]] =
    smallNonEmptyListOf(for {
        cond <- exprGen(depth)
        expr <- exprGen(depth)
      } yield Case(cond, expr))

  def constExprGen: Gen[Expr] =
    Gen.oneOf(
      Gen.chooseNum(0, 100).flatMap(IntLiteral(_)),       // Note: negative numbers are parsed as Unop(-, _)
      Gen.chooseNum(0.0, 10.0).flatMap(FloatLiteral(_)),  // Note: negative numbers are parsed as Unop(-, _)
      Gen.alphaStr.flatMap(StringLiteral(_)),
      // Note: only `'` gets special encoding; the rest should be accepted as is.
      for {
        s  <- Gen.choose(1, 5)
        cs <- Gen.listOfN(s, Gen.oneOf("'", "\\", " ", "\n", "\t", "a", "b", "c"))
      } yield StringLiteral(cs.mkString),
      Gen.const(NullLiteral()),
      Gen.const(BoolLiteral(true)),
      Gen.const(BoolLiteral(false)))

  /** Generates non-empty lists which grow based on the `size` parameter, but
    * slowly (log), so that trees built out of the lists don't get
    * exponentially big.
    */
  private def smallNonEmptyListOf[A](gen: Gen[A]): Gen[List[A]] = {
    def log2(x: Int): Int = (java.lang.Math.log(x)/java.lang.Math.log(2)).toInt
    for {
      sz <- Gen.size
      n  <- Gen.choose(1, log2(sz))
      l  <- Gen.listOfN(n, gen)
    } yield l
  }
}

object ExprArbitrary extends ExprArbitrary
