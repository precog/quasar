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
import quasar.{Data, TermLogicalPlanMatchers}
import quasar.contrib.pathy.sandboxCurrent
import quasar.fp.ski._
import quasar.logicalPlan.{LogicalPlan, Optimizer}
import quasar.sql.SemanticAnalysis._
import quasar.std._, StdLib._, structural._

import matryoshka._
import org.specs2.matcher.MustThrownMatchers._
import pathy.Path._
import scalaz._, Scalaz._

trait CompilerHelpers extends TermLogicalPlanMatchers {
  import quasar.frontend.fixpoint.lpf

  val compile: String => String \/ Fix[LogicalPlan] = query => {
    for {
      select <- fixParser.parse(Query(query)).leftMap(_.toString)
      attr   <- AllPhases(select).leftMap(_.toString)
      cld    <- Compiler.compile(attr).leftMap(_.toString)
    } yield cld
  }

  val optimizer = new Optimizer[Fix]
  val lpr = optimizer.lpr

  // Compile -> Optimize -> Typecheck
  val fullCompile: String => String \/ Fix[LogicalPlan] =
    q => compile(q).map(optimizer.optimize).flatMap(lp =>
      lpr.ensureCorrectTypes(lp)
        .disjunction.leftMap(_.list.toList.mkString(";")))

  // NB: this plan is simplified and normalized, but not optimized. That allows
  // the expected result to be controlled more precisely. Assuming you know
  // what plan the compiler produces for a reference query, you can demand that
  // `optimize` produces the same plan given a query in some more deviant form.
  def compileExp(query: String): Fix[LogicalPlan] =
    compile(query).fold(
      e => throw new RuntimeException("could not compile query for expected value: " + query + "; " + e),
      lp => (lpr.normalizeLets _ >>> lpr.normalizeTempNames _)(optimizer.simplify(lp)))

  // Compile the given query, including optimization and typechecking
  def fullCompileExp(query: String): Fix[LogicalPlan] =
    fullCompile(query).valueOr(e =>
      throw new RuntimeException(s"could not full-compile query for expected value '$query': $e"))

  def testLogicalPlanCompile(query: String, expected: Fix[LogicalPlan]) = {
    compile(query).map(optimizer.optimize).toEither must beRight(equalToPlan(expected))
  }

  def testTypedLogicalPlanCompile(query: String, expected: Fix[LogicalPlan]) =
    fullCompile(query).toEither must beRight(equalToPlan(expected))

  def read(file: String): Fix[LogicalPlan] =
    lpf.read(sandboxCurrent(posixCodec.parsePath(Some(_), Some(_), κ(None), κ(None))(file).get).get)

  // TODO: This type and implicit are a failed experiment and should be removed,
  //       but they infect the compiler tests.
  type FLP = Fix[LogicalPlan]
  implicit def toFix[F[_]](unFixed: F[Fix[F]]): Fix[F] = Fix(unFixed)

  def makeObj(ts: (String, Fix[LogicalPlan])*): Fix[LogicalPlan] =
    Fix(MakeObjectN(ts.map(t => lpf.constant(Data.Str(t._1)) -> t._2): _*))
}
