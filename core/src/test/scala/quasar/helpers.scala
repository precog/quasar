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
import quasar.fs.sandboxCurrent
import quasar.fp._
import quasar.sql.Query
import quasar.std._

import matryoshka._
import scalaz._, Scalaz._
import pathy.Path._

trait CompilerHelpers extends quasar.QuasarSpecification with TermLogicalPlanMatchers {
  import StdLib._
  import structural._
  import LogicalPlan._
  import SemanticAnalysis._

  val compile: String => String \/ Fix[LogicalPlan] = query => {
    for {
      select <- sql.fixParser.parse(Query(query)).leftMap(_.toString)
      attr   <- AllPhases(select).leftMap(_.toString)
      cld    <- Compiler.compile(attr).leftMap(_.toString)
    } yield cld
  }

  // Compile -> Optimize -> Typecheck
  val fullCompile: String => String \/ Fix[LogicalPlan] =
    q => compile(q).map(Optimizer.optimize).flatMap(lp =>
      LogicalPlan.ensureCorrectTypes(lp)
        .disjunction.leftMap(_.list.toList.mkString(";")))

  // NB: this plan is simplified and normalized, but not optimized. That allows
  // the expected result to be controlled more precisely. Assuming you know
  // what plan the compiler produces for a reference query, you can demand that
  // `optimize` produces the same plan given a query in some more deviant form.
  def compileExp(query: String): Fix[LogicalPlan] =
    compile(query).fold(
      e => throw new RuntimeException("could not compile query for expected value: " + query + "; " + e),
      lp => (LogicalPlan.normalizeLets _ >>> LogicalPlan.normalizeTempNames _)(Optimizer.simplify(lp)))

  // Compile the given query, including optimization and typechecking
  def fullCompileExp(query: String): Fix[LogicalPlan] =
    fullCompile(query).valueOr(e =>
      throw new RuntimeException(s"could not full-compile query for expected value '$query': $e"))

  def testLogicalPlanCompile(query: String, expected: Fix[LogicalPlan]) = {
    compile(query).map(Optimizer.optimize).toEither must beRight(equalToPlan(expected))
  }

  def testTypedLogicalPlanCompile(query: String, expected: Fix[LogicalPlan]) =
    fullCompile(query).toEither must beRight(equalToPlan(expected))

  def read(file: String): Fix[LogicalPlan] = LogicalPlan.Read(sandboxCurrent(posixCodec.parsePath(Some(_), Some(_), κ(None), κ(None))(file).get).get)

  type FLP = Fix[LogicalPlan]
  implicit def toFix[F[_]](unFixed: F[Fix[F]]): Fix[F] = Fix(unFixed)

  def makeObj(ts: (String, Fix[LogicalPlan])*): Fix[LogicalPlan] =
    Fix(MakeObjectN(ts.map(t => Constant(Data.Str(t._1)) -> t._2): _*))

}
