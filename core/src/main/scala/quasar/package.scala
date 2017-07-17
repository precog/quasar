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

import slamdata.Predef._
import quasar.common.{PhaseResult, PhaseResultW}
import quasar.connector.CompileM
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.effect.Failure
import quasar.fp._
import quasar.fp.numeric._
import quasar.frontend.{SemanticErrors, SemanticErrsT}
import quasar.frontend.logicalplan.{LogicalPlan => LP, Free => _, _}
import quasar.fs.{FileSystemError, FileSystemErrT}
import quasar.fs.FileSystemError._
import quasar.fs.PathError._
import quasar.fs.mount.Mounting
import quasar.sql._
import quasar.std.StdLib.set._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._

package object quasar {
  private def phase[A: RenderTree](label: String, r: SemanticErrors \/ A):
      CompileM[A] =
    EitherT(r.point[PhaseResultW]) flatMap { a =>
      (a.set(Vector(PhaseResult.tree(label, a)))).liftM[SemanticErrsT]
    }

  /** Compiles a query into raw LogicalPlan, which has not yet been optimized or
    * typechecked.
    */
  // TODO: Move this into the SQL package, provide a type class for it in core.
  def precompile[T: Equal: RenderTree]
    (query: Fix[Sql], vars: Variables, basePath: ADir)
    (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP])
      : CompileM[T] = {
    import SemanticAnalysis._
    for {
      ast      <- phase("SQL AST", query.right)
      substAst <- phase("Variables Substituted", Variables.substVars(ast, vars))
      absAst   <- phase("Absolutized", substAst.mkPathsAbsolute(basePath).right)
      normed   <- phase("Normalized Projections", normalizeProjections[Fix[Sql]](absAst).right)
      sortProj <- phase("Sort Keys Projected", projectSortKeys[Fix[Sql]](normed).right)
      annAst   <- phase("Annotated Tree", annotate[Fix[Sql]](sortProj))
      logical  <- phase("Logical Plan", Compiler.compile[T](annAst) leftMap (_.wrapNel))
    } yield logical
  }

  private val optimizer = new Optimizer[Fix[LP]]
  private val lpr = optimizer.lpr

  /** Optimizes and typechecks a `LogicalPlan` returning the improved plan.
    */
  def preparePlan(lp: Fix[LP]): CompileM[Fix[LP]] =
    for {
      optimized   <- phase("Optimized", optimizer.optimize(lp).right)
      typechecked <- phase("Typechecked", lpr.ensureCorrectTypes(optimized).disjunction)
      rewritten   <- phase("Rewritten Joins", optimizer.rewriteJoins(typechecked).right)
    } yield rewritten

  /** Identify plans which reduce to a (set of) constant value(s). */
  def refineConstantPlan(lp: Fix[LP]): List[Data] \/ Fix[LP] =
    lp.project match {
      case Constant(Data.Set(records)) => records.left
      case Constant(value)             => List(value).left
      case _                           => lp.right
    }

  def resolveImports[S[_]](scopedExpr: ScopedExpr[Fix[Sql]], baseDir: ADir)(implicit
    mount: Mounting.Ops[S],
    fsFail: Failure.Ops[FileSystemError, S]
  ): EitherT[Free[S, ?], SemanticError, Fix[Sql]] =
    EitherT(fsFail.unattemptT(resolveImports_(scopedExpr, baseDir).run))

  def resolveImports_[S[_]](scopedExpr: ScopedExpr[Fix[Sql]], baseDir: ADir)(implicit
    mount: Mounting.Ops[S]
  ): EitherT[FileSystemErrT[Free[S, ?], ?], SemanticError, Fix[Sql]] =
    resolveImportsImpl[EitherT[FileSystemErrT[Free[S, ?], ?], SemanticError, ?], Fix](
      scopedExpr,
      baseDir,
      d => EitherT(EitherT(mount
             .lookupModuleConfig(d)
             .bimap(e => SemanticError.genericError(e.shows), _.statements)
             .run.run ∘ (_ \/> (pathErr(pathNotFound(d))))))
    ).run >>= (i => EitherT(EitherT.right(i.η[Free[S, ?]])))

  /** Returns the `LogicalPlan` for the given SQL^2 query, or a list of
    * results, if the query was foldable to a constant.
    */
  def queryPlan(
    expr: Fix[Sql], vars: Variables, basePath: ADir, off: Natural, lim: Option[Positive]):
      CompileM[List[Data] \/ Fix[LP]] =
    precompile[Fix[LP]](expr, vars, basePath)
      .flatMap(lp => preparePlan(addOffsetLimit(lp, off, lim)))
      .map(refineConstantPlan)

  def addOffsetLimit[T]
    (lp: T, off: Natural, lim: Option[Positive])
    (implicit T: Corecursive.Aux[T, LP])
      : T = {
    val skipped = Drop(lp, constant[T](Data.Int(off.value)).embed).embed
    lim.fold(
      skipped)(
      l => Take(skipped, constant[T](Data.Int(l.value)).embed).embed)
  }
}
