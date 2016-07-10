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

import quasar.Predef.{List, Long, String, Vector}
import quasar.effect.Failure
import quasar.fp._
import quasar.fp.numeric._
import quasar.fs.ADir
import quasar.sql._
import quasar.std.StdLib.set._

import scala.Option

import matryoshka._, Recursive.ops._
import scalaz._
import scalaz.Leibniz._
import scalaz.std.vector._
import scalaz.std.list._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.either._
import scalaz.syntax.writer._
import scalaz.syntax.nel._
import shapeless._

package object quasar {
  type SemanticErrors = NonEmptyList[SemanticError]
  type SemanticErrsT[F[_], A] = EitherT[F, SemanticErrors, A]

  type PhaseResults = Vector[PhaseResult]
  type PhaseResultW[A] = Writer[PhaseResults, A]
  type PhaseResultT[F[_], A] = WriterT[F, PhaseResults, A]

  type CompileM[A] = SemanticErrsT[PhaseResultW, A]

  type EnvErr[A] = Failure[EnvironmentError, A]
  type EnvErrT[F[_], A] = EitherT[F, EnvironmentError, A]

  type SeqNameGeneratorT[F[_], A] = StateT[F, Long, A]
  type SaltedSeqNameGeneratorT[F[_], A] = ReaderT[SeqNameGeneratorT[F, ?], String, A]

  private def phase[A: RenderTree](label: String, r: SemanticErrors \/ A):
      CompileM[A] =
      EitherT(r.point[PhaseResultW]) flatMap { a =>
        val pr = PhaseResult.Tree(label, RenderTree[A].render(a))
        (a.set(Vector(pr)): PhaseResultW[A]).liftM[SemanticErrsT]
      }

  // TODO: Move this into the SQL package, provide a type class for it in core.
  def precompile(query: Fix[Sql], vars: Variables, basePath: ADir)(
    implicit RT: RenderTree[Fix[Sql]]):
      CompileM[Fix[LogicalPlan]] = {
    import SemanticAnalysis.AllPhases

    for {
      ast      <- phase("SQL AST", query.right)
      substAst <- phase("Variables Substituted",
                    Variables.substVars(ast, vars) leftMap (_.wrapNel))
      absAst   <- phase("Absolutized", substAst.mkPathsAbsolute(basePath).right)
      annTree  <- phase("Annotated Tree", AllPhases(absAst))
      logical  <- phase("Logical Plan", Compiler.compile(annTree) leftMap (_.wrapNel))
    } yield logical
  }

  /** Optimizes and typechecks a `LogicalPlan` returning the improved plan or
    * just a list of results, if the query was foldable to a constant.
    */
  def preparePlan(lp: Fix[LogicalPlan], off: Natural, lim: Option[Positive])
      : CompileM[List[Data] \/ Fix[LogicalPlan]] =
    for {
      optimized   <- phase("Optimized", Optimizer.optimize(addOffsetLimit(lp, off, lim)).right)
      typechecked <- phase("Typechecked", LogicalPlan.ensureCorrectTypes(optimized).disjunction)
    } yield typechecked.project match {
      case LogicalPlan.ConstantF(Data.Set(records)) => records.left
      case LogicalPlan.ConstantF(value)             => List(value).left
      case _                                        => typechecked.right
    }

  /** Returns the `LogicalPlan` for the given SQL^2 query, or a list of
    * results, if the query was foldable to a constant.
    */
  def queryPlan(
    query: Fix[Sql], vars: Variables, basePath: ADir, off: Natural, lim: Option[Positive])(
    implicit RT: RenderTree[Fix[Sql]]):
      CompileM[List[Data] \/ Fix[LogicalPlan]] =
    precompile(query, vars, basePath).flatMap(preparePlan(_, off, lim))

  def addOffsetLimit[T[_[_]]: Corecursive](
    lp: T[LogicalPlan], off: Natural, lim: Option[Positive]):
      T[LogicalPlan] = {
    val skipped =
      Drop(lp, LogicalPlan.ConstantF[T[LogicalPlan]](Data.Int(off.get)).embed).embed
    lim.fold(
      skipped)(
      l => Take(skipped, LogicalPlan.ConstantF[T[LogicalPlan]](Data.Int(l.get)).embed).embed)
  }

  // TODO generalize this and contribute to shapeless-contrib
  implicit class FuncUtils[A, N <: shapeless.Nat](val self: Func.Input[A, N]) extends scala.AnyVal {
    def reverse: Func.Input[A, N] =
      Sized.wrap[List[A], N](self.unsized.reverse)

    def foldMap[B](f: A => B)(implicit F: Monoid[B]): B =
      self.unsized.foldMap(f)

    def foldRight[B](z: => B)(f: (A, => B) => B): B =
      Foldable[List].foldRight(self.unsized, z)(f)

    def traverse[G[_], B](f: A => G[B])(implicit G: Applicative[G]): G[Func.Input[B, N]] =
      G.map(self.unsized.traverse(f))(bs => Sized.wrap[List[B], N](bs))

    // Input[Option[B], N] -> Option[Input[B, N]]
    def sequence[G[_], B](implicit ev: A === G[B], G: Applicative[G]): G[Func.Input[B, N]] =
      G.map(self.unsized.sequence(ev, G))(bs => Sized.wrap[List[B], N](bs))

    def zip[B](input: Func.Input[B, N]): Func.Input[(A, B), N] =
      Sized.wrap[List[(A, B)], N](self.unsized.zip(input))

    def unzip3[X, Y, Z](implicit ev: A === (X, (Y, Z))): (Func.Input[X, N], Func.Input[Y, N], Func.Input[Z, N]) =
      Unzip[List].unzip3[X, Y, Z](ev.subst(self.unsized)) match {
        case (x, y, z) => (Sized.wrap[List[X], N](x), Sized.wrap[List[Y], N](y), Sized.wrap[List[Z], N](z))
      }
  }
}
