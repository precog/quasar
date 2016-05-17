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

import quasar.Predef.{List, Long, String, Vector, IS}
import quasar.effect.Failure
import quasar.fp._
import quasar.sql._

import matryoshka._
import scalaz._
import scalaz.Leibniz._
import scalaz.std.vector._
import scalaz.std.list._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.either._
import scalaz.syntax.writer._
import scalaz.syntax.nel._

package object quasar {
  type SemanticErrors = NonEmptyList[SemanticError]
  type SemanticErrsT[F[_], A] = EitherT[F, SemanticErrors, A]

  type PhaseResults = Vector[PhaseResult]
  type PhaseResultW[A] = Writer[PhaseResults, A]
  type PhaseResultT[F[_], A] = WriterT[F, PhaseResults, A]

  type CompileM[A] = SemanticErrsT[PhaseResultW, A]

  type EnvErr[A]         = Failure[EnvironmentError, A]
  type EnvErrF[A]        = Coyoneda[EnvErr, A]
  type EnvErrT[F[_], A] = EitherT[F, EnvironmentError, A]

  type SeqNameGeneratorT[F[_], A] = StateT[F, Long, A]
  type SaltedSeqNameGeneratorT[F[_], A] = ReaderT[SeqNameGeneratorT[F, ?], String, A]

  /** Returns the `LogicalPlan` for the given SQL^2 query. */
  def queryPlan(query: Fix[Sql], vars: Variables)(implicit RT: RenderTree[Fix[Sql]]):
      CompileM[Fix[LogicalPlan]] = {
    import SemanticAnalysis.AllPhases

    def phase[A: RenderTree](label: String, r: SemanticErrors \/ A): CompileM[A] =
      EitherT(r.point[PhaseResultW]) flatMap { a =>
        val pr = PhaseResult.Tree(label, RenderTree[A].render(a))
        (a.set(Vector(pr)): PhaseResultW[A]).liftM[SemanticErrsT]
      }

    for {
      ast         <- phase("SQL AST", query.right)
      substAst    <- phase("Variables Substituted",
                        Variables.substVars(ast, vars) leftMap (_.wrapNel))
      annTree     <- phase("Annotated Tree", AllPhases(substAst))
      logical     <- phase("Logical Plan", Compiler.compile(annTree) leftMap (_.wrapNel))
      optimized   <- phase("Optimized", \/-(Optimizer.optimize(logical)))
      typechecked <- phase("Typechecked", LogicalPlan.ensureCorrectTypes(optimized).disjunction)
    } yield typechecked
  }

  // TODO write generic impls (not using IndexedSeq) and put in fp package
  implicit class FuncUtils[A, N <: shapeless.Nat](val self: Func.Input[A, N]) extends scala.AnyVal {
    import shapeless._

    def reverse: Func.Input[A, N] =
      Sized.wrap[IS[A], N](self.unsized.reverse)

    def foldMap[B](f: A => B)(implicit F: Monoid[B]): B =
      self.unsized.toList.foldMap(f)

    def foldRight[B](z: => B)(f: (A, => B) => B): B =
      Foldable[List].foldRight(self.unsized.toList, z)(f)

    def traverse[G[_], B](f: A => G[B])(implicit G: Applicative[G]): G[Func.Input[B, N]] =
      G.map(self.unsized.toList.traverse(f))(bs => Sized.wrap[IS[B], N](bs.toIndexedSeq))

    def traverseU[GB](f: A => GB)(implicit G: Unapply[Applicative, GB]): G.M[Func.Input[G.A, N]] = {
      // use andThen instead of compose because anonymous types confuse scalac
      traverse[G.M, G.A](f andThen (G.leibniz(_)))(G.TC)
    }

    // Input[Option[B], N] -> Option[Input[B, N]]
    def sequence[G[_], B](implicit ev: A === G[B], G: Applicative[G]): G[Func.Input[B, N]] =
      G.map(self.unsized.toList.sequence(ev, G))(bs => Sized.wrap[IS[B], N](bs.toIndexedSeq))

    def sequenceU(implicit G: Unapply[Applicative, A]): G.M[Func.Input[G.A, N]] =
      sequence[G.M, G.A](G.leibniz, G.TC)

    def zip[B](input: Func.Input[B, N]): Func.Input[(A, B), N] =
      Sized.wrap[IS[(A, B)], N](self.unsized.zip(input))

    def unzip3[X, Y, Z](implicit ev: A === (X, (Y, Z))): (Func.Input[X, N], Func.Input[Y, N], Func.Input[Z, N]) =
      Unzip[List].unzip3[X, Y, Z](ev.subst(self.unsized.toList)) match {
        case (x, y, z) => (Sized.wrap[IS[X], N](x.toIndexedSeq), Sized.wrap[IS[Y], N](y.toIndexedSeq), Sized.wrap[IS[Z], N](z.toIndexedSeq))
      }
  }
}
