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

package quasar.std

import quasar.Predef._
import quasar.fp._
import quasar.{Func, LogicalPlan, Type, SemanticError}

import matryoshka._
import scalaz._, Validation.{success, failure}

trait Library {
  import Func._

  protected val noSimplification: Simplifier = new Simplifier {
    def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) =
      None
  }

  protected def constTyper(codomain: Codomain): Typer = { _ =>
    Validation.success(codomain)
  }

  private def partialTyperOV(f: Domain => Option[VCodomain]): Typer = { args =>
    f(args).getOrElse {
      val msg: String = "Unknown arguments: " + args
      Validation.failure(NonEmptyList(SemanticError.GenericError(msg)))
    }
  }

  protected def partialTyperV(f: PartialFunction[Domain, VCodomain]): Typer =
    partialTyperOV(f.lift)

  protected def partialTyper(f: PartialFunction[Domain, Codomain]): Typer =
    partialTyperOV(f.lift(_).map(success))

  protected def basicUntyper: Untyper =
    (func, _) => success(func.domain)

  protected def untyper(f: Codomain => VDomain): Untyper =
    (func, rez) => Type.typecheck(rez, func.codomain).fold(
      κ(f(rez)),
      κ(success(func.domain)))

  private def partialUntyperOV(f: Codomain => Option[VDomain]): Untyper =
    (func, rez) => Type.typecheck(rez, func.codomain).fold(
      e => f(rez).getOrElse(failure(e.map(ι[SemanticError]))),
      κ(success(func.domain)))

  protected def partialUntyperV(f: PartialFunction[Codomain, VDomain]): Untyper =
    partialUntyperOV(f.lift)

  protected def partialUntyper(f: PartialFunction[Codomain, Domain]): Untyper =
    partialUntyperOV(f.lift(_).map(success))

  protected def reflexiveTyper: Typer = {
    case Type.Const(data) :: Nil => success(data.dataType)
    case x :: Nil => success(x)
    case _ =>
      failure(NonEmptyList(SemanticError.GenericError("Wrong number of arguments for reflexive typer")))
  }

  protected val numericWidening = {
    def mapFirst[A, B](f: A => A, p: PartialFunction[A, B]) = new PartialFunction[A, B] {
      def isDefinedAt(a: A) = p.isDefinedAt(f(a))
      def apply(a: A) = p(f(a))
    }

    val half: PartialFunction[Domain, Codomain] = {
      case t1 :: t2 :: Nil       if t1 contains t2       => t1
      case Type.Dec :: t2 :: Nil if Type.Int contains t2 => Type.Dec
      case Type.Int :: t2 :: Nil if Type.Dec contains t2 => Type.Dec
    }
    partialTyper(half orElse mapFirst[Domain, Codomain](_.reverse, half))
  }

  protected implicit class TyperW(self: Typer) {
    def ||| (that: Typer): Typer = { args =>
      self(args) ||| that(args)
    }
  }

  def functions: List[Func]
}
