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
import quasar.contrib.pathy.ADir
import quasar.fp._
import quasar.fs.PathError
import quasar.frontend.logicalplan.LogicalPlan

import matryoshka._
import matryoshka.data.Fix
import monocle.Prism
import pathy.Path.posixCodec
import scalaz._, Scalaz._
import shapeless.Nat

object Planner {
  sealed trait PlannerError {
    def message: String
  }

  final case class NonRepresentableData(data: Data) extends PlannerError {
    def message = "The back-end has no representation for the constant: " + data.shows
  }
  final case class NonRepresentableEJson(data: String)
      extends PlannerError {
    def message = "The back-end has no representation for the constant: " + data
  }
  final case class UnsupportedFunction[N <: Nat](func: GenericFunc[N], hint: Option[String]) extends PlannerError {
    def message = "The function '" + func.shows + "' is recognized but not supported by this back-end." + hint.map(" (" + _ + ")").getOrElse("")
  }
  final case class PlanPathError(error: PathError) extends PlannerError {
    def message = error.shows
  }
  final case class UnsupportedJoinCondition(cond: Fix[LogicalPlan]) extends PlannerError {
    def message = s"Joining with ${cond.shows} is not currently supported"
  }
  final case class UnsupportedPlan(plan: LogicalPlan[_], hint: Option[String]) extends PlannerError {
    def message = "The back-end has no or no efficient means of implementing the plan" + hint.map(" (" + _ + ")").getOrElse("")+ ": " + plan
  }
  final case class FuncApply[N <: Nat](func: GenericFunc[N], expected: String, actual: String) extends PlannerError {
    def message = "A parameter passed to function " + func.shows + " is invalid: Expected " + expected + " but found: " + actual
  }
  final case class ObjectIdFormatError(str: String) extends PlannerError {
    def message = "Invalid ObjectId string: " + str
  }

  final case class NonRepresentableInJS(value: String) extends PlannerError {
    def message = "Operation/value could not be compiled to JavaScript: " + value
  }
  final case class UnsupportedJS(value: String) extends PlannerError {
    def message = "Conversion of operation/value to JavaScript not implemented: " + value
  }

  final case class UnboundVariable(name: Symbol) extends PlannerError {
    def message = s"The variable “$name” is unbound at a use site."
  }

  final case class NoFilesFound(dirs: List[ADir]) extends PlannerError {
    def message = dirs.map(posixCodec.printPath) match {
      case Nil => "No paths provided to read from."
      case ds  =>
        "None of these directories contain any files to read from: " ++
          ds.mkString(", ")
      }
  }

  final case class InternalError(msg: String, cause: Option[Exception]) extends PlannerError {
    def message = msg + ~cause.map(ex => s" (caused by: $ex)")
  }

  object InternalError {
    def fromMsg(msg: String): PlannerError = apply(msg, None)
  }

  implicit val PlannerErrorRenderTree: RenderTree[PlannerError] = new RenderTree[PlannerError] {
    def render(v: PlannerError) = Terminal(List("Error"), Some(v.message))
  }

  implicit val plannerErrorShow: Show[PlannerError] =
    Show.show(_.message)

  sealed trait CompilationError {
    def message: String
  }
  object CompilationError {
    final case class CompilePathError(error: PathError)
        extends CompilationError {
      def message = error.shows
    }
    final case class CSemanticError(error: SemanticError)
        extends CompilationError {
      def message = error.message
    }
    final case class CPlannerError(error: PlannerError)
        extends CompilationError {
      def message = error.message
    }
    final case class ManyErrors(errors: NonEmptyList[SemanticError])
        extends CompilationError {
      def message = errors.map(_.message).list.toList.mkString("[", "\n", "]")
    }
  }

  val CompilePathError = Prism.partial[CompilationError, PathError] {
    case CompilationError.CompilePathError(error) => error
  } (CompilationError.CompilePathError(_))

  val CSemanticError = Prism.partial[CompilationError, SemanticError] {
    case CompilationError.CSemanticError(error) => error
  } (CompilationError.CSemanticError(_))

  val CPlannerError = Prism.partial[CompilationError, PlannerError] {
    case CompilationError.CPlannerError(error) => error
  } (CompilationError.CPlannerError(_))

  val ManyErrors = Prism.partial[CompilationError, NonEmptyList[SemanticError]] {
    case CompilationError.ManyErrors(error) => error
  } (CompilationError.ManyErrors(_))
}
