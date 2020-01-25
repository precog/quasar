/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.compile

import slamdata.Predef._
import quasar.{ArgumentError, VarName, VarValue}
import quasar.contrib.pathy.ADir
import quasar.common.CIName
import quasar.sql._

import matryoshka._
import matryoshka.data._
import monocle._
import pathy.Path, Path._
import scalaz._, Scalaz._
import shapeless.{Prism => _, _}

sealed abstract class SemanticError {
  def message: String
}

object SemanticError {
  implicit val SemanticErrorShow: Show[SemanticError] = Show.shows(_.message)

  final case class GenericError(message: String) extends SemanticError

  final case class ArgError(argumentError: ArgumentError) extends SemanticError {
    def message = argumentError.message
  }

  // Modules
  final case class AmbiguousFunctionInvoke(name: CIName, from: List[(CIName,ADir)]) extends SemanticError {
    def fullyQualifiedFuncs = from.map { case (name, dir) => posixCodec.printPath(dir) + name.value}
    def message = {
      val functions = fullyQualifiedFuncs.mkString(", ")
      s"Function call `${name.shows}` is ambiguous because the following functions: $functions could be applied here"
    }
  }

  final case class InvalidFunctionDefinition(funcDef: FunctionDecl[Fix[Sql]], reason: String) extends SemanticError {
    def message = s"The function '${funcDef.name.shows}' is invalid because: $reason"
  }

  // Compiler
  final case class FunctionNotFound(name: CIName) extends SemanticError {
    def message = s"The function '${name.shows}' could not be found in the standard library"
  }

  final case class VariableParseError(vari: VarName, value: VarValue, cause: quasar.sql.ParsingError) extends SemanticError {
    def message = s"The variable ${vari.toString} should contain a SQL expression but was `${value.value}` (${cause.message})"
  }

  final case class UnboundVariable(vari: VarName) extends SemanticError {
    def message = s"There is no binding for the variable $vari"
  }

  final case class DuplicateRelationName(defined: String) extends SemanticError {
    def message = s"Found relation with duplicate name '$defined'"
  }

  final case class NoTableDefined(node: Fix[Sql]) extends SemanticError {
    def message = s"No table was defined in the scope of '${pprint(node)}'"
  }

  final case class DuplicateAlias(name: String) extends SemanticError {
    def message = s"Alias `$name` appears twice in projections"
  }

  final case class WrongArgumentCount(funcName: CIName, expected: Int, actual: Int) extends SemanticError {
    def message = s"Wrong number of arguments for function '${funcName.shows}': expected $expected but found $actual"
  }

  final case class AmbiguousReference(node: Fix[Sql], relations: List[SqlRelation[Unit]])
      extends SemanticError {
    def message = "The expression '" + pprint(node) + "' is ambiguous and might refer to any of the tables " + relations.mkString(", ")
  }

  final case object CompiledTableMissing extends SemanticError {
    def message = "Expected the root table to be compiled but found nothing"
  }

  final case class CompiledSubtableMissing(name: String) extends SemanticError {
    def message = s"""Expected to find a compiled subtable with name "$name""""
  }

  final case class InvalidPathError(path: Path[_, File, _], hint: Option[String]) extends SemanticError {
    def message = "Invalid path: " + posixCodec.unsafePrintPath(path) + hint.map(" (" + _ + ")").getOrElse("")
  }

  final case class UnexpectedDatePart(part: String) extends SemanticError {
    def message = s"""Invalid selector for DATE_PART: $part (expected "century", "day", etc.)"""
  }

  // TODO: Add other prisms when necessary (unless we enable the "No Any" wart first)
  val genericError: Prism[SemanticError, String] = Prism.partial[SemanticError, String] {
    case GenericError(msg) => msg
  } (GenericError(_))

  val unboundVariable: Prism[SemanticError, VarName] = Prism.partial[SemanticError, VarName] {
    case UnboundVariable(varname) => varname
  }(UnboundVariable(_))

  val ambiguousFunctionInvoke: Prism[SemanticError, (CIName, List[(CIName, ADir)])] =
    Prism.partial[SemanticError, (CIName, List[(CIName, ADir)])] {
      case AmbiguousFunctionInvoke(name, from) => (name, from)
    }(AmbiguousFunctionInvoke.tupled)

  val invalidFunctionDefinition: Prism[SemanticError, (FunctionDecl[Fix[Sql]], String)] =
    Prism.partial[SemanticError, (FunctionDecl[Fix[Sql]], String)] {
      case InvalidFunctionDefinition(funcDef, reason) => (funcDef, reason)
    }(InvalidFunctionDefinition.tupled)

  val ambiguousReference: Prism[SemanticError, (Fix[Sql], List[SqlRelation[Unit]])] =
    Prism.partial[SemanticError, (Fix[Sql], List[SqlRelation[Unit]])] {
      case AmbiguousReference(node, relations) => (node, relations)
    }(AmbiguousReference.tupled)

  val duplicateAlias: Prism[SemanticError, String] = Prism.partial[SemanticError, String] {
    case DuplicateAlias(name) => name
  }(DuplicateAlias(_))

  val duplicateRelationName: Prism[SemanticError, String] =
    Prism.partial[SemanticError, String] {
      case DuplicateRelationName(name) => name
    } (DuplicateRelationName(_))

  val wrongArgumentCount: Prism[SemanticError, (CIName, Int, Int)] = Prism.partial[SemanticError, (CIName, Int, Int)] {
    case WrongArgumentCount(name, expected, found) => (name, expected, found)
  }(WrongArgumentCount.tupled)

  val compiledSubtableMissing: Prism[SemanticError, String] = Prism.partial[SemanticError, String] {
    case CompiledSubtableMissing(name) => name
  }(CompiledSubtableMissing(_))

  val noTableDefined: Prism[SemanticError, Fix[Sql]] =
    Prism.partial[SemanticError, Fix[Sql]] {
      case NoTableDefined(expr) => expr
    } (NoTableDefined(_))

  val argError: Prism[SemanticError, ArgumentError] =
    Prism.partial[SemanticError, ArgumentError] {
      case ArgError(e) => e
    } (ArgError(_))
}
