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

package quasar

import slamdata.Predef._
import quasar.SemanticError._
import quasar.frontend.SemanticErrors
import quasar.sql.{Sql, Query, Select, Vari, VariRelationAST}

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._

final case class Variables(value: Map[VarName, VarValue]) {
  def lookup(name: VarName): SemanticError \/ Fix[Sql] =
    value.get(name).fold[SemanticError \/ Fix[Sql]](
      UnboundVariable(name).left)(
      varValue => sql.fixParser.parseExpr(Query(varValue.value))
        .leftMap(VariableParseError(name, varValue, _)))
}
final case class VarName(value: String) {
  override def toString = ":" + value
}
final case class VarValue(value: String)

object Variables {
  val empty: Variables = Variables(Map())

  def fromMap(value: Map[String, String]): Variables =
    Variables(value.map(t => VarName(t._1) -> VarValue(t._2)))

  def substVarsƒ(vars: Variables):
      AlgebraM[SemanticError \/ ?, Sql, Fix[Sql]] = {
    case Vari(name) =>
      vars.lookup(VarName(name))
    case sel: Select[Fix[Sql]] =>
      sel.substituteRelationVariable[SemanticError \/ ?, Fix[Sql]](v => vars.lookup(VarName(v.symbol))).join.map(_.embed)
    case x => x.embed.right
  }

  def allVariables: Algebra[Sql, List[VarName]] = {
    case Vari(name)                                      => List(VarName(name))
    case sel @ Select(_, _, rel, _, _, _) =>
      rel.toList.collect { case VariRelationAST(vari, _) => VarName(vari.symbol) } ++
      (sel: Sql[List[VarName]]).fold
    case other                                           => other.fold
  }

  // FIXME: Get rid of this
  def substVars(expr: Fix[Sql], variables: Variables)
      : SemanticErrors \/ Fix[Sql] = {
    val allVars = expr.cata(allVariables)
    val errors = allVars.map(variables.lookup(_)).collect { case -\/(semErr) => semErr }.toNel
    errors.fold(
      expr.cataM[SemanticError \/ ?, Fix[Sql]](substVarsƒ(variables)).leftMap(_.wrapNel))(
      errors => errors.left)
  }

  implicit val equal: Equal[Variables] = Equal.equalA
}
