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
import quasar.fp.ski._
import quasar.SemanticError._
import quasar.sql.{Sql, Ident, Query, Select, Vari, TableRelationAST, VariRelationAST, pprint}

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path.posixCodec
import scalaz._, Scalaz._

final case class Variables(value: Map[VarName, VarValue]) {
  def lookup(name: VarName): SemanticError \/ Fix[Sql] =
    value.get(name).fold[SemanticError \/ Fix[Sql]](
      UnboundVariable(name).left)(
      varValue => sql.fixParser.parse(Query(varValue.value))
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
    case sel @ Select(dist, proj, Some(rel), filter, group, order) =>
      rel.transformM[SemanticError \/ ?, Fix[Sql]]({
        case VariRelationAST(vari, alias) =>
          val varName = VarName(vari.symbol)
          vars.lookup(varName) flatMap {
            case Fix(Ident(name)) =>
              posixCodec.parsePath(Some(_), Some(_), κ(None), κ(None))(name).cata(
                TableRelationAST(_, alias).right,
                GenericError(s"bad path: $name (note: absolute file path required)").left)  // FIXME
            case x =>
              GenericError(s"not a valid table name: ${pprint(x)}").left  // FIXME
          }
        case r => r.right
      }, _.right[SemanticError]).map(rel =>
        sel.copy(relations = rel.some).embed)
    case x => x.embed.right
  }

  // FIXME: Get rid of this
  def substVars(expr: Fix[Sql], variables: Variables)
      : SemanticError \/ Fix[Sql] =
    expr.cataM[SemanticError \/ ?, Fix[Sql]](substVarsƒ(variables))
}
