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

package quasar.sql

import slamdata.Predef._
import quasar._, RenderTree.ops._
import quasar.contrib.std._

import pathy.Path
import pathy.Path._
import matryoshka._
import matryoshka.implicits._
import matryoshka.data.Fix
import monocle.macros.Lenses
import monocle.Prism
import scalaz._, Scalaz._
import scalaz.Liskov._

sealed abstract class Statement[BODY] {
  def pprint(implicit ev: BODY <~< String): String
}

object Statement {
  implicit val traverse: Traverse[Statement] = new Traverse[Statement] {
    def traverseImpl[G[_]:Applicative,A,B](fa: Statement[A])(f: A => G[B]): G[Statement[B]] = fa match {
      case funcDef: FunctionDecl[_] => funcDef.transformBodyM(f).map(x => (x:Statement[B]))
      case Import(path)             => (Import(path):Statement[B]).point[G]
    }
  }
  implicit def renderTreeStatement[BODY:RenderTree]: RenderTree[Statement[BODY]] =
    new RenderTree[Statement[BODY]] {
      def render(statement: Statement[BODY]) = statement match {
        case func: FunctionDecl[_] => func.render
        case Import(path) => NonTerminal("Import" :: Nil, Some(posixCodec.unsafePrintPath(path)), Nil)
      }
    }
  implicit def equal[BODY:Equal]: Equal[Statement[BODY]] =
    Equal.equalBy(s => (functionDecl.getOption(s), import_.getOption(s)))

  def functionDecl[BODY] = Prism.partial[Statement[BODY], (CIName, List[CIName], BODY)] {
    case FunctionDecl(name, args, body) => (name, args, body)
  } ((FunctionDecl[BODY](_,_,_)).tupled)

  def import_[BODY] = Prism.partial[Statement[BODY], Path[Any, Dir, Unsandboxed]] {
    case Import(path) => path
  } (Import(_))
}

@Lenses final case class FunctionDecl[BODY](name: CIName, args: List[CIName], body: BODY) extends Statement[BODY] {
  def transformBody[B](f: BODY => B): FunctionDecl[B] =
    FunctionDecl(name, args, f(body))
  def transformBodyM[M[_]: Functor, B](f: BODY => M[B]) =
    f(body).map(FunctionDecl(name, args, _))
  override def pprint(implicit ev: BODY <~< String) =
    s"CREATE FUNCTION ${name.shows}(${args.map(":" + _.shows).mkString(", ")})\n  BEGIN\n    ${ev(body)}\n  END"
  def applyArgs[T[_[_]]: BirecursiveT](argsProvided: List[T[Sql]])(implicit ev: BODY <~< T[Sql]): SemanticError \/ T[Sql] = {
    val expected = args.size
    val actual   = argsProvided.size
    args.duplicates.headOption.cata(
      duplicates => SemanticError.InvalidFunctionDefinition(this.map(ev(_).convertTo[Fix[Sql]]), s"parameter :${duplicates.head.value} is defined multiple times").left, {
        if (expected ≠ actual) SemanticError.WrongArgumentCount(name, expected, actual).left
        else {
          val argMap = args.zip(argsProvided).toMap
          ev(body).cataM[SemanticError \/ ?, T[Sql]] {
            case v: Vari[T[Sql]] =>
              argMap.getOrElse(CIName(v.symbol), v.embed).right // Leave the variable there in case it will be substituted by an external variable
            case s: Select[T[Sql]] =>
              s.substituteRelationVariable[Id, T[Sql]](v => argMap.getOrElse(CIName(v.symbol), v.embed)).map(_.embed)
            case other => other.embed.right
          }
        }
      })
  }
}

object FunctionDecl {
  implicit def renderTreeFunctionDecl[BODY:RenderTree]: RenderTree[FunctionDecl[BODY]] =
    new RenderTree[FunctionDecl[BODY]] {
      def render(funcDec: FunctionDecl[BODY]) =
        NonTerminal("Function Declaration" :: Nil, Some(funcDec.name.value), List(funcDec.body.render))
    }

  implicit val traverse: Traverse[FunctionDecl] = new Traverse[FunctionDecl] {
    def traverseImpl[G[_]:Applicative,A,B](funcDec: FunctionDecl[A])(f: A => G[B]): G[FunctionDecl[B]] =
      funcDec.transformBodyM(f)
  }
}

@Lenses final case class Import[BODY](path: Path[Any, Dir, Unsandboxed]) extends Statement[BODY] {
  override def pprint(implicit ev: BODY <~< String) =
    // We need to escape any backticks in the resulting String as pathy is
    // indiferent but since this is a SQL string they yield invalid SQL
    // if not escaped
    s"import `${posixCodec.unsafePrintPath(path).replace("`", "\\`")}`"
}
