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

import quasar.Predef._
import quasar._, RenderTree.ops._

import monocle.macros.Lenses
import scalaz._, Scalaz._

sealed trait Statement[BODY] {
  def pprint(implicit show: Show[BODY]): String

  implicit val functor: Functor[Statement] = new Functor[Statement] {
    def map[A, B](s: Statement[A])(f: A => B): Statement[B] = s match {
      case funcDef: FunctionDecl[_] => funcDef.transformBody(f)
      case Import(path)              => Import(path)
    }
  }
  implicit val traverse: Traverse[Statement] = new Traverse[Statement] {
    def traverseImpl[G[_]:Applicative,A,B](fa: Statement[A])(f: A => G[B]): G[Statement[B]] = fa match {
      case funcDef: FunctionDecl[_] => funcDef.transformBodyM(f).map(x => (x:Statement[B]))
      case Import(path)             => (Import(path):Statement[B]).point[G]
    }
  }
}

@Lenses final case class FunctionDecl[BODY](name: CIName, args: List[CIName], body: BODY) extends Statement[BODY] {
  def transformBody[B](f: BODY => B): FunctionDecl[B] =
    FunctionDecl(name, args, f(body))
  def transformBodyM[M[_]: Functor, B](f: BODY => M[B]) =
    f(body).map(FunctionDecl(name, args, _))
  override def pprint(implicit show: Show[BODY]) =
    s"CREATE FUNCTION ${name.shows}(${args.map(":" + _.shows).mkString(",")})\n  BEGIN\n    ${body.shows}\n  END"
}

object FunctionDecl {
  implicit def renderTreeFunctionDecl[BODY:RenderTree]: RenderTree[FunctionDecl[BODY]] =
    new RenderTree[FunctionDecl[BODY]] {
      def render(funcDec: FunctionDecl[BODY]) =
        NonTerminal("Function Declaration" :: Nil, Some(funcDec.name.value), List(funcDec.body.render))
    }
}

@Lenses final case class Import[BODY](path: String) extends Statement[BODY] {
  override def pprint(implicit show: Show[BODY]) =
    s"import `$path`"
}
