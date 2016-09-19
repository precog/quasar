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

package quasar.physical.marklogic.xquery

import quasar.Predef._

import scalaz._, Id.Id
import scalaz.std.tuple._
import scalaz.syntax.foldable._
import scalaz.syntax.functor._

sealed abstract class FunctionDecl {
  def name: xml.QName
  def parameters: NonEmptyList[FunctionParam]
  def returnType: SequenceType
  def body: XQuery

  // TODO: Naming?
  def render: String = {
    val paramsList = parameters.map(_.render).toList
    s"declare function ${name}${paramsList.mkString("(", ", ", ")")} as $returnType {\n  $body\n}"
  }
}

object FunctionDecl {
  // TODO: Maybe just name? Until we have namespaces?
  implicit val order: Order[FunctionDecl] =
    Order.orderBy(fd => (fd.name, fd.parameters, fd.returnType))

  implicit val show: Show[FunctionDecl] =
    Show.shows(fd => s"FunctionDecl(${fd.name})")

  final case class FunctionDecl1(
    name: xml.QName,
    param1: FunctionParam,
    returnType: SequenceType,
    body: XQuery
  ) extends FunctionDecl {
    def parameters = NonEmptyList(param1)
  }

  final case class FunctionDecl2(
    name: xml.QName,
    param1: FunctionParam,
    param2: FunctionParam,
    returnType: SequenceType,
    body: XQuery
  ) extends FunctionDecl {
    def parameters = NonEmptyList(param1, param2)
  }

  final case class FunctionDecl3(
    name: xml.QName,
    param1: FunctionParam,
    param2: FunctionParam,
    param3: FunctionParam,
    returnType: SequenceType,
    body: XQuery
  ) extends FunctionDecl {
    def parameters = NonEmptyList(param1, param2, param3)
  }

  final case class FunctionDeclDsl(fname: xml.QName) {
    def apply(p1: FunctionParam): FunctionDecl1Dsl =
      FunctionDecl1Dsl(fname, p1, SequenceType.Top)

    def apply(p1: FunctionParam, p2: FunctionParam): FunctionDecl2Dsl =
      FunctionDecl2Dsl(fname, p1, p2, SequenceType.Top)

    def apply(p1: FunctionParam, p2: FunctionParam, p3: FunctionParam): FunctionDecl3Dsl =
      FunctionDecl3Dsl(fname, p1, p2, p3, SequenceType.Top)
  }

  final case class FunctionDecl1Dsl(fn: xml.QName, p1: FunctionParam, rt: SequenceType) {
    def as(rType: SequenceType): FunctionDecl1Dsl = copy(rt = rType)

    def apply[F[_]: Functor](body: XQuery => F[XQuery]): F[FunctionDecl1] =
      body(p1.name.xqy) map (FunctionDecl1(fn, p1, rt, _))

    def apply(body: XQuery => XQuery): FunctionDecl1 =
      apply[Id](body)
  }

  final case class FunctionDecl2Dsl(fn: xml.QName, p1: FunctionParam, p2: FunctionParam, rt: SequenceType) {
    def as(rType: SequenceType): FunctionDecl2Dsl = copy(rt = rType)

    def apply[F[_]: Functor](body: (XQuery, XQuery) => F[XQuery]): F[FunctionDecl2] =
      body(p1.name.xqy, p2.name.xqy) map (FunctionDecl2(fn, p1, p2, rt, _))

    def apply(body: (XQuery, XQuery) => XQuery): FunctionDecl2 =
      apply[Id](body)
  }

  final case class FunctionDecl3Dsl(fn: xml.QName, p1: FunctionParam, p2: FunctionParam, p3: FunctionParam, rt: SequenceType) {
    def as(rType: SequenceType): FunctionDecl3Dsl = copy(rt = rType)

    def apply[F[_]: Functor](body: (XQuery, XQuery, XQuery) => F[XQuery]): F[FunctionDecl3] =
      body(p1.name.xqy, p2.name.xqy, p3.name.xqy) map (FunctionDecl3(fn, p1, p2, p3, rt, _))

    def apply(body: (XQuery, XQuery, XQuery) => XQuery): FunctionDecl3 =
      apply[Id](body)
  }
}
