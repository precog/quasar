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

package quasar.physical.marklogic.xquery

import slamdata.Predef._

import scalaz._, Id.Id
import scalaz.std.option._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.foldable._
import scalaz.syntax.functor._
import scalaz.syntax.std.option._
import xml.name._

sealed abstract class FunctionDecl {
  def name: QName
  def parameters: NonEmptyList[TypedBindingName]
  def returnType: Option[SequenceType]
  def body: XQuery

  def arity: Int =
    parameters.length

  def render: String = {
    val paramsList = parameters.map(_.render).toList
    val retType = returnType.map(rt => s" as $rt").orZero
    s"declare function ${name}${paramsList.mkString("(", ", ", ")")}$retType {\n  $body\n}"
  }
}

object FunctionDecl {
  implicit val order: Order[FunctionDecl] =
    Order.orderBy(fd => (fd.name, fd.parameters, fd.returnType))

  implicit val show: Show[FunctionDecl] =
    Show.shows(fd => s"FunctionDecl(${fd.name})")

  final case class FunctionDecl1(
    name: QName,
    param1: TypedBindingName,
    returnType: Option[SequenceType],
    body: XQuery
  ) extends FunctionDecl {
    def parameters = NonEmptyList(param1)
  }

  final case class FunctionDecl2(
    name: QName,
    param1: TypedBindingName,
    param2: TypedBindingName,
    returnType: Option[SequenceType],
    body: XQuery
  ) extends FunctionDecl {
    def parameters = NonEmptyList(param1, param2)
  }

  final case class FunctionDecl3(
    name: QName,
    param1: TypedBindingName,
    param2: TypedBindingName,
    param3: TypedBindingName,
    returnType: Option[SequenceType],
    body: XQuery
  ) extends FunctionDecl {
    def parameters = NonEmptyList(param1, param2, param3)
  }

  final case class FunctionDecl4(
    name: QName,
    param1: TypedBindingName,
    param2: TypedBindingName,
    param3: TypedBindingName,
    param4: TypedBindingName,
    returnType: Option[SequenceType],
    body: XQuery
  ) extends FunctionDecl {
    def parameters = NonEmptyList(param1, param2, param3, param4)
  }

  final case class FunctionDecl5(
    name: QName,
    param1: TypedBindingName,
    param2: TypedBindingName,
    param3: TypedBindingName,
    param4: TypedBindingName,
    param5: TypedBindingName,
    returnType: Option[SequenceType],
    body: XQuery
  ) extends FunctionDecl {
    def parameters = NonEmptyList(param1, param2, param3, param4, param5)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  final case class FunctionDeclDsl(fname: QName) {
    def apply(p1: TypedBindingName): FunctionDecl1Dsl =
      FunctionDecl1Dsl(fname, p1, None)

    def apply(p1: TypedBindingName, p2: TypedBindingName): FunctionDecl2Dsl =
      FunctionDecl2Dsl(fname, p1, p2, None)

    def apply(p1: TypedBindingName, p2: TypedBindingName, p3: TypedBindingName): FunctionDecl3Dsl =
      FunctionDecl3Dsl(fname, p1, p2, p3, None)

    def apply(p1: TypedBindingName, p2: TypedBindingName, p3: TypedBindingName, p4: TypedBindingName): FunctionDecl4Dsl =
      FunctionDecl4Dsl(fname, p1, p2, p3, p4, None)

    def apply(p1: TypedBindingName, p2: TypedBindingName, p3: TypedBindingName, p4: TypedBindingName, p5: TypedBindingName): FunctionDecl5Dsl =
      FunctionDecl5Dsl(fname, p1, p2, p3, p4, p5, None)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  final case class FunctionDecl1Dsl(fn: QName, p1: TypedBindingName, rt: Option[SequenceType]) {
    def as(rType: SequenceType): FunctionDecl1Dsl = copy(rt = some(rType))

    def apply[F[_]: Functor](body: XQuery => F[XQuery]): F[FunctionDecl1] =
      body(~p1) map (FunctionDecl1(fn, p1, rt, _))

    def apply(body: XQuery => XQuery): FunctionDecl1 =
      apply[Id](body)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  final case class FunctionDecl2Dsl(fn: QName, p1: TypedBindingName, p2: TypedBindingName, rt: Option[SequenceType]) {
    def as(rType: SequenceType): FunctionDecl2Dsl = copy(rt = some(rType))

    def apply[F[_]: Functor](body: (XQuery, XQuery) => F[XQuery]): F[FunctionDecl2] =
      body(~p1, ~p2) map (FunctionDecl2(fn, p1, p2, rt, _))

    def apply(body: (XQuery, XQuery) => XQuery): FunctionDecl2 =
      apply[Id](body)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  final case class FunctionDecl3Dsl(fn: QName, p1: TypedBindingName, p2: TypedBindingName, p3: TypedBindingName, rt: Option[SequenceType]) {
    def as(rType: SequenceType): FunctionDecl3Dsl = copy(rt = some(rType))

    def apply[F[_]: Functor](body: (XQuery, XQuery, XQuery) => F[XQuery]): F[FunctionDecl3] =
      body(~p1, ~p2, ~p3) map (FunctionDecl3(fn, p1, p2, p3, rt, _))

    def apply(body: (XQuery, XQuery, XQuery) => XQuery): FunctionDecl3 =
      apply[Id](body)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  final case class FunctionDecl4Dsl(fn: QName, p1: TypedBindingName, p2: TypedBindingName, p3: TypedBindingName, p4: TypedBindingName, rt: Option[SequenceType]) {
    def as(rType: SequenceType): FunctionDecl4Dsl = copy(rt = some(rType))

    def apply[F[_]: Functor](body: (XQuery, XQuery, XQuery, XQuery) => F[XQuery]): F[FunctionDecl4] =
      body(~p1, ~p2, ~p3, ~p4) map (FunctionDecl4(fn, p1, p2, p3, p4, rt, _))

    def apply(body: (XQuery, XQuery, XQuery, XQuery) => XQuery): FunctionDecl4 =
      apply[Id](body)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  final case class FunctionDecl5Dsl(fn: QName, p1: TypedBindingName, p2: TypedBindingName, p3: TypedBindingName, p4: TypedBindingName, p5: TypedBindingName, rt: Option[SequenceType]) {
    def as(rType: SequenceType): FunctionDecl5Dsl = copy(rt = some(rType))

    def apply[F[_]: Functor](body: (XQuery, XQuery, XQuery, XQuery, XQuery) => F[XQuery]): F[FunctionDecl5] =
      body(~p1, ~p2, ~p3, ~p4, ~p5) map (FunctionDecl5(fn, p1, p2, p3, p4, p5, rt, _))

    def apply(body: (XQuery, XQuery, XQuery, XQuery, XQuery) => XQuery): FunctionDecl5 =
      apply[Id](body)
  }
}
