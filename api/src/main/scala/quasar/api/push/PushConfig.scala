/*
 * Copyright 2020 Precog Data
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

package quasar.api.push

import slamdata.Predef.{Eq => _, _}

import quasar.api.{Column, ColumnType}
import quasar.api.resource.ResourcePath

import cats.{Apply, Eq, Eval, NonEmptyTraverse, Show}
import cats.data.NonEmptyList
import cats.implicits._

import monocle.{Lens, PLens, Prism}

import shims.{equalToCats, functorToScalaz, showToCats}

sealed trait PushConfig[O, +Q] extends Product with Serializable {
  def path: ResourcePath
  def query: Q
  def columns: PushConfig.Columns
}

object PushConfig {
  type OutputColumn = Column[(ColumnType.Scalar, SelectedType)]
  type Columns = NonEmptyList[OutputColumn]

  final case class Full[O, +Q](
      path: ResourcePath,
      query: Q,
      columns: Columns)
      extends PushConfig[O, Q]

  final case class Incremental[O, +Q](
      path: ResourcePath,
      query: Q,
      outputColumns: List[OutputColumn],
      resumeConfig: ResumeConfig[O],
      initialOffset: Option[OffsetKey.Actual[O]])
      extends PushConfig[O, Q] {

    def columns: Columns =
      NonEmptyList(
        resumeConfig.resultIdColumn.map(_.leftMap(IdType.scalarP(_))),
        outputColumns)
  }

  final case class SourceDriven[O, +Q](
      path: ResourcePath,
      query: Q,
      columns: Columns,
      offsetTag: OffsetKey.Formal[Unit, O],
      offsetPath: OffsetPath)
      extends PushConfig[O, Q]


  def full[O, Q]: Prism[PushConfig[O, Q], (ResourcePath, Q, Columns)] =
    Prism.partial[PushConfig[O, Q], (ResourcePath, Q, Columns)] {
      case Full(p, q, c) => (p, q, c)
    } ((Full[O, Q] _).tupled)

  def incremental[O, Q]: Prism[PushConfig[O, Q], (ResourcePath, Q, List[OutputColumn], ResumeConfig[O], Option[OffsetKey.Actual[O]])] =
    Prism.partial[PushConfig[O, Q], (ResourcePath, Q, List[OutputColumn], ResumeConfig[O], Option[OffsetKey.Actual[O]])] {
      case Incremental(p, q, c, r, o) => (p, q, c, r, o)
    } ((Incremental[O, Q] _).tupled)

  def sourceDriven[O, Q]: Prism[PushConfig[O, Q], (ResourcePath, Q, Columns, OffsetKey.Formal[Unit, O], OffsetPath)] =
    Prism.partial[PushConfig[O, Q], (ResourcePath, Q, Columns, OffsetKey.Formal[Unit, O], OffsetPath)] {
      case SourceDriven(p, q, c, r, op) => (p, q, c, r, op)
    } ((SourceDriven[O, Q] _).tupled)

  def query[O, Q1, Q2]: PLens[PushConfig[O, Q1], PushConfig[O, Q2], Q1, Q2] =
    PLens[PushConfig[O, Q1], PushConfig[O, Q2], Q1, Q2](_.query)(q2 => {
      case f @ Full(_, _, _) => f.copy(query = q2)
      case i @ Incremental(_, _, _, _, _) => i.copy(query = q2)
      case s @ SourceDriven(_, _, _, _, _) => s.copy(query = q2)
    })

  def path[O, Q]: Lens[PushConfig[O, Q], ResourcePath] =
    Lens[PushConfig[O, Q], ResourcePath](_.path)(p2 => {
      case f @ Full(_, _, _) => f.copy(path = p2)
      case i @ Incremental(_, _, _, _, _) => i.copy(path = p2)
      case s @ SourceDriven(_, _, _, _, _) => s.copy(path = p2)
    })

  implicit def pushConfigEq[O, Q: Eq]: Eq[PushConfig[O, Q]] =
    Eq.by(p => (full[O, Q].getOption(p), incremental[O, Q].getOption(p), sourceDriven[O, Q].getOption(p)))

  implicit def pushConfigShow[O, Q: Show]: Show[PushConfig[O, Q]] =
    Show show {
      case Full(p, q, c) =>
        s"Full(${p.show}, ${q.show}, ${c.map(_.name).show})"

      case i @ Incremental(p, q, c, r, o) =>
        s"Incremental(${p.show}, ${q.show}, ${c.map(_.name).show}, ${r.show}, ${o.show})"

      case s @ SourceDriven(p, q, c, r, op) =>
        s"SourceDriven(${p.show}, ${q.show}, ${c.map(_.name).show}, ${r.show}, ${op.show})"
    }

  implicit def pushConfigNonEmptyTraverse[O]: NonEmptyTraverse[PushConfig[O, ?]] =
    new NonEmptyTraverse[PushConfig[O, ?]] {
      def nonEmptyTraverse[F[_]: Apply, A, B](fa: PushConfig[O, A])(f: A => F[B]) =
        query[O, A, B].modifyF(f)(fa)

      def foldLeft[A, B](fa: PushConfig[O, A], b: B)(f: (B, A) => B): B =
        f(b, query[O, A, A].get(fa))

      def foldRight[A, B](fa: PushConfig[O, A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        f(query[O, A, A].get(fa), lb)

      def reduceLeftTo[A, B](fa: PushConfig[O, A])(f: A => B)(g: (B, A) => B): B =
        f(query[O, A, A].get(fa))

      def reduceRightTo[A, B](fa: PushConfig[O, A])(f: A => B)(g: (A, Eval[B]) => Eval[B]): Eval[B] =
        Eval.now(f(query[O, A, A].get(fa)))
    }
}
