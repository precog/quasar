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

package quasar.connector

import slamdata.Predef.{Eq => _, _}

import cats._
import cats.implicits._

import fs2.Chunk

sealed trait DataEvent[+P, +O] extends Product with Serializable
sealed trait AppendEvent[+P] extends DataEvent[P, Nothing]

object DataEvent {
  final case class Create[P](records: Chunk[P]) extends AppendEvent[P]
  final case class Delete(recordIds: IdBatch) extends DataEvent[Nothing, Nothing]

  /** A transaction boundary, consumers should treat all events since the
    * previous `Commit` (or the start of the stream) as part of a single
    * transaction, if possible.
    */
  final case class Commit[O](offset: O) extends DataEvent[Nothing, O]
  final case object AppendCommit extends AppendEvent[Nothing]

  def create[P, O](records: Chunk[P]): DataEvent[P, O] =
    Create(records)

  def delete[P, O](recordIds: IdBatch): DataEvent[P, O] =
    Delete(recordIds)

  def commit[P, O](offset: O): DataEvent[P, O] =
    Commit(offset)

  def append[P, O]: DataEvent[P, O] =
    AppendCommit

  implicit def dataEventTraverse[P]: Traverse[DataEvent[P, ?]] =
    new Traverse[DataEvent[P, ?]] {
      def foldLeft[A, B](fa: DataEvent[P, A], b: B)(f: (B, A) => B): B =
        fa match {
          case Create(_) => b
          case Delete(_) => b
          case Commit(a) => f(b, a)
          case AppendCommit => b
        }

      def foldRight[A, B](fa: DataEvent[P, A], b: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        fa match {
          case Create(_) => b
          case Delete(_) => b
          case Commit(a) => f(a, b)
          case AppendCommit => b
        }

      def traverse[F[_]: Applicative, A, B](fa: DataEvent[P, A])(f: A => F[B]): F[DataEvent[P, B]] =
        fa match {
          case c @ Create(_) => (c: DataEvent[P, B]).pure[F]
          case d @ Delete(_) => (d: DataEvent[P, B]).pure[F]
          case Commit(a) => f(a).map(Commit(_))
          case AppendCommit => (AppendCommit: DataEvent[P, B]).pure[F]
        }
    }

  implicit def dataEventEq[P: Eq, O: Eq]: Eq[DataEvent[P, O]] =
    Eq by {
      case Create(cs) => (Some(cs), None, None, None)
      case Delete(ids) => (None, Some(ids), None, None)
      case Commit(o) => (None, None, Some(o), None)
      case AppendCommit => (None, None, None, Some(true))
    }

  implicit def dataEventShow[P, O: Show]: Show[DataEvent[P, O]] =
    Show show {
      case Create(rs) => s"Create(${rs.size} elements)"
      case Delete(ids) => s"Delete(${ids.show})"
      case Commit(o) => s"Commit(${o.show})"
      case AppendCommit => "AppendCommit"
    }
}
