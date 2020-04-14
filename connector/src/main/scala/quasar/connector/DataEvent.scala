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

import slamdata.Predef._

import cats.{Eq, Show}
import cats.instances.byte._
import cats.syntax.eq._
import cats.syntax.show._

import fs2.Chunk

sealed trait DataEvent[+I, +O] extends Product with Serializable

object DataEvent {
  final case class Replace[I](id: I, newRecord: Chunk[Byte]) extends DataEvent[I, Nothing]

  sealed trait Primitive[+I, +O] extends DataEvent[I, O]
  final case class Create(records: Chunk[Byte]) extends Primitive[Nothing, Nothing]
  final case class Delete[I](recordIds: IdBatch[I]) extends Primitive[I, Nothing]

  /** A transaction boundary, consumers should treat all events since the
    * previous `Commit` (or the start of the stream) as part of a single
    * transaction, if possible.
    */
  final case class Commit[O](offset: O) extends Primitive[Nothing, O]

  implicit def dataEventEq[I: Eq, O: Eq, E <: DataEvent[I, O]]: Eq[E] =
    Eq instance {
      case (Replace(idx, rx), Replace(idy, ry)) => idx === idy && rx === ry
      case (Create(rx), Create(ry)) => rx === ry
      case (Delete(idsx), Delete(idsy)) => idsx === idsy
      case (Commit(ox), Commit(oy)) => ox === oy
      case _ => false
    }

  implicit def dataEventShow[I: Show, O: Show, E <: DataEvent[I, O]]: Show[E] = {
    Show show {
      case Replace(i, r) => s"Replace(${i.show}, ${r.size} bytes)"
      case Create(rs) => s"Create(${rs.size} bytes)"
      case Delete(ids) => s"Delete(${ids.show})"
      case Commit(o) => s"Commit(${o.show})"
    }
  }
}
