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

package quasar.api.table

import slamdata.Predef._

import scalaz.Show
import scalaz.syntax.show._

sealed trait TableError[+I] extends Product with Serializable

object TableError {
  sealed trait ModificationError[+I] extends TableError[I]

  sealed trait CreateError[+I] extends ModificationError[I]
  final case class NameConflict(name: TableName) extends CreateError[Nothing]

  sealed trait ExistenceError[+I] extends ModificationError[I]
  final case class TableNotFound[I](tableId: I) extends ExistenceError[I]

  implicit def showCreateError[I]: Show[CreateError[I]] =
    Show.shows {
      case NameConflict(n) =>
        "NameConflict(" + n.shows + ")"
    }

  implicit def showExistenceError[I: Show]: Show[ExistenceError[I]] =
    Show.shows {
      case TableNotFound(id) =>
        "TableNotFound(" + id.shows + ")"
    }

  implicit def showModificationError[I: Show]: Show[ModificationError[I]] =
    Show.shows {
      case c: CreateError[I] => c.shows
      case e: ExistenceError[I] => e.shows
    }
}
