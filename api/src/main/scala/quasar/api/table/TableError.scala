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

import slamdata.Predef.{Product, Serializable}

import scalaz.{Cord, Show}
import scalaz.syntax.show._

sealed trait TableError extends Product with Serializable

object TableError {
  final case class PreparationNotInProgress[I](tableId: I) extends TableError

  sealed trait ModificationError[I] extends TableError
  final case class PreparationExists[I](tableId: I) extends ModificationError[I]

  sealed trait CreateError[I] extends ModificationError[I]
  final case class NameConflict[I](name: TableName) extends CreateError[I]

  sealed trait PrePreparationError[I] extends ModificationError[I]
  final case class PreparationInProgress[I](tableId: I) extends PrePreparationError[I]

  sealed trait ExistenceError[I] extends ModificationError[I] with PrePreparationError[I]
  final case class TableNotFound[I](tableId: I) extends ExistenceError[I]

  implicit def showPreparationNotInProgress[I: Show]: Show[PreparationNotInProgress[I]] =
    Show.show {
      case PreparationNotInProgress(id) =>
        Cord("PreparationNotInProgress(") ++ id.show ++ Cord(")")
    }

  implicit def showModificationError[I: Show]: Show[ModificationError[I]] =
    Show.show {
      case PreparationExists(id) =>
        Cord("PreparationExists(") ++ id.show ++ Cord(")")
      case e:CreateError[I] => e.show
      case e:PrePreparationError[I] => e.show
    }

  implicit def showCreateError[I]: Show[CreateError[I]] =
    Show.show {
      case NameConflict(n) =>
        Cord("NameConflict(") ++ n.show ++ Cord(")")
    }

  implicit def showPrePreparationError[I: Show]: Show[PrePreparationError[I]] =
    Show.show {
      case PreparationInProgress(id) =>
        Cord("PreparationInProgress(") ++ id.show ++ Cord(")")
      case e:ExistenceError[I] => e.show
    }

  implicit def showExistenceError[I: Show]: Show[ExistenceError[I]] =
    Show.show {
      case TableNotFound(id) =>
        Cord("TableNotFound(") ++ id.show ++ Cord(")")
    }
}
