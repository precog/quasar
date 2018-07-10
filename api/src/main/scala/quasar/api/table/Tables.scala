/*
 * Copyright 2014–2018 SlamData Inc.
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

import quasar.Condition
import scalaz.{\/, NonEmptyList}

/** @tparam I identity
  * @tparam D materialized table data
  */
trait Tables[F[_], G[_], I, D] {
  import TableError.{CreationError, ExistenceError, ModificationError, PrePreparationError}

  def allTables: F[G[(I, Table)]]

  def createTable(table: Table): F[CreationError \/ I]

  def table(tableId: I): F[ExistenceError[I] \/ Table]

  def setTableAttributes(tableId: I, attributes: NonEmptyList[TableAttribute]): F[Condition[ModificationError[I]]]

  def prepareTable(tableId: I): F[Condition[PrePreparationError[I]]]

  def preparationStatus(tableId: I): F[ExistenceError[I] \/ PreparationStatus]

  def preparedData(tableId: I): F[ExistenceError[I] \/ PreparationResult[D]]
}
