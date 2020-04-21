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

package quasar.connector.render

import slamdata.Predef._

import quasar.api.Column
import quasar.api.table.TableColumn
import quasar.connector.{ActualKey, DataEvent, IdType, TypedKey}

import cats.data.NonEmptyList

import fs2.Stream

trait ResultRender[F[_], I] {
  def render(
      input: I,
      columns: List[TableColumn],
      config: RenderConfig,
      rowLimit: Option[Long])
      : Stream[F, Byte]

  def renderUpserts[A](
      input: Input[I],
      idColumn: Column[IdType],
      offsetColumn: Column[TypedKey[Unit, A]],
      renderedColumns: NonEmptyList[TableColumn],
      config: RenderConfig.Csv,
      limit: Option[Long])
      : Stream[F, DataEvent[ActualKey[A]]]
}
