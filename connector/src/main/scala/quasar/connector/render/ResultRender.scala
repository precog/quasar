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

import quasar.api.{Column, ColumnType}
import quasar.api.push.{IdType, OffsetKey}
import quasar.connector.{AppendEvent, DataEvent}

import cats.data.NonEmptyList

import fs2.Stream

trait ResultRender[F[_], I] {
  def render[A](
      input: I,
      columns: NonEmptyList[Column[ColumnType.Scalar]],
      config: RenderConfig[A],
      rowLimit: Option[Long])
      : Stream[F, A]

  def renderUpserts[A, P](
      input: RenderInput[I],
      idColumn: Column[IdType],
      offsetColumn: Column[OffsetKey.Formal[Unit, A]],
      renderedColumns: NonEmptyList[Column[ColumnType.Scalar]],
      config: RenderConfig[P],
      rowLimit: Option[Long])
      : Stream[F, DataEvent[P, OffsetKey.Actual[A]]]

  def renderAppend[P](
      input: I,
      columns: NonEmptyList[Column[ColumnType.Scalar]],
      config: RenderConfig[P],
      rowLimit: Option[Long])
      : Stream[F, AppendEvent[P]]
}
