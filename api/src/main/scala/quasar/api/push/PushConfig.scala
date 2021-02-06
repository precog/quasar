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

import cats.data.NonEmptyList
import cats.implicits._

import monocle.{Lens, PLens, Prism}

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
      initialOffset: Option[InternalKey.Actual[O]])
      extends PushConfig[O, Q] {

    def columns: Columns =
      NonEmptyList(
        resumeConfig.resultIdColumn.map(_.leftMap(IdType.scalarP(_))),
        outputColumns)
  }

  final case class SourceDriven[+Q](
      path: ResourcePath,
      query: Q,
      outputColumns: PushColumns[OutputColumn])
      extends PushConfig[ExternalOffsetKey, Q] {
    def columns: Columns = outputColumns.toNel
  }


  def full[O, Q]: Prism[PushConfig[O, Q], (ResourcePath, Q, Columns)] =
    Prism.partial[PushConfig[O, Q], (ResourcePath, Q, Columns)] {
      case Full(p, q, c) => (p, q, c)
    } ((Full[O, Q] _).tupled)

  def incremental[O, Q]: Prism[PushConfig[O, Q], (ResourcePath, Q, List[OutputColumn], ResumeConfig[O], Option[InternalKey.Actual[O]])] =
    Prism.partial[PushConfig[O, Q], (ResourcePath, Q, List[OutputColumn], ResumeConfig[O], Option[InternalKey.Actual[O]])] {
      case Incremental(p, q, c, r, o) => (p, q, c, r, o)
    } ((Incremental[O, Q] _).tupled)

  def sourceDriven[Q]: Prism[PushConfig[ExternalOffsetKey, Q], (ResourcePath, Q, PushColumns[OutputColumn])] =
    Prism.partial[PushConfig[ExternalOffsetKey, Q], (ResourcePath, Q, PushColumns[OutputColumn])] {
      case SourceDriven(p, q, c) => (p, q, c)
    } ((SourceDriven[Q] _).tupled)

  def query[O, Q1, Q2]: PLens[PushConfig[O, Q1], PushConfig[O, Q2], Q1, Q2] =
    PLens[PushConfig[O, Q1], PushConfig[O, Q2], Q1, Q2](_.query)(q2 => {
      case f @ Full(_, _, _) => f.copy(query = q2)
      case i @ Incremental(_, _, _, _, _) => i.copy(query = q2)
      case s @ SourceDriven(_, _, _) => s.copy(query = q2)
    })

  def path[O, Q]: Lens[PushConfig[O, Q], ResourcePath] =
    Lens[PushConfig[O, Q], ResourcePath](_.path)(p2 => {
      case f @ Full(_, _, _) => f.copy(path = p2)
      case i @ Incremental(_, _, _, _, _) => i.copy(path = p2)
      case s @ SourceDriven(_, _, _) => s.copy(path = p2)
    })
}
