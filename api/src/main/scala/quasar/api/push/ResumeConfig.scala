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

import quasar.api.Column

import cats.{Eq, Show}
import cats.implicits._

import monocle.macros.Lenses

/** Configuration required to resume an incremental push. */
@Lenses
final case class ResumeConfig[O](
    resultIdColumn: Column[(IdType, SelectedType)],
    resultOffsetColumn: Column[InternalKey.Formal[Unit, O]],
    sourceOffsetPath: OffsetPath)
/*
object ResumeConfig {
  implicit def resumeConfigEq[O]: Eq[ResumeConfig[O]] =
    Eq.by(c => (c.resultIdColumn, c.resultOffsetColumn, c.sourceOffsetPath))

  implicit def resumeConfigShow[O]: Show[ResumeConfig[O]] =
    Show.show(rc =>
      s"ResumeConfig(${rc.resultIdColumn.show}, ${rc.resultOffsetColumn.show}, ${rc.sourceOffsetPath.show})")
  */
