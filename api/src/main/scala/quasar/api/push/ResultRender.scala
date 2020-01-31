/*
 * Copyright 2014–2020 SlamData Inc.
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

import slamdata.Predef._

import quasar.api.table.TableColumn

import fs2.Stream

trait ResultRender[F[_], I] {

  def renderCsv(
      input: I,
      columns: List[TableColumn],
      config: RenderConfig.Csv,
      limit: Option[Long])
      : Stream[F, Byte]

  def renderJson(
      input: I,
      prefix: String,
      delimiter: String,
      suffix: String)
      : Stream[F, Byte]
}
