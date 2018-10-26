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

package quasar.impl.datasource.local

import quasar.api.resource.{ResourceName, ResourcePath}
import quasar.connector.{DatasourceSpec, ResourceError}
import quasar.contrib.scalaz.MonadError_

import java.nio.file.Paths
import scala.concurrent.ExecutionContext.Implicits.global

import cats.effect.IO
import fs2.Stream
import shims._

final class LocalDatasourceSpec
    extends DatasourceSpec[IO, Stream[IO, ?]] {

  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  implicit val tmr = IO.timer(global)

  val datasource =
    LocalDatasource[IO](Paths.get("./it/src/main/resources/tests"), 1024, global)

  val nonExistentPath =
    ResourcePath.root() / ResourceName("non") / ResourceName("existent")

  def gatherMultiple[A](g: Stream[IO, A]) = g.compile.toList
}
