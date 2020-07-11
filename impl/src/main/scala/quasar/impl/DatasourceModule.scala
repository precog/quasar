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

package quasar.impl

import quasar.api.datasource.DatasourceType
import quasar.api.datasource.DatasourceError.ConfigurationError
import quasar.connector.datasource.{HeavyweightDatasourceModule, LightweightDatasourceModule, Reconfiguration}

import scala.util.Either

import argonaut.Json

sealed trait DatasourceModule {
  def kind: DatasourceType

  def sanitizeConfig(config: Json): Json

  def migrateConfig(config: Json)
      : Either[ConfigurationError[Json], Json]

  def reconfigure(original: Json, patch: Json)
      : Either[ConfigurationError[Json], (Reconfiguration, Json)]
}

object DatasourceModule {
  final case class Lightweight(lw: LightweightDatasourceModule) extends DatasourceModule {
    def kind = lw.kind

    def sanitizeConfig(config: Json): Json = lw.sanitizeConfig(config)

    def migrateConfig(config: Json)
        : Either[ConfigurationError[Json], Json] =
      lw.migrateConfig(config)

    def reconfigure(original: Json, patch: Json)
        : Either[ConfigurationError[Json], (Reconfiguration, Json)] =
      lw.reconfigure(original, patch)
  }

  final case class Heavyweight(hw: HeavyweightDatasourceModule) extends DatasourceModule {
    def kind = hw.kind

    def sanitizeConfig(config: Json): Json = hw.sanitizeConfig(config)

    def migrateConfig(config: Json)
        : Either[ConfigurationError[Json], Json] =
      hw.migrateConfig(config)

    def reconfigure(original: Json, patch: Json)
        : Either[ConfigurationError[Json], (Reconfiguration, Json)] =
      hw.reconfigure(original, patch)
  }
}
