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

package quasar.run

import slamdata.Predef.{Exception, Product, Serializable, Throwable}
import quasar.api.datasource.DatasourceError.CreateError
import quasar.common.resource.ResourceError
import quasar.compile.SemanticErrors
import quasar.qscript.PlannerError
import quasar.sql.ParsingError
import quasar.yggdrasil.vfs.{ResourceError => MimirResourceError}

import argonaut.Json
import monocle.Prism

sealed abstract trait QuasarError extends Product with Serializable

object QuasarError {
  final case class Compiling(errors: SemanticErrors) extends QuasarError
  final case class Connecting(error: CreateError[Json]) extends QuasarError
  final case class Evaluating(error: ResourceError) extends QuasarError
  final case class Parsing(error: ParsingError) extends QuasarError
  final case class Planning(error: PlannerError) extends QuasarError
  final case class Storing(error: MimirResourceError) extends QuasarError

  val compiling: Prism[QuasarError, SemanticErrors] =
    Prism.partial[QuasarError, SemanticErrors] {
      case Compiling(errs) => errs
    } (Compiling(_))

  val connecting: Prism[QuasarError, CreateError[Json]] =
    Prism.partial[QuasarError, CreateError[Json]] {
      case Connecting(err) => err
    } (Connecting(_))

  val evaluating: Prism[QuasarError, ResourceError] =
    Prism.partial[QuasarError, ResourceError] {
      case Evaluating(err) => err
    } (Evaluating(_))

  val parsing: Prism[QuasarError, ParsingError] =
    Prism.partial[QuasarError, ParsingError] {
      case Parsing(err) => err
    } (Parsing(_))

  val planning: Prism[QuasarError, PlannerError] =
    Prism.partial[QuasarError, PlannerError] {
      case Planning(err) => err
    } (Planning(_))

  val storing: Prism[QuasarError, MimirResourceError] =
    Prism.partial[QuasarError, MimirResourceError] {
      case Storing(err) => err
    } (Storing(_))

  val throwableP: Prism[Throwable, QuasarError] =
    Prism.partial[Throwable, QuasarError] {
      case QuasarException(qe) => qe
    } (QuasarException(_))

  ////

  private final case class QuasarException(qe: QuasarError) extends Exception
}
