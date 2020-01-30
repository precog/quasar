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

package quasar.run

import slamdata.Predef.{Exception, Product, Serializable, Throwable}

import quasar.api.datasource.DatasourceError.CreateError
import quasar.api.destination.DestinationError.{CreateError => DestCreateError}
import quasar.compile.SemanticErrors
import quasar.connector.ResourceError
import quasar.qscript.PlannerError
import quasar.run.store.StoreError
import quasar.sql.ParsingError

import argonaut.Json
import argonaut.JsonScalaz._
import monocle.Prism
import scalaz.Show
import scalaz.syntax.show._

import shims.{showToCats, showToScalaz}

sealed abstract trait QuasarError extends Product with Serializable

object QuasarError {
  final case class Pushing(error: DestCreateError[Json]) extends QuasarError
  final case class Compiling(errors: SemanticErrors) extends QuasarError
  final case class Connecting(error: CreateError[Json]) extends QuasarError
  final case class Evaluating(error: ResourceError) extends QuasarError
  final case class Parsing(error: ParsingError) extends QuasarError
  final case class Planning(error: PlannerError) extends QuasarError
  final case class Storing(error: StoreError) extends QuasarError

  val pushing: Prism[QuasarError, DestCreateError[Json]] =
    Prism.partial[QuasarError, DestCreateError[Json]] {
      case Pushing(err) => err
    } (Pushing(_))

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

  val storing: Prism[QuasarError, StoreError] =
    Prism.partial[QuasarError, StoreError] {
      case Storing(err) => err
    } (Storing(_))

  val throwableP: Prism[Throwable, QuasarError] =
    Prism.partial[Throwable, QuasarError] {
      case QuasarException(qe) => qe
    } (QuasarException(_))

  implicit val show: Show[QuasarError] =
    Show show {
      case Compiling(e) => e.show
      case Connecting(e) => e.show
      case Evaluating(e) => e.show
      case Parsing(e) => e.show
      case Planning(e) => e.show
      case Storing(e) => e.show
      case Pushing(e) => e.show
    }

  ////

  private final case class QuasarException(qe: QuasarError) extends Exception(qe.shows)
}
