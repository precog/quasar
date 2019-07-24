/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.impl.parsing

import slamdata.Predef.{Option, Some, String, Throwable}

import quasar.api.resource.ResourcePath
import quasar.connector.{ParsableType, ResourceError}
import quasar.connector.ParsableType.JsonVariant

import tectonic.{IncompleteParseException, ParseException}

object TectonicResourceError {
  def apply(path: ResourcePath, tpe: ParsableType, cause: Throwable): Option[ResourceError] =
    Some(cause) collect {
      case ParseException(msg, _, _, _) =>
        ResourceError.malformedResource(path, typeSummary(tpe), Some(msg), Some(cause))

      case IncompleteParseException(msg) =>
        ResourceError.malformedResource(path, typeSummary(tpe), Some(msg), Some(cause))
    }

  val typeSummary: ParsableType => String = {
    case ParsableType.Json(vnt, isPrecise) =>
      JsonVariant.stringP(vnt) + (if (isPrecise) " (precise)" else "")
  }
}
