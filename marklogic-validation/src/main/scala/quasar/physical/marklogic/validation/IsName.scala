/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.marklogic.validation

import scala.Predef.String
import scala.StringContext

import eu.timepit.refined.api.Validate
import org.apache.xerces.util.XMLChar

/** Refined predicate that checks if a `String` is a valid XML Name.
  * @see https://www.w3.org/TR/xml/#NT-Name
  */
final case class IsName()

object IsName {
  implicit def isNameValidate: Validate.Plain[String, IsName] =
    Validate.fromPredicate(XMLChar.isValidName(_), s => s"""isValidName("$s")""", IsName())
}
