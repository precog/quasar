/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.physical.marklogic.xml

import eu.timepit.refined.auto._
import xml.name._

object namespaces {
  // NB: We've choosen to only support EJSON maps with string keys for the
  //     time-being as that maps reasonably to XML elements.
  val ejsonNs         = Namespace(NSPrefix(NCName("ejson")), NSUri("http://quasar-analytics.org/ejson"))
  val ejsonEjson      = ejsonNs(NCName("ejson"))
  val ejsonArrayElt   = ejsonNs(NCName("array-element"))
  val ejsonType       = ejsonNs(NCName("type"))

  // Related to encoding of non-QName elements
  val ejsonEncodedName = ejsonNs(NCName("key"))
  val ejsonEncodedAttr = ejsonNs(NCName("key-id"))

  val filesystemNs    = Namespace(NSPrefix(NCName("filesystem")), NSUri("http://quasar-analytics.org/filesystem"))
  val filesystemError = filesystemNs(NCName("error"))

  val qscriptNs       = Namespace(NSPrefix(NCName("qscript")), NSUri("http://quasar-analytics.org/qscript"))
}
