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

import slamdata.Predef.String

import javax.xml.parsers.SAXParserFactory
import org.xml.sax.SAXException
import scala.xml.Elem
import scala.xml.factory.XMLLoader

import scalaz.\/

/** Provides methods for securely parsing XML documents, avoiding known DoS attacks.
  *
  * @see https://github.com/scala/scala-xml/issues/17
  * @see https://github.com/akka/akka/pull/17660/files#diff-3f57ed15f4aa764e53d971ec647b544fR47
  */
object SecureXML {
  def loadString(s: String): SAXException \/ Elem =
    \/.fromTryCatchThrowable[Elem, SAXException](loader.loadString(s))

  ////

  private val loader: XMLLoader[Elem] = new XMLLoader[Elem] {
    override def parser = {
      val factory = SAXParserFactory.newInstance()
      import com.sun.org.apache.xerces.internal.impl.Constants
      import javax.xml.XMLConstants

      factory.setFeature(Constants.SAX_FEATURE_PREFIX + Constants.EXTERNAL_GENERAL_ENTITIES_FEATURE, false)
      factory.setFeature(Constants.SAX_FEATURE_PREFIX + Constants.EXTERNAL_PARAMETER_ENTITIES_FEATURE, false)
      factory.setFeature(Constants.XERCES_FEATURE_PREFIX + Constants.DISALLOW_DOCTYPE_DECL_FEATURE, true)
      factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true)
      factory.newSAXParser()
    }
  }
}
