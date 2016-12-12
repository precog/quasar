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

package quasar.physical.marklogic.xquery

import quasar.Predef._
import quasar.Data
import quasar.physical.marklogic.xquery.syntax._

import scalaz._

final class QScriptLibSpec extends XQuerySpec {
  import expr.{attribute, element}

  xquerySpec(bn => s"XQuery QScript Library (${bn.name})") { eval =>
    "meta" >> {
      "returns element attributes as an element" >> {
        val order = element("order".xs)(mkSeq_(
          attribute("orderId".xs)(2345.xqy),
          attribute("quantity".xs)(23.xqy),
          "Wibble Wabble".xs
        ))
        eval(qscript.meta[M] apply order) must resultIn(Data.Obj(
          "orderId"  -> Data._str("2345"),
          "quantity" -> Data._str("23")
        ))
      }

      "returns empty seq when not an element" >> {
        eval(qscript.meta[M] apply "foobar".xs).toOption must beNone
      }
    }
  }
}
