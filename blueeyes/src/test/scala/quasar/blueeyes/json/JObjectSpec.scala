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

package quasar.blueeyes
package json

import quasar.precog.TestSupport._

class JObjectSpec extends Specification {
  "JObjects equal even fields order is different" in {
    JParser.parseUnsafe(j1) mustEqual (JParser.parseUnsafe(j2))
  }

  val j1 = """{
  "eSzgqekhuzwtceLxkkwysoqKig499526":[[null]],
  "swss249444":"yzGnt",
  "IfnufnjhtztlieemsDegZwlmhwM132924":[["ioucxudplkylHantqbvmpwlClhzbtj",[],{
    "n148475":{
      "fdqlg315362":"kNkp",
      "fry389722":null,
      "eqmweewcuusofowowobvxtlgeujfy71417":null,
      "eTzlyigburigibGoisz729377":{

      }
    },
    "lpnclThlxzRoyzsXiiwyIyEurpEhc476273":4.2965233947521606E307
  },{
    "poxrqllcxS298691":true
  }],{
    "kpukwfmaupzHjc787433":4.2821341208590467E306,
    "zolrhipcnplSqgHkXbqlkDml429336":[],
    "cuhfwLwvpymhZomu305530":[[],1044579423,{
      "ZlsflrxpzyfbvVzcklrfqlnhjqp597093":null,
      "xJkktudxsycehurghehskvsdlPz229613":null,
      "ohwjgnughabpgwjzr268297":{

      },
      "fbGzycrfcvufbomsy823876":null
    }],
    "bvVzzgyogtzkKjPx891350":[["iedlbeWfkSrogIh","nysytthrxtgbfUxvugevtopkoFq"],{

    },false],
    "ntm989887":null
  }],
  "cKfCsaquqbvkrfkykrpby510244":false
}"""

  val j2 = """{
  "IfnufnjhtztlieemsDegZwlmhwM132924":[["ioucxudplkylHantqbvmpwlClhzbtj",[],{
    "lpnclThlxzRoyzsXiiwyIyEurpEhc476273":4.2965233947521606E307,
    "n148475":{
      "fdqlg315362":"kNkp",
      "fry389722":null,
      "eqmweewcuusofowowobvxtlgeujfy71417":null,
      "eTzlyigburigibGoisz729377":{

      }
    }
  },{
    "poxrqllcxS298691":true
  }],{
    "kpukwfmaupzHjc787433":4.2821341208590467E306,
    "cuhfwLwvpymhZomu305530":[[],1044579423,{
      "ohwjgnughabpgwjzr268297":{

      },
      "ZlsflrxpzyfbvVzcklrfqlnhjqp597093":null,
      "xJkktudxsycehurghehskvsdlPz229613":null,
      "fbGzycrfcvufbomsy823876":null
    }],
    "bvVzzgyogtzkKjPx891350":[["iedlbeWfkSrogIh","nysytthrxtgbfUxvugevtopkoFq"],{

    },false],
    "ntm989887":null,
    "zolrhipcnplSqgHkXbqlkDml429336":[]
  }],
  "swss249444":"yzGnt",
  "cKfCsaquqbvkrfkykrpby510244":false,
  "eSzgqekhuzwtceLxkkwysoqKig499526":[[null]]
}"""
}
