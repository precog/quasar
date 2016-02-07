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

package quasar.fs.mount

import quasar.Predef._
import quasar.{Variables, VarName, VarValue}
import quasar.fp.prism._
import quasar.sql

import argonaut._, Argonaut._
import org.specs2.mutable
import org.specs2.scalaz._

class MountConfigSpec extends mutable.Specification with DisjunctionMatchers {
  import MountConfig2._

  "View config codec" should {
    val read = sql.Select(
      sql.SelectAll,
      List(sql.Proj(sql.Splice(None), None)),
      Some(sql.TableRelationAST("zips", None)),
      None, None, None)

    def viewJson(uri: String) =
      Json("view" := Json("connectionUri" := uri))

    "encode" >> {
      "no vars" in {
        viewConfig(read, Variables(Map()))
          .asJson must_== viewJson("sql2:///?q=%28select%20%2A%20from%20zips%29")
      }

      "with var" in {
        viewConfig(read, Variables(Map(VarName("a") -> VarValue("1"))))
          .asJson must_== viewJson("sql2:///?q=%28select%20%2A%20from%20zips%29&var.a=1")
      }
    }

    "decode" >> {
      "no vars" >> {
        viewJson("sql2:///?q=%28select+*+from+zips%29")
          .as[MountConfig2].toDisjunction must
            beRightDisjunction(ViewConfig(read, Variables(Map())))
      }

      "decode with var" in {
        // NB: the _last_ value for a given var name is used.
        viewJson("sql2:///?q=%28select+*+from+zips%29&var.a=1&var.b=2&var.b=3")
          .as[MountConfig2].toDisjunction must
            beRightDisjunction(
              ViewConfig(read, Variables(Map(
                VarName("a") -> VarValue("1"),
                VarName("b") -> VarValue("3")))))
      }

      "decode with missing query" in {
        viewJson("sql2:///?p=foo")
          .as[MountConfig2].toDisjunction.leftMap(_._1) must
            beLeftDisjunction("missing query: sql2:///?p=foo")
      }

      "decode with bad scheme" in {
        viewJson("foo:///?q=%28select+*+from+zips%29")
          .as[MountConfig2].toDisjunction.leftMap(_._1) must
            beLeftDisjunction("unrecognized scheme: foo")
      }

      "decode with unparseable URI" in {
        viewJson("?")
          .as[MountConfig2].toDisjunction.leftMap(_._1) must
            beLeftDisjunction("missing URI scheme: ?")
      }

      "decode with bad encoding" in {
        viewJson("sql2:///?q=%F%28select+*+from+zips%29")
          .as[MountConfig2].toDisjunction.leftMap(_._1) must beLeftDisjunction
      }
    }
  }
}
