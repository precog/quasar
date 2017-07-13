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

package quasar.api

import slamdata.Predef._
import quasar.contrib.pathy._

import org.http4s.dsl.{Path => HPath}
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._

class AsPathSpec extends quasar.Qspec {
  "AsPath" should {
    "decode any Path we can throw at it" >> {
      "AbsFile" >> prop { file: AFile =>
        val httpPath = HPath(UriPathCodec.printPath(file))
        AsFilePath.unapply(httpPath) must_== Some(file)
      }
      "AbsDir" >> prop { dir : ADir =>
        val httpPath = HPath(UriPathCodec.printPath(dir))
        AsDirPath.unapply(httpPath) must_== Some(dir)
      }
    }

    "decode root" in {
      val httpPath = HPath("/")
      AsDirPath.unapply(httpPath) must_== Some(rootDir)
    }

    "decode escaped /" in {
      val httpPath = HPath("/foo%2Fbar/baz/")
      AsDirPath.unapply(httpPath) must beSome(rootDir </> dir("foo/bar") </> dir("baz"))
    }
  }
}
