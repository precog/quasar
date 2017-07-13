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
import scalaz.Scalaz._

class Http4sPathEncodingSpec extends quasar.Qspec {
  val codec = UriPathCodec

  "print and parse through http4s should produce same Path" >> {
    "absolute file with plus" >> {
      val path = rootDir </> file("a+b/c")
      val hpath = HPath(codec.printPath(path))
      AsFilePath.unapply(hpath) must_= Some(path)
    }

    "absolute dir with plus" >> {
      val path = rootDir </> dir("a+b/c")
      val hpath = HPath(codec.printPath(path))
      AsDirPath.unapply(hpath) must_= Some(path)
    }

    "absolute file" >> prop { path: AFile =>
      val hpath = HPath(codec.printPath(path))
      AsFilePath.unapply(hpath) must_= Some(path)
    }
    "absolute dir" >> prop { path: ADir =>
      val hpath = HPath(codec.printPath(path))
      AsDirPath.unapply(hpath) must_= Some(path)
    }
  }
}
