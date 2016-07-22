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

package quasar.api

import quasar.Predef._
import quasar._
import fs._

import org.specs2.ScalaCheck
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._

/** This is largely copied from `pathy`. It would be nice to expose from a pathy
  * a function to validate a `PathCodec`. */
class UriPathCodecSpec extends quasar.QuasarSpecification with ScalaCheck {

  "UriPathCodec" should {
    val codec = UriPathCodec
    "print and parse again should produce same Path" >> {
      "absolute file" ! prop { path: AFile =>
        codec.parseAbsFile(codec.printPath(path)) must_== Some(path)
      }
      "relative file" ! prop { path: RFile =>
        codec.parseRelFile(codec.printPath(path)) must_== Some(path)
      }
      "absolute dir" ! prop { path: ADir =>
        codec.parseAbsDir(codec.printPath(path)) must_== Some(path)
      }
      "relative dir" ! prop { path: RDir =>
        codec.parseRelDir(codec.printPath(path)) must_== Some(path)
      }
    }

    "encode special characters properly" >> {
      "/" in {
        codec.printPath(rootDir </> dir("foo/bar") </> dir("baz")) must_== "/foo%2Fbar/baz/"
      }
      "." in {
        codec.printPath(rootDir </> dir(".") </> dir("baz")) must_== "/$dot$/baz/"
      }
      ".." in {
        codec.printPath(rootDir </> dir("..") </> dir("baz")) must_== "/$dotdot$/baz/"
      }
    }

  }
}
