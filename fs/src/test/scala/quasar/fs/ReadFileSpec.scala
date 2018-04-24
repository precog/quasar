/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.fs

import scala.Predef.$conforms
import slamdata.Predef._
import quasar.contrib.scalaz.stream._

import scalaz._, Scalaz._

class ReadFileSpec extends quasar.Qspec with FileSystemFixture {

  "ReadFile" should {
    "scan should read data until an empty vector is received" >> prop {
      fs: SingleFileMemState =>

      val doRead = read.scanAll(fs.file).runLogCatch.run

      Mem.interpret(doRead).eval(fs.state) must_=== fs.contents.right.right
    }.set(maxSize = 10)

    "scan should automatically close the read handle when terminated early" >> prop {
      fs: SingleFileMemState => fs.contents.nonEmpty ==> {
        val n = fs.contents.length / 2
        val doRead = read.scanAll(fs.file).take(n).runLogCatch.run

        Mem.interpret(doRead).run(fs.state).leftMap(_.rm) must_===
          ((Map.empty, (fs.contents take n).right.right))
      }
    }.set(maxSize = 10)
  }
}
