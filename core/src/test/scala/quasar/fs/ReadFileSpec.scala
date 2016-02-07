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

package quasar.fs

import quasar.Predef._
import quasar.{Data, DataArbitrary}
import quasar.fp._

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._

class ReadFileSpec extends Specification with ScalaCheck with FileSystemFixture {
  import DataArbitrary._, FileSystemError._, PathError2._

  "ReadFile" should {
    "scan should read data until an empty vector is received" ! prop {
      (f: AFile, xs: Vector[Data]) =>

      val p = write.append(f, xs.toProcess).drain ++ read.scanAll(f)

      MemTask.runLogEmpty(p).run must_== \/-(xs)
    }

    "scan should automatically close the read handle when terminated early" ! prop {
      (f: AFile, xs: Vector[Data]) => xs.nonEmpty ==> {
        val n = xs.length / 2
        val p = write.append(f, xs.toProcess).drain ++ read.scanAll(f).take(n)

        MemTask.runLog(p).run.run(emptyMem)
          .run.leftMap(_.rm) must_== ((Map.empty, \/.right(xs take n)))
      }
    }

    "scan should automatically close the read handle on failure" ! prop {
      (f: AFile, xs: Vector[Data]) => xs.nonEmpty ==> {
        val reads = List(xs.right, pathError(PathNotFound(f)).left)

        MemFixTask.runLogWithReads(reads, read.scanAll(f)).run
          .leftMap(_.rm)
          .run(emptyMem)
          .run must_== ((Map.empty, \/.left(pathError(PathNotFound(f)))))
      }
    }
  }
}
