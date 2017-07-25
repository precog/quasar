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

package quasar.physical.marklogic.fs

import slamdata.Predef._
import quasar.Data
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.fs._
import quasar.sql.SqlStringContext

import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

final class DirectoryQueriesSpec extends MultiFormatFileSystemTest {

  def multiFormatFileSystemShould(js: BackendEffect ~> Task, xml: BackendEffect ~> Task) = {
    "Querying directory paths" >> {
      "results in a dataset comprised of immediate child documents" >> {
        val loc: ADir = rootDir </> dir("childdocs")
        val a: Data   = Data.Obj("a" -> Data._int(1))
        val b: Data   = Data.Obj("b" -> Data._int(2))
        val c: Data   = Data.Obj("c" -> Data._int(3))

        val setup = List(
          FileName("a") -> a,
          FileName("b") -> b,
          FileName("c") -> c
        ) traverse_ {
          case (n, o) => write.saveThese(loc </> file1(n), Vector(o)).void
        }

        // NB: We left shift here to get the flattend set of documents as the
        //     root of each file is an array of data.
        val lp   = fullCompileExp(sqlE"select (*)[*] from `/childdocs`")
        val eval = query.evaluate(lp) translate dropPhases

        (setup.liftM[Process] *> eval)
          .translate(runFsE(js))
          .runLog.run.map (_.toEither must beRight(contain(exactly(a, b, c))))
          .unsafePerformSync
      }

      "only includes child documents of the format configured for the mount" >> {
        val loc: ADir = rootDir </> dir("onlyfmt")
        val a: Data   = Data.Obj("a" -> Data._int(1))
        val b: Data   = Data.Obj("b" -> Data._int(2))
        val c: Data   = Data.Obj("c" -> Data._int(3))
        val d: Data   = Data.Obj("d" -> Data._int(4))
        val e: Data   = Data.Obj("e" -> Data._int(5))
        val f: Data   = Data.Obj("f" -> Data._int(6))

        val saveJs = List(
          FileName("a") -> a,
          FileName("b") -> b,
          FileName("c") -> c
        ) traverse_ {
          case (n, o) => write.saveThese(loc </> file1(n), Vector(o)).void
        }

        val saveXml = List(
          FileName("d") -> d,
          FileName("e") -> e,
          FileName("f") -> f
        ) traverse_ {
          case (n, o) => write.saveThese(loc </> file1(n), Vector(o)).void
        }

        val lp   = fullCompileExp(sqlE"select (*)[*] from `/onlyfmt`")
        val eval = query.evaluate(lp) translate dropPhases

        val loadAndQueryJs =
          (saveJs.liftM[Process] *> eval).translate(runFsE(js)).runLog

        (runFsE(xml)(saveXml) *> loadAndQueryJs)
          .run.map (_.toEither must beRight(contain(exactly(a, b, c))))
          .unsafePerformSync
      }
    }
  }
}
