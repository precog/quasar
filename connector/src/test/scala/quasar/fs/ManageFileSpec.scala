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

package quasar.fs

import slamdata.Predef._
import quasar.Data
import quasar.contrib.scalaz.eitherT._
import quasar.contrib.scalaz.stream._

import pathy.Path._
import scalaz._, Scalaz._

class ManageFileSpec extends quasar.Qspec with FileSystemFixture {

  "ManageFile" should {
    "renameFile" >> {
      "moves the existing file to a new name in the same directory" >> prop {
        (s: SingleFileMemState, name: String) => {
          val rename = manage.renameFile(s.file, name)
          val exists = query.fileExistsM(s.file)
          val data: manage.M[Throwable \/ Vector[Data]] =
            read.scanAll(fileParent(s.file) </> file(name)).runLogCatch

          Mem.interpret((rename *> (exists |@| data).tupled).run).eval(s.state)
            .toEither must beRight((false, s.contents.right))
        }
      }
    }
  }
}
