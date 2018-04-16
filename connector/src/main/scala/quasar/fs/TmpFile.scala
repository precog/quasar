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

import quasar.contrib.pathy._

import pathy.Path._
import scalaz._, Scalaz._

object TmpFile {

  def tmpFileName[A: Show](prefix: TempFilePrefix, a: A): FileName =
    FileName(prefix.s + a.shows)

  def tmpFile[A: Show](dir: ADir, prefix: TempFilePrefix, a: A): AFile =
    dir </> file1(tmpFileName(prefix, a))

  def tmpFile0[A: Show](near: APath, prefix: TempFilePrefix, a: A): AFile =
    tmpFile(nearDir(near), prefix, a)
}
