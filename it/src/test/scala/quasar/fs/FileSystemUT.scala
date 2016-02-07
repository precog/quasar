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

import quasar.BackendName

import scalaz._
import scalaz.concurrent.Task

/** FileSystem Under Test
  *
  * @param name the name of the filesystem
  * @param run an interpreter of the filesystem into the `Task` monad
  * @param testDir a directory in the filesystem tests may use for temp data
  */
final case class FileSystemUT[S[_]](
  name: BackendName,
  run: S ~> Task,
  testDir: ADir
) {
  def contramap[T[_]](f: T ~> S): FileSystemUT[T] =
    FileSystemUT(name, run compose f, testDir)
}
