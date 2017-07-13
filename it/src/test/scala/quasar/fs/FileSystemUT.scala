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
import quasar.{BackendCapability, BackendRef}
import quasar.contrib.pathy._
import quasar.fp.free, free._

import scalaz._
import scalaz.concurrent.Task


final case class SupportedFs[S[_]](
  ref: BackendRef,
  impl: Option[FileSystemUT[S]],
  implNonChrooted: Option[FileSystemUT[S]] = None
) {
  def liftIO: SupportedFs[Coproduct[Task, S, ?]] =
    this.copy(impl = impl.map(_.liftIO), implNonChrooted = implNonChrooted.map(_.liftIO))
}

/** FileSystem Under Test
  *
  * @param ref description of the filesystem
  * @param testInterp an interpreter of the filesystem into the `Task` monad
  * @param setupInterp a second interpreter which has the ability to insert
  *   and otherwise write to the filesystem, even if `testInterp` does not
  * @param testDir a directory in the filesystem tests may use for temp data
  * @param close an effect to clean up any resources created when the
  *   interpreters are used. Need not be called if no interpreted effect was
  *   ever run, but it's safe to call it either way.
  */
final case class FileSystemUT[S[_]](
  ref:         BackendRef,
  testInterp:  S ~> Task,
  setupInterp: S ~> Task,
  testDir:     ADir,
  close:       Task[Unit]
) {

  type F[A] = Free[S, A]

  def supports(bc: BackendCapability): Boolean =
    ref supports bc

  def liftIO: FileSystemUT[Coproduct[Task, S, ?]] =
    FileSystemUT(
      ref,
      NaturalTransformation.refl[Task] :+: testInterp,
      NaturalTransformation.refl[Task] :+: setupInterp,
      testDir,
      close)

  def contramap[T[_]](f: T ~> S): FileSystemUT[T] =
    FileSystemUT(ref, testInterp compose f, setupInterp compose f, testDir, close)

  def contramapF[T[_]](f: T ~> Free[S, ?]): FileSystemUT[T] =
    FileSystemUT(
      ref,
      foldMapNT(testInterp) compose f,
      foldMapNT(setupInterp) compose f,
      testDir,
      close)

  val testInterpM: F ~> Task = foldMapNT(testInterp)
  val setupInterpM: F ~> Task = foldMapNT(setupInterp)
}
