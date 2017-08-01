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

import quasar.{BackendCapability, TestConfig}
import quasar.fp.free.foldMapNT
import quasar.fp.ski.κ
import quasar.fs._
import quasar.physical.marklogic.testing
import quasar.sql.CompilerHelpers

import eu.timepit.refined.auto._
import org.specs2.specification.core.{Fragment, Fragments}
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

abstract class MultiFormatFileSystemTest extends quasar.Qspec with CompilerHelpers {
  def multiFormatFileSystemShould(
    js:  BackendEffect ~> Task,
    xml: BackendEffect ~> Task
  ): Fragment

  type Fs[A]  = Free[BackendEffect, A]
  type FsE[A] = FileSystemErrT[Fs, A]

  val manage = ManageFile.Ops[BackendEffect]
  val query  = QueryFile.Ops[BackendEffect]
  val read   = ReadFile.Ops[BackendEffect]
  val write  = WriteFile.Ops[BackendEffect]

  val dropPhases = λ[query.transforms.ExecM ~> FsE](e => EitherT(e.run.value))

  def runFs(fs: BackendEffect ~> Task): Fs ~> Task =
    foldMapNT(fs)

  def runFsE(fs: BackendEffect ~> Task): FsE ~> FileSystemErrT[Task, ?] =
    Hoist[FileSystemErrT].hoist(runFs(fs))

  TestConfig.fileSystemConfigs(FsType).flatMap (cfgs =>
    cfgs filter { case (ref, _, _) =>
      ref.supports(BackendCapability.write()) &&
      ref.supports(BackendCapability.query())
    } take 1 traverse {
      case (_, uri, _) =>
        (testing.multiFormatDef(uri, 10000L, 10000L) |@| testsRoot) { (mfd, root) =>
          val (js, xml, close) = mfd
          val chrootJs  = foldMapNT(js) compose chroot.backendEffect[BackendEffect](root)
          val chrootXml = foldMapNT(xml) compose chroot.backendEffect[BackendEffect](root)
          val cleanup   = {
            val delRoot = manage.delete(root).run.void
            (delRoot foldMap js) *> (delRoot foldMap xml)
          }

          "Multi-format MarkLogic FileSystem" >> Fragments(
            multiFormatFileSystemShould(chrootJs, chrootXml),
            step((cleanup onFinish κ(close)).unsafePerformSync))
        }
    }).unsafePerformSync

  ////

  private def testsRoot =
    TestConfig.testDataPrefix map (_ </> dir("ml-multi-format"))
}
