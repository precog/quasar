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

package quasar.fs.mount.module

import slamdata.Predef._
import quasar.Data
import quasar.contrib.pathy.{AFile, APath}
import quasar.effect.KeyValueStore
import quasar.fp.evalNT
import quasar.fp.free._
import quasar.fs.{FileSystemError, PathError, ReadFile, WriteFile}
import quasar.fs.mount.{module, Mounter, Mounting, MountConfig, MountConfigs}
import quasar.sql._

import eu.timepit.refined.auto._
import matryoshka.data.Fix
import monocle.Lens
import monocle.syntax.fields._
import pathy.Path._
import scalaz.{~>, \/, -\/, \/-, Coproduct, Free, Id, State}, Id.Id
import scalaz.syntax.either._
import scalaz.syntax.equal._

class ModuleFileSystemSpec extends quasar.Qspec {
  type S0[A] = Coproduct[ReadFile, Mounting, A]
  type S[A]  = Coproduct[WriteFile, S0, A]
  type M = Map[APath, MountConfig]

  val modDir = rootDir </> dir("foo") </> dir("bar")
  val query = sqlE"select * from quux"
  val fun = Statement.functionDecl[Fix[Sql]](CIName("baz"), Nil, query)
  val mod = MountConfig.moduleConfig(List(fun))
  val mounts: M = Map(modDir -> mod)

  val runRead: ReadFile ~> Id =
    λ[ReadFile ~> Id] {
      case ReadFile.Open(f, _, _) => ReadFile.ReadHandle(f, 0L).right
      case ReadFile.Read(_)       => Vector.empty[Data].right
      case ReadFile.Close(_)      => ()
    }

  val runWrite: WriteFile ~> Id =
    λ[WriteFile ~> Id] {
      case WriteFile.Open(f)     => WriteFile.WriteHandle(f, 0L).right
      case WriteFile.Write(_, _) => Vector.empty[FileSystemError]
      case WriteFile.Close(_)    => ()
    }

  val runS: Free[S, ?] ~> Id = {
    val mt =
      Mounter.trivial[MountConfigs]
        .andThen(foldMapNT(KeyValueStore.impl.toState[State[M, ?]](Lens.id[M])))
        .andThen(evalNT[Id, M](mounts))

    foldMapNT(runWrite :+: runRead :+: mt)
  }

  def openR(f: AFile): Free[S, FileSystemError \/ ReadFile.ReadHandle] =
    ReadFile.Unsafe[ReadFile]
      .open(f, 0L, None)
      .run
      .flatMapSuspension(module.readFile[S])

  def openW(f: AFile): Free[S, FileSystemError \/ WriteFile.WriteHandle] =
    WriteFile.Unsafe[WriteFile]
      .open(f)
      .run
      .flatMapSuspension(module.writeFile[S])

  val invalidPath =
    FileSystemError.pathErr composePrism
    PathError.invalidPath   composeLens
    _1

  "module filesystem" should {
    "readfile" >> {
      "fails when file refers to a module function" >> {
        val baz = rootDir </> dir("foo") </> dir("bar") </> file("baz")
        runS(openR(baz)) must beLike {
          case -\/(err) => invalidPath.exist(_ ≟ baz)(err) must beTrue
        }
      }

      "fails when file refers to a path within a module" >> {
        val quux = rootDir </> dir("foo") </> dir("bar") </> dir("baat") </> file("quux")
        runS(openR(quux)) must beLike {
          case -\/(err) => invalidPath.exist(_ ≟ quux)(err) must beTrue
        }
      }

      "succeeds when file not in a module" >> {
        val tps = rootDir </> dir("foo") </> dir("reports") </> file("tps")
        runS(openR(tps)) must beLike {
          case \/-(ReadFile.ReadHandle(f, _)) => f must equal(tps)
        }
      }
    }

    "writefile" >> {
      "fails when attempt to write to a module function" >> {
        val baz = rootDir </> dir("foo") </> dir("bar") </> file("baz")
        runS(openW(baz)) must beLike {
          case -\/(err) => invalidPath.exist(_ ≟ baz)(err) must beTrue
        }
      }

      "fails when attempt to write within a module" >> {
        val quux = rootDir </> dir("foo") </> dir("bar") </> dir("baat") </> file("quux")
        runS(openW(quux)) must beLike {
          case -\/(err) => invalidPath.exist(_ ≟ quux)(err) must beTrue
        }
      }

      "succeeds when file not in a module" >> {
        val tps = rootDir </> dir("foo") </> dir("reports") </> file("tps")
        runS(openW(tps)) must beLike {
          case \/-(WriteFile.WriteHandle(f, _)) => f must equal(tps)
        }
      }
    }
  }
}
