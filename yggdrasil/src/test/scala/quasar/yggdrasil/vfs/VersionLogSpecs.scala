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

package quasar.yggdrasil.vfs

import quasar.contrib.pathy.RPath
import quasar.contrib.scalaz.catchable._

import cats.effect.IO

import fs2.{Sink, Stream}

import org.specs2.mutable._

import pathy.Path

import scalaz.syntax.monad._
import iotaz.CopK

import scodec.bits.ByteVector

import shims._

import smock._

import java.util.UUID

object VersionLogSpecs extends Specification {
  import POSIXOp._
  import StreamTestUtils._

  val HWT = Harness[POSIXWithIOCopK, IO]

  "version log manager" should {
    val BaseDir = Path.rootDir </> Path.dir("foo")

    "read and return empty log" in {
      val interp = for {
        _ <- HWT.pattern[Boolean] {
          case CPL(Exists(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("versions.json"))

              true
            }
        }

        _ <- HWT.pattern[Stream[POSIXWithIO, ByteVector]] {
          case CPL(OpenR(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("versions.json"))

              Stream(ByteVector("[]".getBytes))
            }
        }

        _ <- drainIO

        _ <- HWT.pattern[List[RPath]] {
          case CPL(Ls(BaseDir)) => IO.pure(Nil)
        }
      } yield ()

      val result =
        interp(VersionLog.init[POSIXWithIOCopK](BaseDir)).unsafeRunSync

      result mustEqual VersionLog(BaseDir, Nil, Set())
    }

    "return and init empty log when does not exist" in {
      val interp = for {
        _ <- HWT.pattern[Boolean] {
          case CPL(Exists(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("versions.json"))

              false
            }
        }

        _ <- HWT.pattern[Sink[POSIXWithIO, ByteVector]] {
          case CPL(OpenW(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("versions.json.new"))

              assertionSink(_ mustEqual "[]")
            }
        }

        _ <- drainIO

        _ <- HWT.pattern[Unit] {
          case CPL(Move(from, to)) =>
            IO {
              from mustEqual (BaseDir </> Path.file("versions.json.new"))
              to mustEqual (BaseDir </> Path.file("versions.json"))
            }
        }

        _ <- HWT.pattern[List[RPath]] {
          case CPL(Ls(BaseDir)) => IO.pure(Nil)
        }
      } yield ()

      val result =
        interp(VersionLog.init[POSIXWithIOCopK](BaseDir)).unsafeRunSync

      result mustEqual VersionLog(BaseDir, Nil, Set())
    }

    "read and return empty log with uncommitted versions" in {
      val versions = List.fill(2)(UUID.randomUUID()).map(Version)

      val members =
        Path.file("versions.json") :: versions.map(v => Path.dir(v.value.toString))

      val interp = for {
        _ <- HWT.pattern[Boolean] {
          case CPL(Exists(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("versions.json"))

              true
            }
        }

        _ <- HWT.pattern[Stream[POSIXWithIO, ByteVector]] {
          case CPL(OpenR(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("versions.json"))

              Stream(ByteVector("[]".getBytes))
            }
        }

        _ <- drainIO

        _ <- HWT.pattern[List[RPath]] {
          case CPL(Ls(BaseDir)) => IO.pure(members)
        }
      } yield ()

      val result =
        interp(VersionLog.init[POSIXWithIOCopK](BaseDir)).unsafeRunSync

      result mustEqual VersionLog(BaseDir, Nil, versions.toSet)
    }

    "read and return non-empty log with uncommitted versions" in {
      val uncommitted = List.fill(2)(UUID.randomUUID()).map(Version)
      val committed = List.fill(2)(UUID.randomUUID()).map(Version)
      val versions = uncommitted ++ committed

      val members =
        Path.file("versions.json") :: versions.map(v => Path.dir(v.value.toString))

      val interp = for {
        _ <- HWT.pattern[Boolean] {
          case CPL(Exists(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("versions.json"))

              true
            }
        }

        _ <- HWT.pattern[Stream[POSIXWithIO, ByteVector]] {
          case CPL(OpenR(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("versions.json"))

              Stream(ByteVector(s"""["${committed(0).value.toString}","${committed(1).value.toString}"]""".getBytes))
            }
        }

        _ <- drainIO

        _ <- HWT.pattern[List[RPath]] {
          case CPL(Ls(BaseDir)) => IO.pure(members)
        }
      } yield ()

      val result =
        interp(VersionLog.init[POSIXWithIOCopK](BaseDir)).unsafeRunSync

      result mustEqual VersionLog(BaseDir, committed, versions.toSet)
    }

    "generate fresh version from empty version set" in {
      val init = VersionLog(BaseDir, Nil, Set())
      val uuid = UUID.randomUUID()

      val interp = for {
        _ <- HWT.pattern[UUID] {
          case CPL(GenUUID) => IO.pure(uuid)
        }

        _ <- HWT.pattern[Unit] {
          case CPL(MkDir(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.dir(uuid.toString))
            }
        }
      } yield ()

      val (log, version) = interp(VersionLog.fresh[POSIXWithIOCopK].run(init)).unsafeRunSync

      log mustEqual VersionLog(BaseDir, Nil, Set(Version(uuid)))
      version mustEqual Version(uuid)
    }

    "re-generate version upon collision with existing version" in {
      val uuid = UUID.randomUUID()
      val collision = UUID.randomUUID()

      val init = VersionLog(BaseDir, Nil, Set(Version(collision)))

      val interp = for {
        _ <- HWT.pattern[UUID] {
          case CPL(GenUUID) => IO.pure(collision)
        }

        _ <- HWT.pattern[UUID] {
          case CPL(GenUUID) => IO.pure(uuid)
        }

        _ <- HWT.pattern[Unit] {
          case CPL(MkDir(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.dir(uuid.toString))
            }
        }
      } yield ()

      val (log, version) = interp(VersionLog.fresh[POSIXWithIOCopK].run(init)).unsafeRunSync

      log mustEqual VersionLog(BaseDir, Nil, Set(Version(uuid), Version(collision)))
      version mustEqual Version(uuid)
    }

    "commit version to head" in {
      val v = Version(UUID.randomUUID())
      val other = Version(UUID.randomUUID())
      val init = VersionLog(BaseDir, List(other), Set(v, other))

      val interp = for {
        _ <- HWT.pattern[Sink[POSIXWithIO, ByteVector]] {
          case CPL(OpenW(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("versions.json.new"))

              assertionSink(_ mustEqual s"""["${v.value.toString}","${other.value.toString}"]""")
            }
        }

        _ <- drainIO

        _ <- HWT.pattern[Unit] {
          case CPL(Move(from, to)) =>
            IO {
              from mustEqual (BaseDir </> Path.file("versions.json.new"))
              to mustEqual (BaseDir </> Path.file("versions.json"))
            }
        }

        _ <- HWT.pattern[Unit] {
          case CPL(Delete(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.dir("HEAD"))
            }
        }

        _ <- HWT.pattern[Boolean] {
          case CPL(LinkDir(from, to)) =>
            IO {
              from mustEqual (BaseDir </> Path.dir(v.value.toString))
              to mustEqual (BaseDir </> Path.dir("HEAD"))

              true
            }
        }
      } yield ()

      val state =
        interp(VersionLog.commit[POSIXWithIOCopK](v).exec(init)).unsafeRunSync

      state mustEqual VersionLog(BaseDir, v :: other :: Nil, Set(v, other))
    }

    "silently ignore invalid commits" in {
      val v = Version(UUID.randomUUID())
      val other = Version(UUID.randomUUID())
      val init = VersionLog(BaseDir, List(other), Set(other))

      val interp = ().point[Harness[POSIXWithIOCopK, IO, ?]]

      val state =
        interp(VersionLog.commit[POSIXWithIOCopK](v).exec(init)).unsafeRunSync

      state mustEqual init
    }

    "purge old versions beyond the limit" in {
      val front = List.fill(5)(UUID.randomUUID())
      val back = List.fill(2)(UUID.randomUUID())

      val uuids = front ++ back
      val init = VersionLog(BaseDir, uuids.map(Version), uuids.map(Version).toSet)

      val interp = for {
        _ <- HWT.pattern[Unit] {
          case CPL(Delete(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.dir(back(0).toString))
            }
        }

        _ <- HWT.pattern[Unit] {
          case CPL(Delete(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.dir(back(1).toString))
            }
        }
      } yield ()

      val result = interp(VersionLog.purgeOld[POSIXWithIOCopK].exec(init)).unsafeRunSync

      val committed2 = init.committed.take(5)
      result mustEqual VersionLog(BaseDir, committed2, committed2.toSet)
    }
  }

  val CPR = CopK.Inject[IO, POSIXWithIOCopK]

  val CPL = CopK.Inject[POSIXOp, POSIXWithIOCopK]

  val drainIO = HWT.whileDefined[Unit] {
    case CPR(ioa) => ioa
  }
}
