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

import quasar.contrib.pathy.{ADir, RPath}
import quasar.contrib.scalaz.catchable._

import cats.effect.IO

import fs2.{Stream, Sink}

import org.specs2.mutable._
import org.specs2.matcher.DisjunctionMatchers

import pathy.Path

import iotaz.CopK

import scalaz.Need
import scalaz.syntax.monad._
import scalaz.syntax.std.either._

import shims._

import scodec.Codec
import scodec.bits.ByteVector

import smock._

import java.util.UUID

object FreeVFSSpecs extends Specification with DisjunctionMatchers {
  import POSIXOp._
  import StreamTestUtils._

  type S[A] = POSIXWithIOCopK[A]

  val H = Harness[S, IO]

  val BaseDir = Path.rootDir </> Path.dir("foo")

  val currentVFSVersionBV =
    Codec.encode(FreeVFS.currentVFSVersion)
      .fold(
        e => IO.raiseError(new RuntimeException(e.message)),
        r => IO.pure(r.toByteVector))
      .unsafeRunSync

  val currentMetaVersionBV =
    Codec.encode(FreeVFS.currentMetaVersion)
      .fold(
        e => IO.raiseError(new RuntimeException(e.message)),
        r => IO.pure(r.toByteVector))
      .unsafeRunSync

  "vfs layer" should {
    "initialize from an empty state" in {
      val interp = for {
        _ <- H.pattern[Boolean] {
          case CPL(Exists(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("VFSVERSION"))

              false
            }
        }

        _ <- H.pattern[Sink[POSIXWithIO, ByteVector]] {
          case CPL(OpenW(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("VFSVERSION"))
              assertionSinkBV(_ mustEqual currentMetaVersionBV)
            }
        }

        _ <- drainIO

        _ <- H.pattern[Boolean] {
          case CPL(Exists(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.dir("META"))

              false
            }
        }

        _ <- H.pattern[Unit] {
          case CPL(MkDir(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.dir("META"))
            }
        }

        _ <- vlogInit(BaseDir </> Path.dir("META"), None)

        _ <- persistMeta(
          BaseDir </> Path.dir("META"),
          _ mustEqual "{}",
          _ mustEqual "{}")

        _ <- H.pattern[List[RPath]] {
          case CPL(Ls(target)) =>
            IO {
              target mustEqual BaseDir

              List(Path.dir("META"))
            }
        }
      } yield ()

      val vfs = interp(FreeVFS.init[S](BaseDir)).unsafeRunSync

      vfs must beLike {
        case VFS(BaseDir, VersionLog(vlogBase, committed, versions), paths, index, vlogs, blobs) =>
          paths must beEmpty
          index must beEmpty
          vlogs must beEmpty
          blobs must beEmpty

          vlogBase mustEqual (BaseDir </> Path.dir("META"))
          committed must haveSize(1)
          versions mustEqual committed.toSet
      }
    }

    "initialize from an empty state with pre-existing expected VFSVERSION" in {
      val interp = for {
        _ <- H.pattern[Boolean] {
          case CPL(Exists(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("VFSVERSION"))

              true
            }
        }

        _ <- H.pattern[Stream[POSIXWithIO, ByteVector]] {
          case CPL(OpenR(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("VFSVERSION"))

              Stream.emit(currentVFSVersionBV)
            }
        }

        _ <- H.pattern[Unit] {
          case CPR(io) => io
        }.replicateM(4)

        _ <- H.pattern[FreeVFS.VFSVersion] {
          case CPR(ta) =>
            ta.map { a =>
              a mustEqual FreeVFS.currentVFSVersion

              a
            }
        }

        _ <- H.pattern[Boolean] {
          case CPL(Exists(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.dir("META"))

              true
            }
        }

        _ <- vlogInit(BaseDir </> Path.dir("META"), None)

        _ <- persistMeta(
          BaseDir </> Path.dir("META"),
          _ mustEqual "{}",
          _ mustEqual "{}")

        _ <- H.pattern[List[RPath]] {
          case CPL(Ls(target)) =>
            IO {
              target mustEqual BaseDir

              List(Path.dir("META"))
            }
        }
      } yield ()

      val vfs = interp(FreeVFS.init[S](BaseDir)).unsafeRunSync

      vfs must beLike {
        case VFS(BaseDir, VersionLog(vlogBase, committed, versions), paths, index, vlogs, blobs) =>
          paths must beEmpty
          index must beEmpty
          vlogs must beEmpty
          blobs must beEmpty

          vlogBase mustEqual (BaseDir </> Path.dir("META"))
          committed must haveSize(1)
          versions mustEqual committed.toSet
      }
    }

    "initialize from an empty state with pre-existing unexpected VFSVERSION" in {
      val interp = for {
        _ <- H.pattern[Boolean] {
          case CPL(Exists(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("VFSVERSION"))

              true
            }
        }

        _ <- H.pattern[Stream[POSIXWithIO, ByteVector]] {
          case CPL(OpenR(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("VFSVERSION"))

              Stream.emit(ByteVector.fromInt(16384))
            }
        }

        _ <- drainIO

        _ <- H.pattern[Boolean] {
          case CPL(Exists(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.dir("META"))

              true
            }
        }

        _ <- vlogInit(BaseDir </> Path.dir("META"), None)

        _ <- persistMeta(
          BaseDir </> Path.dir("META"),
          _ mustEqual "{}",
          _ mustEqual "{}")

        _ <- H.pattern[List[RPath]] {
          case CPL(Ls(target)) =>
            IO {
              target mustEqual BaseDir

              List(Path.dir("META"))
            }
        }
      } yield ()

      val vfs = interp(FreeVFS.init[S](BaseDir)).attempt.unsafeRunSync

      vfs.disjunction.leftMap(_.getMessage) must be_-\/("Unexpected VERSION, 0100000000000000")
    }

    "initialize from an empty state with pre-existing directory" in {
      val interp = for {
        _ <- H.pattern[Boolean] {
          case CPL(Exists(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("VFSVERSION"))

              false
            }
        }

        _ <- H.pattern[Sink[POSIXWithIO, ByteVector]] {
          case CPL(OpenW(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("VFSVERSION"))

              assertionSinkBV(_ mustEqual currentVFSVersionBV)
            }
        }

        _ <- drainIO

        _ <- H.pattern[Boolean] {
          case CPL(Exists(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.dir("META"))

              true
            }
        }

        _ <- vlogInit(BaseDir </> Path.dir("META"), None)

        _ <- persistMeta(
          BaseDir </> Path.dir("META"),
          _ mustEqual "{}",
          _ mustEqual "{}")

        _ <- H.pattern[List[RPath]] {
          case CPL(Ls(target)) =>
            IO {
              target mustEqual BaseDir

              List(Path.dir("META"))
            }
        }
      } yield ()

      val vfs = interp(FreeVFS.init[S](BaseDir)).unsafeRunSync

      vfs must beLike {
        case VFS(BaseDir, VersionLog(vlogBase, committed, versions), paths, index, vlogs, blobs) =>
          paths must beEmpty
          index must beEmpty
          vlogs must beEmpty
          blobs must beEmpty

          vlogBase mustEqual (BaseDir </> Path.dir("META"))
          committed must haveSize(1)
          versions mustEqual committed.toSet
      }
    }

    "initialize from a non-empty state without pre-existing paths or blobs" in {
      val version = Version(UUID.randomUUID())

      val interp = for {
        _ <- H.pattern[Boolean] {
          case CPL(Exists(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("VFSVERSION"))

              false
            }
        }

        _ <- H.pattern[Sink[POSIXWithIO, ByteVector]] {
          case CPL(OpenW(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("VFSVERSION"))

              (_ => Stream.empty)
            }
        }

        _ <- drainIO

        _ <- H.pattern[Boolean] {
          case CPL(Exists(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.dir("META"))

              true
            }
        }

        _ <- vlogInit(BaseDir </> Path.dir("META"), Some(List(version)))

        headDir = BaseDir </> Path.dir("META") </> Path.dir(version.value.toString)

        _ <- H.pattern[Stream[POSIXWithIO, ByteVector]] {
          case CPL(OpenR(target)) =>
            IO {
              target mustEqual (headDir </> Path.file("paths.json"))

              Stream(ByteVector("{}".getBytes))
            }
        }

        _ <- drainIO

        _ <- H.pattern[Stream[POSIXWithIO, ByteVector]] {
          case CPL(OpenR(target)) =>
            IO {
              target mustEqual (headDir </> Path.file("index.json"))

              Stream(ByteVector("{}".getBytes))
            }
        }

        _ <- drainIO

        _ <- H.pattern[List[RPath]] {
          case CPL(Ls(target)) =>
            IO {
              target mustEqual BaseDir

              List(Path.dir("META"))
            }
        }
      } yield ()

      val vfs = interp(FreeVFS.init[S](BaseDir)).unsafeRunSync

      vfs must beLike {
        case VFS(BaseDir, VersionLog(vlogBase, committed, versions), paths, index, vlogs, blobs) =>
          paths must beEmpty
          index must beEmpty
          vlogs must beEmpty
          blobs must beEmpty

          vlogBase mustEqual (BaseDir </> Path.dir("META"))
          committed must haveSize(1)
          versions mustEqual committed.toSet
      }
    }

    "initialize with pre-existing paths" in {
      val version = Version(UUID.randomUUID())
      val blob = Blob(UUID.randomUUID())

      val interp = for {
        _ <- H.pattern[Boolean] {
          case CPL(Exists(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("VFSVERSION"))

              false
            }
        }

        _ <- H.pattern[Sink[POSIXWithIO, ByteVector]] {
          case CPL(OpenW(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("VFSVERSION"))

              (_ => Stream.empty)
            }
        }

        _ <- drainIO

        _ <- H.pattern[Boolean] {
          case CPL(Exists(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.dir("META"))

              true
            }
        }

        _ <- vlogInit(BaseDir </> Path.dir("META"), Some(List(version)))

        headDir = BaseDir </> Path.dir("META") </> Path.dir(version.value.toString)

        _ <- H.pattern[Stream[POSIXWithIO, ByteVector]] {
          case CPL(OpenR(target)) =>
            IO {
              target mustEqual (headDir </> Path.file("paths.json"))

              Stream(ByteVector(s"""{"/foo/bar":"${blob.value}"}""".getBytes))
            }
        }

        _ <- drainIO

        _ <- H.pattern[Stream[POSIXWithIO, ByteVector]] {
          case CPL(OpenR(target)) =>
            IO {
              target mustEqual (headDir </> Path.file("index.json"))

              Stream(ByteVector("""{"/foo/":["./bar"],"/":["./foo/"]}""".getBytes))
            }
        }

        _ <- drainIO

        _ <- H.pattern[List[RPath]] {
          case CPL(Ls(target)) =>
            IO {
              target mustEqual BaseDir

              List(Path.dir("META"), Path.dir(blob.value.toString))
            }
        }
      } yield ()

      val vfs = interp(FreeVFS.init[S](BaseDir)).unsafeRunSync

      vfs must beLike {
        case VFS(BaseDir, VersionLog(vlogBase, committed, versions), paths, index, vlogs, blobs) =>
          val foobar = Path.rootDir </> Path.dir("foo") </> Path.file("bar")

          paths must haveSize(1)
          paths must haveKey(foobar)
          paths(foobar) mustEqual blob

          index must haveSize(2)

          index must haveKey(Path.rootDir)
          index must haveKey(Path.rootDir </> Path.dir("foo"))

          index(Path.rootDir) mustEqual Vector(Path.dir("foo"))
          index(Path.rootDir </> Path.dir("foo")) mustEqual Vector(Path.file("bar"))

          vlogs must beEmpty

          blobs mustEqual Set(blob)

          vlogBase mustEqual (BaseDir </> Path.dir("META"))
          committed must haveSize(1)
          versions mustEqual committed.toSet
      }
    }

    "initialize with pre-existing paths and extra blobs" in {
      val version = Version(UUID.randomUUID())
      val blob = Blob(UUID.randomUUID())
      val extra = Blob(UUID.randomUUID())

      val interp = for {
        _ <- H.pattern[Boolean] {
          case CPL(Exists(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("VFSVERSION"))

              false
            }
        }

        _ <- H.pattern[Sink[POSIXWithIO, ByteVector]] {
          case CPL(OpenW(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.file("VFSVERSION"))

              (_ => Stream.empty)
            }
        }

        _ <- drainIO

        _ <- H.pattern[Boolean] {
          case CPL(Exists(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.dir("META"))

              true
            }
        }

        _ <- vlogInit(BaseDir </> Path.dir("META"), Some(List(version)))

        headDir = BaseDir </> Path.dir("META") </> Path.dir(version.value.toString)

        _ <- H.pattern[Stream[POSIXWithIO, ByteVector]] {
          case CPL(OpenR(target)) =>
            IO {
              target mustEqual (headDir </> Path.file("paths.json"))

              Stream(ByteVector(s"""{"/foo/bar":"${blob.value}"}""".getBytes))
            }
        }

        _ <- drainIO

        _ <- H.pattern[Stream[POSIXWithIO, ByteVector]] {
          case CPL(OpenR(target)) =>
            IO {
              target mustEqual (headDir </> Path.file("index.json"))

              Stream(ByteVector("""{"/foo/":["./bar"],"/":["./foo/"]}""".getBytes))
            }
        }

        _ <- drainIO

        _ <- H.pattern[List[RPath]] {
          case CPL(Ls(target)) =>
            IO {
              target mustEqual BaseDir

              List(Path.dir("META"), Path.dir(blob.value.toString), Path.dir(extra.value.toString))
            }
        }
      } yield ()

      val vfs = interp(FreeVFS.init[S](BaseDir)).unsafeRunSync

      vfs must beLike {
        case VFS(BaseDir, VersionLog(vlogBase, committed, versions), paths, index, vlogs, blobs) =>
          val foobar = Path.rootDir </> Path.dir("foo") </> Path.file("bar")

          paths must haveSize(1)
          paths must haveKey(foobar)
          paths(foobar) mustEqual blob

          index must haveSize(2)

          index must haveKey(Path.rootDir)
          index must haveKey(Path.rootDir </> Path.dir("foo"))

          index(Path.rootDir) mustEqual Vector(Path.dir("foo"))
          index(Path.rootDir </> Path.dir("foo")) mustEqual Vector(Path.file("bar"))

          vlogs must beEmpty

          blobs mustEqual Set(blob, extra)

          vlogBase mustEqual (BaseDir </> Path.dir("META"))
          committed must haveSize(1)
          versions mustEqual committed.toSet
      }
    }

    val BlankVFS =
      VFS(
        BaseDir,
        VersionLog(
          BaseDir </> Path.dir("META"),
          Nil,
          Set()),
        Map(),
        Map(),
        Map(),
        Set())

    "create scratch blob" in {
      val blob = Blob(UUID.randomUUID())

      val interp = for {
        _ <- H.pattern[UUID] {
          case CPL(GenUUID) => IO.pure(blob.value)
        }

        _ <- H.pattern[Unit] {
          case CPL(MkDir(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.dir(blob.value.toString))
            }
        }

        _ <- vlogInit(BaseDir </> Path.dir(blob.value.toString), None)
      } yield ()

      val result = interp(FreeVFS.scratch[S].eval(BlankVFS)).unsafeRunSync

      result mustEqual blob
    }

    "create scratch blob after gen conflict" in {
      val conflict = Blob(UUID.randomUUID())
      val blob = Blob(UUID.randomUUID())

      val interp = for {
        _ <- H.pattern[UUID] {
          case CPL(GenUUID) => IO.pure(conflict.value)
        }

        _ <- H.pattern[UUID] {
          case CPL(GenUUID) => IO.pure(blob.value)
        }

        _ <- H.pattern[Unit] {
          case CPL(MkDir(target)) =>
            IO {
              target mustEqual (BaseDir </> Path.dir(blob.value.toString))
            }
        }

        _ <- vlogInit(BaseDir </> Path.dir(blob.value.toString), None)
      } yield ()

      val result = interp(FreeVFS.scratch[S].eval(BlankVFS.copy(blobs = Set(conflict)))).unsafeRunSync

      result mustEqual blob
    }

    "exists fails with blank VFS" in {
      FreeVFS.exists[Need](Path.rootDir </> Path.file("foo")).eval(BlankVFS).value mustEqual false
    }

    "exists finds file in VFS" in {
      val target = Path.rootDir </> Path.file("foo")
      val vfs = BlankVFS.copy(paths = Map(target -> Blob(UUID.randomUUID())))
      FreeVFS.exists[Need](target).eval(vfs).value mustEqual true
    }

    "ls produces empty in a blank VFS" in {
      FreeVFS.ls[Need](Path.rootDir).eval(BlankVFS).value mustEqual Nil
    }

    "ls finds children in index" in {
      val target = Path.rootDir
      val children = Vector(Path.file("foo"), Path.dir("bar"))
      val vfs = BlankVFS.copy(index = Map(target -> children))

      FreeVFS.ls[Need](Path.rootDir).eval(vfs).value mustEqual children.toList
    }

    "link fails with non-existent blob" in {
      val blob = Blob(UUID.randomUUID())
      val target = Path.rootDir </> Path.file("foo")

      val interp = ().point[Harness[S, IO, ?]]

      val result = interp(FreeVFS.link[S](blob, target).eval(BlankVFS)).unsafeRunSync

      result mustEqual false
    }

    "link fails with duplicated target" in {
      val blob = Blob(UUID.randomUUID())
      val target = Path.rootDir </> Path.file("foo")

      val vfs =
        BlankVFS.copy(
          paths = Map(target -> blob),
          blobs = Set(blob))

      val interp = ().point[Harness[S, IO, ?]]

      val result = interp(FreeVFS.link[S](blob, target).eval(vfs)).unsafeRunSync

      result mustEqual false
    }

    "link updates paths and index" in {
      val blob = Blob(UUID.randomUUID())
      val orig = Path.rootDir </> Path.file("foo")
      val target = Path.rootDir </> Path.file("bar")

      val blobJson = s""""${blob.value}""""

      val vfs =
        BlankVFS.copy(
          paths = Map(orig -> blob),
          index = Map(Path.rootDir -> Vector(Path.file("foo"))),
          blobs = Set(blob))

      val interp =
        persistMeta(
          BaseDir </> Path.dir("META"),
          { pathsJson =>
            (pathsJson mustEqual s"""{"/foo":$blobJson,"/bar":$blobJson}""") or
              (pathsJson mustEqual s"""{"/bar":$blobJson,"/foo":$blobJson}""")
          },
          { indexJson =>
            (indexJson mustEqual """{"/":["./foo","./bar"]}""") or
              (indexJson mustEqual """{"/":["./bar","./foo"]}""")
          })

      val (vfs2, result) = interp(FreeVFS.link[S](blob, target).apply(vfs)).unsafeRunSync

      result mustEqual true

      vfs2.paths must haveKey(orig)
      vfs2.paths(orig) mustEqual blob
      vfs2.paths must haveKey(target)
      vfs2.paths(target) mustEqual blob

      (vfs2.index(Path.rootDir) mustEqual Vector(Path.file("foo"), Path.file("bar"))) or
        (vfs2.index(Path.rootDir) mustEqual Vector(Path.file("bar"), Path.file("foo")))
    }

    "moveFile fails with non-existent source" in {
      val from = Path.rootDir </> Path.file("foo")
      val to = Path.rootDir </> Path.file("bar")

      val interp = ().point[Harness[S, IO, ?]]

      val result =
        interp(FreeVFS.moveFile[S](from, to, MoveSemantics.FailIfExists).eval(BlankVFS)).unsafeRunSync

      result mustEqual false
    }

    "moveFile fails with extant target (FailIfExists)" in {
      val blob = Blob(UUID.randomUUID())
      val from = Path.rootDir </> Path.file("foo")
      val to = Path.rootDir </> Path.file("bar")

      val vfs = BlankVFS.copy(paths = Map(from -> Blob(UUID.randomUUID()), to -> blob))

      val interp = ().point[Harness[S, IO, ?]]

      val result =
        interp(FreeVFS.moveFile[S](from, to, MoveSemantics.FailIfExists).eval(vfs)).unsafeRunSync

      result mustEqual false
    }

    "moveFile fails with non-existent target (FailIfMissing)" in {
      val blob = Blob(UUID.randomUUID())
      val from = Path.rootDir </> Path.file("foo")
      val to = Path.rootDir </> Path.file("bar")

      val vfs = BlankVFS.copy(paths = Map(from -> blob))

      val interp = ().point[Harness[S, IO, ?]]

      val result =
        interp(FreeVFS.moveFile[S](from, to, MoveSemantics.FailIfMissing).eval(vfs)).unsafeRunSync

      result mustEqual false
    }

    "moveFile silently overwrites with extant target (Overwrite)" in {
      val blob = Blob(UUID.randomUUID())
      val from = Path.rootDir </> Path.file("foo")
      val to = Path.rootDir </> Path.file("bar")

      val blobJson = s""""${blob.value}""""

      val vfs =
        BlankVFS.copy(
          paths = Map(from -> blob, to -> Blob(UUID.randomUUID())),
          index = Map(Path.rootDir -> Vector(Path.file("foo"), Path.file("bar"))))

      val interp =
        persistMeta(
          BaseDir </> Path.dir("META"),
          _ mustEqual s"""{"/bar":$blobJson}""",
          _ mustEqual """{"/":["./bar"]}""")

      val (vfs2, result) =
        interp(FreeVFS.moveFile[S](from, to, MoveSemantics.Overwrite).apply(vfs)).unsafeRunSync

      result mustEqual true

      vfs2.paths must haveKey(to)
      vfs2.paths(to) mustEqual blob
      vfs2.paths must not(haveKey(from))

      vfs2.index(Path.rootDir) mustEqual Vector(Path.file("bar"))
    }

    "moveFile updates paths and index" in {
      val blob = Blob(UUID.randomUUID())
      val from = Path.rootDir </> Path.file("foo")
      val to = Path.rootDir </> Path.file("bar")

      val blobJson = s""""${blob.value}""""

      val vfs =
        BlankVFS.copy(
          paths = Map(from -> blob),
          index = Map(Path.rootDir -> Vector(Path.file("foo"))))

      val interp =
        persistMeta(
          BaseDir </> Path.dir("META"),
          _ mustEqual s"""{"/bar":$blobJson}""",
          _ mustEqual """{"/":["./bar"]}""")

      val (vfs2, result) =
        interp(FreeVFS.moveFile[S](from, to, MoveSemantics.FailIfExists).apply(vfs)).unsafeRunSync

      result mustEqual true

      vfs2.paths must haveKey(to)
      vfs2.paths(to) mustEqual blob
      vfs2.paths must not(haveKey(from))

      vfs2.index(Path.rootDir) mustEqual Vector(Path.file("bar"))
    }

    "moveDir fails with a non-existent source" in {
      val source = Path.rootDir </> Path.dir("source")

      val target = Path.rootDir </> Path.dir("target")

      val interp = ().point[Harness[S, IO, ?]]

      val result =
        interp(FreeVFS.moveDir[S](source, target, MoveSemantics.FailIfExists).eval(BlankVFS)).unsafeRunSync

      result mustEqual false
    }

    "moveDir fails with an extant target (FailIfExisting)" in {
      val blob = Blob(UUID.randomUUID())
      val blob2 = Blob(UUID.randomUUID())

      val source = Path.rootDir </> Path.dir("source")
      val from = source </> Path.file("foo")

      val target = Path.rootDir </> Path.dir("target")

      val vfs =
        BlankVFS.copy(
          paths = Map(from -> blob, (target </> Path.file("bar")) -> blob2),
          index = Map(
            Path.rootDir -> Vector(Path.dir("source"), Path.dir("target")),
            source -> Vector(Path.file("foo")),
            target -> Vector(Path.file("bar"))))

      val interp = ().point[Harness[S, IO, ?]]

      val result =
        interp(FreeVFS.moveDir[S](source, target, MoveSemantics.FailIfExists).eval(vfs)).unsafeRunSync

      result mustEqual false
    }

    "moveDir fails with an non-existing target (FailIfMissing)" in {
      val blob = Blob(UUID.randomUUID())

      val source = Path.rootDir </> Path.dir("source")
      val from = source </> Path.file("foo")

      val target = Path.rootDir </> Path.dir("target")

      val vfs =
        BlankVFS.copy(
          paths = Map(from -> blob),
          index = Map(
            Path.rootDir -> Vector(Path.dir("source")),
            source -> Vector(Path.file("foo"))))

      val interp = ().point[Harness[S, IO, ?]]

      val result =
        interp(FreeVFS.moveDir[S](source, target, MoveSemantics.FailIfMissing).eval(vfs)).unsafeRunSync

      result mustEqual false
    }

    "moveDir silently overwrites (without merge!) pre-existing target (Overwrite)" in {
      val blob = Blob(UUID.randomUUID())
      val blob2 = Blob(UUID.randomUUID())

      val source = Path.rootDir </> Path.dir("source")
      val from = source </> Path.file("foo")

      val target = Path.rootDir </> Path.dir("target")
      val to = target </> Path.file("foo")

      val blobJson = s""""${blob.value}""""

      val vfs =
        BlankVFS.copy(
          paths = Map(from -> blob, (target </> Path.file("bar")) -> blob2),
          index = Map(
            Path.rootDir -> Vector(Path.dir("source"), Path.dir("target")),
            source -> Vector(Path.file("foo")),
            target -> Vector(Path.file("bar"))))

      val interp =
        persistMeta(
          BaseDir </> Path.dir("META"),
          _ mustEqual s"""{"/target/foo":$blobJson}""",
          { json =>
            (json mustEqual """{"/":["./target/"],"/target/":["./foo"]}""") or
              (json mustEqual """{"/target/":["./foo"],"/":["./target/"]}""")
          })

      val (vfs2, result) =
        interp(FreeVFS.moveDir[S](source, target, MoveSemantics.Overwrite).apply(vfs)).unsafeRunSync

      result mustEqual true

      vfs2.paths must haveKey(to)
      vfs2.paths(to) mustEqual blob
      vfs2.paths must not(haveKey(from))

      vfs2.index(target) mustEqual Vector(Path.file("foo"))   // no bar!
    }

    "moveDir moves a directory with a single element" in {
      val blob = Blob(UUID.randomUUID())

      val source = Path.rootDir </> Path.dir("source")
      val from = source </> Path.file("foo")

      val target = Path.rootDir </> Path.dir("target")
      val to = target </> Path.file("foo")

      val blobJson = s""""${blob.value}""""

      val vfs =
        BlankVFS.copy(
          paths = Map(from -> blob),
          index = Map(
            Path.rootDir -> Vector(Path.dir("source")),
            source -> Vector(Path.file("foo"))))

      val interp =
        persistMeta(
          BaseDir </> Path.dir("META"),
          _ mustEqual s"""{"/target/foo":$blobJson}""",
          { json =>
            (json mustEqual """{"/":["./target/"],"/target/":["./foo"]}""") or
              (json mustEqual """{"/target/":["./foo"],"/":["./target/"]}""")
          })

      val (vfs2, result) =
        interp(FreeVFS.moveDir[S](source, target, MoveSemantics.FailIfExists).apply(vfs)).unsafeRunSync

      result mustEqual true

      vfs2.paths must haveKey(to)
      vfs2.paths(to) mustEqual blob
      vfs2.paths must not(haveKey(from))

      vfs2.index(target) mustEqual Vector(Path.file("foo"))
    }

    "delete fails with non-existent target" in {
      val target = Path.rootDir </> Path.file("foo")

      val interp = ().point[Harness[S, IO, ?]]

      val result = interp(FreeVFS.delete[S](target).eval(BlankVFS)).unsafeRunSync

      result mustEqual false
    }

    "delete updates paths and index for file" in {
      val blob = Blob(UUID.randomUUID())
      val target = Path.rootDir </> Path.file("foo")

      val vfs =
        BlankVFS.copy(
          paths = Map(target -> blob),
          index = Map(Path.rootDir -> Vector(Path.file("foo"))),
          blobs = Set(blob))

      val interp =
        persistMeta(
          BaseDir </> Path.dir("META"),
          _ mustEqual "{}",
          _ mustEqual "{}")

      val (vfs2, result) = interp(FreeVFS.delete[S](target).apply(vfs)).unsafeRunSync

      result mustEqual true

      vfs2.paths must beEmpty
      vfs2.index must beEmpty
      vfs2.blobs mustEqual Set(blob)
    }

    "delete updates paths and index for directory" in {
      val blob = Blob(UUID.randomUUID())

      val target = Path.rootDir </> Path.dir("target")
      val foo = target </> Path.file("foo")

      val vfs =
        BlankVFS.copy(
          paths = Map(foo -> blob),
          index = Map(
            Path.rootDir -> Vector(Path.dir("target")),
            target -> Vector(Path.file("foo"))),
          blobs = Set(blob))

      val interp =
        persistMeta(
          BaseDir </> Path.dir("META"),
          _ mustEqual "{}",
          _ mustEqual "{}")

      val (vfs2, result) =
        interp(FreeVFS.delete[S](target).apply(vfs)).unsafeRunSync

      result mustEqual true

      vfs2.paths must beEmpty
      vfs2.index must beEmpty
      vfs2.blobs mustEqual Set(blob)
    }

    "readPath produces None for non-existent path" in {
      FreeVFS.readPath[Need](Path.rootDir </> Path.file("foo")).eval(BlankVFS).value mustEqual None
    }

    "readPath finds blob for extant path" in {
      val blob = Blob(UUID.randomUUID())
      val target = Path.rootDir </> Path.file("foo")

      val vfs =
        BlankVFS.copy(
          paths = Map(target -> blob))

      FreeVFS.readPath[Need](target).eval(vfs).value must beSome(blob)
    }

    "determine the underlying dir for a non-existent blob/version pair" in {
      val blob = Blob(UUID.randomUUID())
      val version = Version(UUID.randomUUID())

      val interp = ().point[Harness[S, IO, ?]]

      val result = interp(FreeVFS.underlyingDir[S](blob, version).eval(BlankVFS)).unsafeRunSync

      result must beNone
    }

    "determine the underlying dir for a blob/version pair already cached" in {
      val blob = Blob(UUID.randomUUID())
      val version = Version(UUID.randomUUID())

      val blobVLog =
        VersionLog(
          BaseDir </> Path.dir(blob.value.toString),
          List(version),
          Set(version))

      val vfs =
        BlankVFS.copy(
          versions = Map(blob -> blobVLog),
          blobs = Set(blob))

      val interp = ().point[Harness[S, IO, ?]]

      val result = interp(FreeVFS.underlyingDir[S](blob, version).eval(vfs)).unsafeRunSync

      result must beSome(BaseDir </> Path.dir(blob.value.toString) </> Path.dir(version.value.toString))
    }

    "determine the underlying dir for a blob/version pair not already cached" in {
      val blob = Blob(UUID.randomUUID())
      val version = Version(UUID.randomUUID())

      val blobVLog =
        VersionLog(
          BaseDir </> Path.dir(blob.value.toString),
          List(version),
          Set(version))

      val vfs = BlankVFS.copy(blobs = Set(blob))

      val interp = vlogInit(BaseDir </> Path.dir(blob.value.toString), Some(List(version)))

      val (vfs2, result) = interp(FreeVFS.underlyingDir[S](blob, version).apply(vfs)).unsafeRunSync

      result must beSome(BaseDir </> Path.dir(blob.value.toString) </> Path.dir(version.value.toString))

      vfs2.versions must haveSize(1)
      vfs2.versions must haveKey(blob)
      vfs2.versions(blob) mustEqual blobVLog
    }

    // headOfBlob, fresh and commit are trivial delegates to VersionLog
  }

  def vlogInit(baseDir: ADir, versions: Option[List[Version]]): Harness[S, IO, Unit] = {
    for {
      _ <- H.pattern[Boolean] {
        case CPL(Exists(target)) =>
          IO {
            target mustEqual (baseDir </> Path.file("versions.json"))

            versions.isDefined
          }
      }

      _ <- versions match {
        case Some(versions) =>
          for {
            _ <- H.pattern[Stream[POSIXWithIO, ByteVector]] {
              case CPL(OpenR(target)) =>
                IO {
                  target mustEqual (baseDir </> Path.file("versions.json"))

                  Stream(ByteVector(versions.map(v => s""""${v.value}"""").mkString("[", ",", "]").getBytes))
                }
            }

            _ <- drainIO

            _ <- H.pattern[List[RPath]] {
              case CPL(Ls(target)) =>
                IO {
                  target mustEqual baseDir

                  versions.map(_.value.toString).map(Path.dir(_))
                }
            }
          } yield ()

        case None =>
          for {
            _ <- H.pattern[Sink[POSIXWithIO, ByteVector]] {
              case CPL(OpenW(target)) =>
                IO {
                  target mustEqual (baseDir </> Path.file("versions.json.new"))

                  (_ => Stream.empty)
                }
            }

            _ <- drainIO

            _ <- H.pattern[Unit] {
              case CPL(Move(from, to)) =>
                IO {
                  from mustEqual (baseDir </> Path.file("versions.json.new"))
                  to mustEqual (baseDir </> Path.file("versions.json"))
                }
            }

            _ <- H.pattern[List[RPath]] {
              case CPL(Ls(target)) =>
                IO {
                  target mustEqual baseDir

                  versions.toList.flatMap(_.map(v => Path.dir(v.value.toString)))
                }
            }
          } yield ()
      }
    } yield ()
  }

  def persistMeta(
      baseDir: ADir,
      pathTest: String => Unit,
      indexTest: String => Unit): Harness[S, IO, Unit] = {

    for {
      uuid <- H.patternWithState[UUID, UUID] {
        case CPL(GenUUID) =>
          IO {
            val uuid = UUID.randomUUID()

            (uuid, uuid)
          }
      }

      _ <- H.pattern[Unit] {
        case CPL(MkDir(target)) =>
          IO {
            target mustEqual (baseDir </> Path.dir(uuid.toString))
          }
      }

      _ <- H.pattern[Sink[POSIXWithIO, ByteVector]] {
        case CPL(OpenW(target)) =>
          IO {
            target mustEqual (baseDir </> Path.dir(uuid.toString) </> Path.file("METAVERSION"))
            assertionSinkBV(_ mustEqual currentMetaVersionBV)
          }
      }

      _ <- drainIO

      _ <- H.pattern[Sink[POSIXWithIO, ByteVector]] {
        case CPL(OpenW(target)) =>
          IO {
            target mustEqual (baseDir </> Path.dir(uuid.toString) </> Path.file("paths.json"))
            assertionSink(pathTest)
          }
      }

      _ <- drainIO

      _ <- H.pattern[Sink[POSIXWithIO, ByteVector]] {
        case CPL(OpenW(target)) =>
          IO {
            target mustEqual (baseDir </> Path.dir(uuid.toString) </> Path.file("index.json"))
            assertionSink(indexTest)
          }
      }

      _ <- drainIO

      _ <- H.pattern[Sink[POSIXWithIO, ByteVector]] {
        case CPL(OpenW(target)) =>
          IO {
            target mustEqual (baseDir </> Path.file("versions.json.new"))
            assertionSink(_ mustEqual s"""["${uuid.toString}"]""")
          }
      }

      _ <- drainIO

      _ <- H.pattern[Unit] {
        case CPL(Move(from, to)) =>
          IO {
            from mustEqual (baseDir </> Path.file("versions.json.new"))
            to mustEqual (baseDir </> Path.file("versions.json"))
          }
      }

      _ <- H.pattern[Unit] {
        case CPL(Delete(target)) =>
          IO {
            target mustEqual (baseDir </> Path.dir("HEAD"))
          }
      }

      _ <- H.pattern[Boolean] {
        case CPL(LinkDir(from, to)) =>
          IO {
            from mustEqual (baseDir </> Path.dir(uuid.toString))
            to mustEqual (baseDir </> Path.dir("HEAD"))

            true
          }
      }
    } yield ()
  }

  val CPR = CopK.Inject[IO, S]

  val CPL = CopK.Inject[POSIXOp, S]

  val drainIO = H.whileDefined[Unit] {
    case CPR(ioa) => ioa
  }
}
