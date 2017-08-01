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

package quasar.fs.mount

import scala.Predef.$conforms
import slamdata.Predef._
import quasar.Variables
import quasar.contrib.pathy.{ADir, AFile, APath}
import quasar.fs.{PathError, FileSystemType}
import quasar.sql, sql.{ScopedExpr, Sql}

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import monocle.function.Field1
import monocle.std.{disjunction => D}
import org.specs2.execute._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

abstract class MountingSpec[S[_]](
  implicit
  S0: Mounting :<: S,
  S1: MountingFailure :<: S,
  S2: PathMismatchFailure :<: S
) extends quasar.Qspec {

  import MountConfig.{viewConfig, fileSystemConfig}

  def interpName: String
  def interpret: S ~> Task

  val mnt = Mounting.Ops[S]
  val mntErr = MountingFailure.Ops[S]
  val mmErr = PathMismatchFailure.Ops[S]
  // NB: Without the explicit imports, scalac complains of an import cycle
  import mnt.{FreeS, havingPrefix, lookupConfig, lookupType, mountView, mountFileSystem, mountModule, remount, replace, unmount, viewsHavingPrefix, modulesHavingPrefix}

  implicit class StrOps(s: String) {
    def >>*[A: AsResult](a: => FreeS[A]) =
      s >> a.foldMap(interpret).unsafePerformSync
  }

  val noVars   = Variables.fromMap(Map.empty)
  val exprA    = ScopedExpr(sql.stringLiteral[Fix[Sql]]("A").embed, Nil)
  val exprB    = ScopedExpr(sql.stringLiteral[Fix[Sql]]("B").embed, Nil)
  val viewCfgA = viewConfig(exprA, noVars)
  val viewCfgB = viewConfig(exprB, noVars)

  val dbType   = FileSystemType("db")
  val uriA     = ConnectionUri("db://example.com/A")
  val uriB     = ConnectionUri("db://example.com/B")
  val fsCfgA   = fileSystemConfig(dbType, uriA)
  val fsCfgB   = fileSystemConfig(dbType, uriB)

  val invalidPath = MountingError.pathError composePrism
                    PathError.invalidPath  composeLens
                    Field1.first

  val notFound = MountingError.pathError composePrism
                 PathError.pathNotFound

  val pathExists = MountingError.pathError composePrism
                   PathError.pathExists

  def maybeNotFound[A](dj: MountingError \/ A): Option[APath] =
    D.left composePrism notFound getOption dj

  def maybeExists[A](dj: MountingError \/ A): Option[APath] =
    D.left composePrism pathExists getOption dj

  def mountViewNoVars(loc: AFile, scopedExpr: ScopedExpr[Fix[Sql]]): FreeS[Unit] =
    mountView(loc, scopedExpr, noVars)

  s"$interpName mounting interpreter" should {
    "havingPrefix" >> {
      "returns all prefixed locations and types" >>* {
        val f1 = rootDir </> dir("d1") </> dir("d1.1") </> file("f1")
        val f2 = rootDir </> dir("d1") </> file("f2")
        val dA = rootDir </> dir("d1") </> dir("A")
        val dB = rootDir </> dir("d2") </> dir("B")

        val mnts = Map[APath, MountType](
          f1 -> MountType.viewMount(),
          f2 -> MountType.viewMount(),
          dA -> MountType.fileSystemMount(dbType))

        val setup =
          mountViewNoVars(f1, exprA)        *>
          mountViewNoVars(f2, exprA)        *>
          mountFileSystem(dA, dbType, uriA) *>
          mountFileSystem(dB, dbType, uriB)

        (setup *> havingPrefix(rootDir </> dir("d1")))
          .map(_ must_=== mnts ∘ (_.right))
      }

      "returns nothing when no mounts have the given prefix" >>* {
        havingPrefix(rootDir </> dir("dne")) map (_ must beEmpty)
      }

      "does not include a mount at the prefix" >>* {
        val d = rootDir </> dir("d3") </> dir("someMount")

        (mountFileSystem(d, dbType, uriA) *> havingPrefix(d))
          .map(_ must beEmpty)
      }
    }

    "lookupConfig" >> {
      "returns a view config when asked for an existing view path" >>* {
        val f = rootDir </> dir("d1") </> file("f1")

        (mountViewNoVars(f, exprA) *> lookupConfig(f).run.run)
          .map(_ must beSome(viewCfgA.right[MountingError]))
      }

      "returns a filesystem config when asked for an existing fs path" >>* {
        val d = rootDir </> dir("d1")

        (mountFileSystem(d, dbType, uriA) *> lookupConfig(d).run.run)
          .map(_ must beSome(fsCfgA.right[MountingError]))
      }

      "returns none when nothing mounted at the requested path" >>* {
        val f = rootDir </> dir("d2") </> file("f2")
        val d = rootDir </> dir("d3")

        lookupConfig(f).run.run.tuple(lookupConfig(d).run.run)
          .map(_ must_=== ((None, None)))
      }

      "viewsHavingPrefix" >>* {
        val v = rootDir </> dir("d1") </> file("f1")
        val m = rootDir </> dir("d1") </> dir("d4")
        val f = rootDir </> dir("d1") </> dir("d5")

        (mountViewNoVars(v, exprA) *> mountModule(m, Nil) *> mountFileSystem(f, dbType, uriA) *> viewsHavingPrefix(rootDir))
          .map(_ must_= Set(v))
      }

      "modulesHavingPrefix" >>* {
        val v = rootDir </> dir("d1") </> file("f1")
        val m = rootDir </> dir("d1") </> dir("d4")
        val f = rootDir </> dir("d1") </> dir("d5")

        (mountViewNoVars(v, exprA) *> mountModule(m, Nil) *> mountFileSystem(f, dbType, uriA) *> modulesHavingPrefix(rootDir))
          .map(_ must_= Set(m))
      }
    }

    "lookupType" >> {
      "returns the view type when asked for an existing view path" >>* {
        val f = rootDir </> dir("d1") </> file("f1")

        (mountViewNoVars(f, exprA) *> lookupType(f).run.run)
          .map(_ must beSome(MountType.viewMount().right[MountingError]))
      }

      "returns a filesystem type when asked for an existing fs path" >>* {
        val d = rootDir </> dir("d1")

        (mountFileSystem(d, dbType, uriA) *> lookupType(d).run.run)
          .map(_ must beSome(MountType.fileSystemMount(dbType).right[MountingError]))
      }

      "returns none when nothing mounted at the requested path" >>* {
        val f = rootDir </> dir("d2") </> file("f2")
        val d = rootDir </> dir("d3")

        lookupType(f).run.run.tuple(lookupType(d).run.run)
          .map(_ must_=== ((None, None)))
      }
    }

    "mountViewNoVars" >> {
      "allow mounting a view at a file path" >>* {
        val f = rootDir </> file("f1")

        (mountViewNoVars(f, exprA) *> lookupConfig(f).run.run)
          .map(_ must beSome(viewCfgA.right[MountingError]))
      }

      "allow mounting a view above another view" >>* {
        val f1 = rootDir </> dir("d1") </> dir("d2") </> file("f1")
        val f2 = rootDir </> dir("d1") </> file("d2")

        val r = (
          mountViewNoVars(f1, exprA) *>
          mountViewNoVars(f2, exprB)
        ) *> lookupConfig(f2).run.run

        r map (_ must beSome(viewCfgB.right[MountingError]))
      }

      "allow mounting a view below another view" >>* {
        val f1 = rootDir </> dir("d1") </> file("d2")
        val f2 = rootDir </> dir("d1") </> dir("d2") </> file("f2")

        val r = (
          mountViewNoVars(f1, exprA) *>
          mountViewNoVars(f2, exprB)
        ) *> lookupConfig(f2).run.run

        r map (_ must beSome(viewCfgB.right[MountingError]))
      }

      "allow mounting a view above a filesystem" >>* {
        val d = rootDir </> dir("d1") </> dir("db")
        val f = rootDir </> file("d1")

        val r = (
          mountFileSystem(d, dbType, uriA) *>
          mountViewNoVars(f, exprA)
        ) *> lookupConfig(f).run.run

        r map (_ must beSome(viewCfgA.right[MountingError]))
      }

      "allow mounting a view at a file with the same name as an fs mount" >>* {
        val d = rootDir </> dir("d1")
        val f = rootDir </> file("d1")

        val r = (
          mountFileSystem(d, dbType, uriA) *>
          mountViewNoVars(f, exprA)
        ) *> lookupConfig(f).run.run

        r map (_ must beSome(viewCfgA.right[MountingError]))
      }

      "allow mounting a view below a filesystem" >>* {
        val d = rootDir </> dir("d1")
        val f = rootDir </> dir("d1") </> file("f1")

        val r = (
          mountFileSystem(d, dbType, uriA) *>
          mountViewNoVars(f, exprA)
        ) *> lookupConfig(f).run.run

        r map (_ must beSome(viewCfgA.right[MountingError]))
      }

      "fail when a view is already mounted at the file path" >>* {
        val f = rootDir </> dir("d1") </> file("f1")

        mntErr.attempt(mountViewNoVars(f, exprA) *> mountViewNoVars(f, exprB)) map { r =>
          maybeExists(r) must beSome(f)
        }
      }
    }

    "mountFileSystem" >> {
      def mountFF(d1: ADir, d2: ADir): FreeS[Unit] =
        mountFileSystem(d1, dbType, uriA) *> mountFileSystem(d2, dbType, uriB)

      def mountVF(f: AFile, d: ADir): FreeS[Option[MountConfig]] =
        mountViewNoVars(f, exprA)        *>
        mountFileSystem(d, dbType, uriA) *>
        lookupConfig(d).run.run ∘ (_ >>= (_.toOption))

      "mounts a filesystem at a directory" >>* {
        val d = rootDir </> dir("d1")

        (mountFileSystem(d, dbType, uriA) *> lookupConfig(d).run.run)
          .map(_ must beSome(fsCfgA.right[MountingError]))
      }

      "succeed mounting above an existing fs mount" >>* {
        val d1 = rootDir </> dir("d1")
        val d2 = d1 </> dir("d2")

        (mountFF(d2, d1) *> lookupConfig(d1).run.run) map (_ must beSome(fsCfgB.right[MountingError]))
      }

      "succeed mounting below an existing fs mount" >>* {
        val d1 = rootDir </> dir("d1")
        val d2 = d1 </> dir("d2")

        (mountFF(d1, d2) *> lookupConfig(d2).run.run) map (_ must beSome(fsCfgB.right[MountingError]))
      }

      "fail when mounting at an existing fs mount" >>* {
        val d = rootDir </> dir("exists")

        mntErr.attempt(mountFF(d, d)).tuple(lookupConfig(d).run.run) map { case (dj, cfg) =>
          maybeExists(dj).tuple(cfg) must beSome((d, fsCfgA.right[MountingError]))
        }
      }

      "succeed when mounting above an existing view mount" >>* {
        val d = rootDir </> dir("d1")
        val f = d </> file("view")

        mountVF(f, d) map (_ must beSome(fsCfgA))
      }

      "succeed when mounting at a dir with same name as existing view mount" >>* {
        val f = rootDir </> dir("d2") </> file("view")
        val d = rootDir </> dir("d2") </> dir("view")

        mountVF(f, d) map (_ must beSome(fsCfgA))
      }

      "succeed when mounting below an existing view mount" >>* {
        val f = rootDir </> dir("d2") </> file("view")
        val d = rootDir </> dir("d2") </> dir("view") </> dir("db")

        mountVF(f, d) map (_ must beSome(fsCfgA))
      }
    }

    "mount" >> {
      "succeeds when loc and config agree" >>* {
        val f = rootDir </> file("view")

        (mnt.mount(f, viewCfgA) *> lookupConfig(f).run.run)
          .map(_ must beSome(viewCfgA.right[MountingError]))
      }

      "fails when using a dir for a view" >>* {
        mmErr.attempt(mnt.mount(rootDir, viewCfgA))
          .map(_ must be_-\/(Mounting.PathTypeMismatch(rootDir)))
      }

      "fails when using a file for a filesystem" >>* {
        val f = rootDir </> file("foo")
        mmErr.attempt(mnt.mount(f, fsCfgA))
          .map(_ must be_-\/(Mounting.PathTypeMismatch(f)))
      }
    }

    "remount" >> {
      "moves the mount at src to dst" >>* {
        val d1 = rootDir </> dir("d1")
        val d2 = rootDir </> dir("d2")

        val r =
          (mnt.mount(d1, fsCfgA) *> remount(d1, d2)) *>
          (lookupConfig(d1).run.run.tuple(lookupConfig(d2).run.run))

        r map (_ must_=== ((None, Some(fsCfgA.right[MountingError]))))
      }

      "moves the mount at src to dst (file)" >>* {
        val d1 = rootDir </> file("d1")
        val d2 = rootDir </> file("d2")

        val r =
          (mnt.mount(d1, viewCfgA) *> remount(d1, d2)) *>
          (lookupConfig(d1).run.run.tuple(lookupConfig(d2).run.run))

        r map (_ must_=== ((None, Some(viewCfgA.right[MountingError]))))
      }

      "moves mount nested under src to dst" >>* {
        val d1 = rootDir </> dir("d1")
        val d2 = rootDir </> dir("d2")
        val f = dir("d3") </> file("foo")

        val r =
          (mnt.mount(d1, fsCfgA) *> mnt.mount(d1 </> f, viewCfgA) *> remount(d1, d2)) *>
          (lookupConfig(d1 </> f).run.run.tuple(lookupConfig(d2 </> f).run.run))

        r map (_ must_=== ((None, Some(viewCfgA.right[MountingError]))))
      }

      "succeeds when src == dst" >>* {
        val d = rootDir </> dir("srcdst")

        val r =
          mnt.mount(d, fsCfgB) *>
          remount(d, d)        *>
          lookupConfig(d).run.run

        r map (_ must beSome(fsCfgB.right[MountingError]))
      }

      "fails if there is no mount at src" >>* {
        val d = rootDir </> dir("dne")

        mntErr.attempt(remount(d, rootDir)) map (dj =>
          maybeNotFound(dj) must beSome(d))
      }

      "restores the mount at src if mounting fails at dst" >>* {
        val f1 = rootDir </> file("f1")
        val f2 = rootDir </> file("f2")

        val r =
          mountViewNoVars(f1, exprA) *>
          mountViewNoVars(f2, exprB) *>
          remount(f1, f2)

        mntErr.attempt(r).tuple(lookupConfig(f1).run.run) map { case (dj, cfg) =>
          maybeExists(dj).tuple(cfg) must beSome((f2, viewCfgA.right[MountingError]))
        }
      }
    }

    "replace" >> {
      "replaces the mount at the location with a new one" >>* {
        val d = rootDir </> dir("replace")

        val r =
          mnt.mount(d, fsCfgA) *>
          replace(d, fsCfgB)   *>
          lookupConfig(d).run.run

        r map (_ must beSome(fsCfgB.right[MountingError]))
      }

      "fails if there is no mount at the given src location" >>* {
        val f = rootDir </> dir("dne") </> file("f1")

        mntErr.attempt(replace(f, viewCfgA)) map (dj =>
          maybeNotFound(dj) must beSome(f))
      }

      "restores the previous mount if mounting the new config fails" >>* {
        val f = rootDir </> file("f1")
        val r = mountViewNoVars(f, exprA) *> replace(f, fsCfgB)

        mmErr.attempt(r).tuple(lookupConfig(f).run.run)
          .map(_ must_=== ((-\/(Mounting.PathTypeMismatch(f)), Some(viewCfgA.right[MountingError]))))
      }
    }

    "unmount" >> {
      "should remove an existing view mount" >>* {
        val f = rootDir </> file("tounmount")

        val r =
          mountViewNoVars(f, exprA) *>
          unmount(f)                *>
          lookupConfig(f).run.run

        r map (_ must beNone)
      }

      "should remove an existing fs mount" >>* {
        val d = rootDir </> dir("tounmount")

        val r =
          mountFileSystem(d, dbType, uriB) *>
          unmount(d)                       *>
          lookupConfig(d).run.run

        r map (_ must beNone)
      }

      "should remove a nested view mount" >>* {
        val d = rootDir </> dir("tounmount")
        val f = d </> file("nested")

        val r =
          mountFileSystem(d, dbType, uriB) *>
          mountViewNoVars(f, exprA)        *>
          unmount(d)                       *>
          lookupConfig(f).run.run

        r map (_ must beNone)
      }

      "should fail when nothing mounted at path" >>* {
        val f = rootDir </> dir("nothing") </> file("there")
        mntErr.attempt(unmount(f)) map (dj => maybeNotFound(dj) must beSome(f))
      }
    }
  }
}
