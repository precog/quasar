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

package quasar.metastore

import slamdata.Predef._
import quasar.contrib.pathy.{ADir, APath}
import quasar.effect.Failure
import quasar.fs.mount._, Mounting.PathTypeMismatch
import quasar.fp._, free._, ski._
import quasar.db._

import doobie.imports._
import org.specs2.ScalaCheck
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task

class MetaStoreMounterSpec extends MountingSpec[MetaStoreMounterSpec.Eff] with ScalaCheck {
  import MetaStoreMounterSpec.Eff
  import MountRequest.MountFileSystem

  isolated  // NB: each test needs a fresh DB, or else need to get cleanup working in between

  val xa = {
    val rand = scala.util.Random.nextInt
    val tr = DbUtil.simpleTransactor(DbUtil.inMemoryConnectionInfo(s"test_mem_$rand"))

    Schema.schema.updateToLatest.transact(tr).unsafePerformSync

    tr
  }

  def interpName = "MetaStoreMounter"

  def interpret: Eff ~> Task = {
    val t: Mounting ~> Free[ConnectionIO, ?] =
      MetaStoreMounter[ConnectionIO, ConnectionIO](
        doMount.andThen(_.point[ConnectionIO]),
        κ(().point[ConnectionIO]))

    val db: ConnectionIO ~> Task =
      λ[ConnectionIO ~> Task](_.transact(xa))

    foldMapNT(db).compose(t)                       :+:
    Failure.toRuntimeError[Task, PathTypeMismatch] :+:
    Failure.toRuntimeError[Task, MountingError]
  }

  val invalidUri = ConnectionUri(uriA.value + "INVALID")
  val invalidCfg = MountConfig.fileSystemConfig(dbType, invalidUri)
  val invalidErr = MountingError.invalidConfig(invalidCfg, "invalid URI".wrapNel)

  def doMount: MountRequest => MountingError \/ Unit = {
    case MountFileSystem(_, `dbType`, `invalidUri`) => invalidErr.left
    case _                                          => ().right
  }

  "Handling mounts" should {
    "fail when handler function fails" >>* {
      val loc = rootDir </> dir("some") </> dir("db")
      val cfg = MountConfig.fileSystemConfig(dbType, invalidUri)

      mntErr.attempt(mnt.mountFileSystem(loc, dbType, invalidUri))
        .tuple(mnt.lookupConfig(loc).run)
        .map(_ must_=== ((MountingError.invalidConfig(cfg, "invalid URI".wrapNel).left, None)))
    }
  }

  "havingPrefix with path containing JDBC matching characters" should {
    // Clean up after each scalacheck-generated dir to avoid collisions:
    def unmountAll(paths: APath*): Task[Unit] =
      paths.toList.traverse_(mnt.unmount).foldMap(interpret)

    "handle '_'" >> prop { parent: ADir =>
      val prefix = parent </> dir("tricky_")
      val mntA = prefix </> dir("mntA")
      val trickyD = parent </> dir("trickyA") </> dir("mnt")
      val trickyF = parent </> dir("trickyB") </> file("mnt")

      val p =
        mnt.mountFileSystem(mntA, dbType, uriA)    *>
        mnt.mountFileSystem(trickyD, dbType, uriB) *>
        mnt.mountView(trickyF, exprA, noVars)      *>
        mnt.havingPrefix(prefix)

      val rez = p.foldMap(interpret).unsafePerformSync

      unmountAll(mntA, trickyD, trickyF).unsafePerformSync

      rez.keySet must_=== Set(mntA)
    }

    "handle '%'" >> prop { parent: ADir =>
      val prefix = parent </> dir("tricky%C")
      val mntA = prefix </> dir("mntA")
      val trickyD = parent </> dir("trickyA") </> dir("subC") </> dir("mnt")
      val trickyF = parent </> dir("trickyC") </> file("mnt")

      val p =
        mnt.mountFileSystem(mntA, dbType, uriA)    *>
        mnt.mountFileSystem(trickyD, dbType, uriB) *>
        mnt.mountView(trickyF, exprA, noVars)      *>
        mnt.havingPrefix(prefix)

      val rez = p.foldMap(interpret).unsafePerformSync

      unmountAll(mntA, trickyD, trickyF).unsafePerformSync

      rez.keySet must_=== Set(mntA)
    }

    "handle '\\\\'" >> prop { parent: ADir =>
      val prefix = parent </> dir("tric\\ky")
      val mntA = prefix </> dir("mntA")
      val trickyD = parent </> dir("tricky") </> dir("sub") </> dir("mnt")
      val trickyF = parent </> dir("tricky") </> file("mnt")

      val p =
        mnt.mountFileSystem(mntA, dbType, uriA)    *>
        mnt.mountFileSystem(trickyD, dbType, uriB) *>
        mnt.mountView(trickyF, exprA, noVars)      *>
        mnt.havingPrefix(prefix)

      val rez = p.foldMap(interpret).unsafePerformSync

      unmountAll(mntA, trickyD, trickyF).unsafePerformSync

      rez.keySet must_=== Set(mntA)
    }
  }
}

object MetaStoreMounterSpec {
  type Eff[A]  = Coproduct[Mounting, Eff0, A]
  type Eff0[A] = Coproduct[PathMismatchFailure, MountingFailure, A]
}
