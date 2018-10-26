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

package quasar.yggdrasil
package table

import quasar.contrib.cats.effect._
import quasar.contrib.pathy.AFile
import quasar.precog.common.CLong
import quasar.niflheim.NIHDB
import quasar.yggdrasil.vfs.{Blob, SerialVFS, Version}

import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.reflectiveCalls

import cats.effect.IO
import org.specs2.mutable.Specification
import pathy.Path.{file, rootDir}
import scalaz.{-\/, StreamT, \/-}
import shims._

object VFSColumnarTableModuleSpec
    extends Specification
    with VFSColumnarTableModule
    with nihdb.NIHDBAkkaSetup {

  implicit val ExecutionContext = scala.concurrent.ExecutionContext.global


  "table tempfile caching" should {

    "check db commits persistence head after clearing hotcache" in {


      val path: AFile =  rootDir </> file("foo")

      val test = for {
        created <- createDB(path) map {
          case \/-(t) => t
          case -\/(_) => ko
        }
        (blob: Blob, version: Version, db: NIHDB) = created

        commit1 <- commitDB(blob, version, db)

        created2 <- createDB(path) map {
          case \/-(t) => t
          case -\/(_) => ko
        }
        (_, version2: Version, db2: NIHDB) = created2

        commit2 <- commitDB(blob, version2, db2)

        _ <- IO.fromFutureShift(IO(db.close))
        _ <- IO.fromFutureShift(IO(db2.close))

        // clear the hot cache
        xx = this.asInstanceOf[{ val quasar$yggdrasil$table$VFSColumnarTableModule$$dbs: ConcurrentHashMap[(Blob, Version), NIHDB] }]
        _ = xx.quasar$yggdrasil$table$VFSColumnarTableModule$$dbs.clear()

        finalHead <- vfs.headOfBlob(blob)
        actualHead = finalHead match {
          case Some(h: Version) => h
          case _ => Version(UUID.fromString("00000000-0000-0000-0000-000000000000"))
        }
      } yield {
        commit1 must_=== true
        commit2 must_=== true
        actualHead must_=== version2
      }

      test.unsafeRunSync
    }



    "evaluate effects exactly once (sequencing lazily)" in {
      val ctr = new AtomicInteger(0)
      val cget = IO(ctr.get())

      val sliceM = IO {
        ctr.incrementAndGet()
        Slice.fromRValues(Stream(CLong(42))) :: StreamT.empty[IO, Slice]
      }

      val table = Table(StreamT.wrapEffect(sliceM), UnknownSize)

      val eff = for {
        cachedD <- cacheTable(table, false)
        cached = cachedD.unsafeValue

        tests = for {
          i <- cget
          _ <- IO(i mustEqual 0)

          _ <- cached.slices.foreach(_ => IO.pure(()))

          i <- cget
          _ <- IO(i mustEqual 1)

          _ <- cached.slices.foreach(_ => IO.pure(()))

          i <- cget
          _ <- IO(i mustEqual 1)
        } yield ()

        e <- tests.attempt
        _ <- cachedD.dispose
        _ <- e.fold(IO.raiseError(_), IO.pure(_))
      } yield ok

      eff.unsafeRunSync
    }

    "evaluate effects exactly once (sequencing eagerly)" in {
      val ctr = new AtomicInteger(0)
      val cget = IO(ctr.get())

      val sliceM = IO {
        ctr.incrementAndGet()
        Slice.fromRValues(Stream(CLong(42))) :: StreamT.empty[IO, Slice]
      }

      val table = Table(StreamT.wrapEffect(sliceM), UnknownSize)

      val eff = for {
        cachedD <- cacheTable(table, true)
        cached = cachedD.unsafeValue

        tests = for {
          i <- cget
          _ <- IO(i mustEqual 1)

          _ <- cached.slices.foreach(_ => IO.pure(()))

          i <- cget
          _ <- IO(i mustEqual 1)

          _ <- cached.slices.foreach(_ => IO.pure(()))

          i <- cget
          _ <- IO(i mustEqual 1)
        } yield ()

        e <- tests.attempt
        _ <- cachedD.dispose
        _ <- e.fold(IO.raiseError(_), IO.pure(_))
      } yield ok

      eff.unsafeRunSync
    }
  }

  val base = Files.createTempDirectory("VFSColumnarTableModuleSpec").toFile
  def vfs = SerialVFS[IO](base, global).unsafeRunSync.unsafeValue
  def StorageTimeout = Timeout

  sealed trait TableCompanion extends VFSColumnarTableCompanion
  object Table extends TableCompanion
}
