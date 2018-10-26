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

package quasar.mimir
package storage

import quasar.EffectfulQSpec
import quasar.contrib.nio.file.deleteRecursively
import quasar.contrib.scalaz.MonadError_
import quasar.precog.common.{CDouble, CLong, CString, RObject}
import quasar.yggdrasil.vfs.{contextShiftForS, ResourceError}

import java.nio.file.Files
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext, ExecutionContext.Implicits.global
import scala.util.Random

import cats.effect.IO
import monocle.Prism
import pathy.Path.{dir, rootDir}
import scalaz.std.option._
import scalaz.syntax.traverse._
import shims._

object MimirPTableStoreSpec extends EffectfulQSpec[IO] {

  val blockingPool = ExecutionContext.fromExecutor(Executors.newCachedThreadPool)

  implicit val ioMonadResourceError: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](Prism.partial[Throwable, ResourceError] {
      case e: RuntimeException => ResourceError.corrupt(e.getMessage)
    } (e => new RuntimeException(e.messages.head)))

  lazy val P = {
    val init = for {
      tmpDir <- IO(Files.createTempDirectory("mimir-ptable-store-spec-"))
      precog <- Precog(tmpDir.toFile, blockingPool)
      result = precog.onDispose(deleteRecursively[IO](tmpDir))
    } yield result

    init.unsafeRunSync
  }

  val EmptyStore =
    IO(Random.nextInt(100000)) map { i =>
      MimirPTableStore[IO](
        P.unsafeValue,
        rootDir </> dir("ptables") </> dir(i.toString))
    }

  "ptable store" should {
    "write and then read back results" >>* {
      val testData = RObject(
        "foo" -> CLong(42),
        "bar" -> CString("hi!"),
        "baz" -> RObject(
          "qux" -> CDouble(3.14)))

      val key = StoreKey("key1")

      for {
        store <- EmptyStore
        _ <- store.write(key, store.cake.Table.fromRValues(Stream(testData))).compile.drain
        readBack <- store.read(key)
        rvaluesOpt <- readBack.traverse(_.toJson)
      } yield {
        val results = rvaluesOpt.toIterable.flatten

        results must haveSize(1)
        results must contain(testData)
      }
    }

    "write repeatedly and then read back last results" >>* {
      val firstTestData = CLong(1)

      val testData = RObject(
        "foo" -> CLong(42),
        "bar" -> CString("hi!"),
        "baz" -> RObject(
          "qux" -> CDouble(3.14)))

      val key = StoreKey("key1")

      for {
        store <- EmptyStore
        _ <- store.write(key, store.cake.Table.fromRValues(Stream(firstTestData))).compile.drain
        _ <- store.write(key, store.cake.Table.fromRValues(Stream(testData))).compile.drain
        readBack <- store.read(key)
        rvaluesOpt <- readBack.traverse(_.toJson)
      } yield {
        val results = rvaluesOpt.toIterable.flatten

        results must haveSize(1)
        results must contain(testData)
      }
    }

    "fail read on a non-existent key" >>* {
      val key = StoreKey("key1")

      for {
        store <- EmptyStore
        check <- store.read(key).map(a => a: Option[AnyRef])    // workaround for scalac's failure to unify an existential type
      } yield check must beNone
    }

    "fail existence on a non-existent key" >>* {
      val key = StoreKey("key1")

      for {
        store <- EmptyStore
        check <- store.exists(key)
      } yield check must beFalse
    }

    "write then show exists" >>* {
      val testData = CLong(42)
      val key = StoreKey("key1")

      for {
        store <- EmptyStore
        _ <- store.write(key, store.cake.Table.fromRValues(Stream(testData))).compile.drain
        check <- store.exists(key)
      } yield check must beTrue
    }

    "write then delete and show not exists" >>* {
      val testData = CString("daniel")
      val key = StoreKey("key1")

      for {
        store <- EmptyStore
        _ <- store.write(key, store.cake.Table.fromRValues(Stream(testData))).compile.drain
        _ <- store.delete(key)
        check <- store.exists(key)
      } yield check must beFalse
    }
  }

  step(P.dispose.unsafeRunSync)
}
