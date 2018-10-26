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

package quasar.mimir.storage

import slamdata.Predef.Map
import quasar.contrib.nio.file.deleteRecursively
import quasar.contrib.scalaz.MonadError_
import quasar.impl.storage.IndexedStoreSpec
import quasar.mimir.Precog
import quasar.precog.common.RValue
import quasar.yggdrasil.vfs.{contextShiftForS, ResourceError}

import java.nio.file.Files
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext, ExecutionContext.Implicits.global
import scala.util.Random

import cats.effect.IO
import monocle.Prism
import pathy.Path._
import shims._

final class MimirIndexedStoreSpec extends IndexedStoreSpec[IO, StoreKey, RValue] {

  val blockingPool = ExecutionContext.fromExecutor(Executors.newCachedThreadPool)

  implicit val ioMonadResourceError: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](Prism.partial[Throwable, ResourceError] {
      case e: RuntimeException => ResourceError.corrupt(e.getMessage)
    } (e => new RuntimeException(e.messages.head)))

  lazy val P = {
    val init = for {
      tmpDir <- IO(Files.createTempDirectory("mimir-indexed-store-spec-"))
      precog <- Precog(tmpDir.toFile, blockingPool)
      result = precog.onDispose(deleteRecursively[IO](tmpDir))
    } yield result

    init.unsafeRunSync
  }

  val emptyStore =
    IO(Random.nextInt(100000)) map { i =>
      MimirIndexedStore[IO](
        P.unsafeValue,
        rootDir </> dir("store") </> dir(i.toString))
    }

  val freshIndex = IO(StoreKey(Random.alphanumeric.take(8).mkString))

  val valueA = RValue.rObject(Map(("value", RValue.rNum(42))))

  val valueB = RValue.rString("B")

  step(P.dispose.unsafeRunSync)
}
