/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.contrib.nio

import slamdata.Predef.{Array, SuppressWarnings, Unit}
import quasar.contrib.fs2.convert

import java.nio.file.{Files, Path}

import cats.effect.Sync
import fs2.Stream

object file {
  /** Deletes the path, including all descendants if it refers to a directory. */
  def deleteRecursively[F[_]](path: Path)(implicit F: Sync[F]): F[Unit] = {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def go(p: Path): Stream[F, Unit] =
      Stream.eval(F.delay(Files.isDirectory(p)))
        .flatMap(isDir =>
          if (isDir)
            convert.fromJavaStream(F.delay(Files.list(p))).flatMap(go)
          else
            Stream.empty)
        .append(Stream.eval(F.delay(Files.delete(p))))

    go(path).compile.drain
  }
}
