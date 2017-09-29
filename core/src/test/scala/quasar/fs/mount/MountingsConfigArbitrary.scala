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

import quasar.contrib.pathy.{ADir, AFile, APath}

import pathy.scalacheck.PathyArbitrary._
import org.scalacheck.{Arbitrary, Gen}

trait MountingsConfigArbitrary {

  implicit val mountingsConfigArbitrary: Arbitrary[MountingsConfig] =
    Arbitrary(
      Gen
        .listOf(Gen.oneOf(genFileSystemConfigEntry, genViewConfigEntry))
        .map(l => MountingsConfig(l.toMap)))

  private def genFileSystemConfigEntry: Gen[(APath, MountConfig)] =
    for {
      dir <- Arbitrary.arbitrary[ADir]
      config <- MountConfigArbitrary.genFileSystemConfig
    } yield (dir, config)

  private def genViewConfigEntry: Gen[(APath, MountConfig)] =
    for {
      file <- Arbitrary.arbitrary[AFile]
      config <- MountConfigArbitrary.genViewConfig
    } yield (file, config)
}

object MountingsConfigArbitrary extends MountingsConfigArbitrary
