/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Variables
import quasar.VariablesArbitrary._
import quasar.fs._, FileSystemTypeArbitrary._
import quasar.sql._, ExprArbitrary._

import matryoshka.data.Fix
import org.scalacheck.{Arbitrary, Gen}

trait MountConfigArbitrary {
  import MountConfig._, ConnectionUriArbitrary._

  implicit val mountConfigArbitrary: Arbitrary[MountConfig] =
    Arbitrary(Gen.oneOf(genFileSystemConfig, genViewConfig))

  private def genFileSystemConfig: Gen[MountConfig] =
    for {
      typ <- Arbitrary.arbitrary[FileSystemType]
      uri <- Arbitrary.arbitrary[ConnectionUri]
    } yield fileSystemConfig(typ, uri)

  private def genViewConfig: Gen[MountConfig] =
    for {
      expr <- Arbitrary.arbitrary[Fix[Sql]]
      vars <- Arbitrary.arbitrary[Variables]
    } yield viewConfig(expr, vars)
}

object MountConfigArbitrary extends MountConfigArbitrary
