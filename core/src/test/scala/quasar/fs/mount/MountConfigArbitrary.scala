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

package quasar.fs.mount

import slamdata.Predef._
import quasar.Variables
import quasar.VariablesGenerators._
import quasar.fs._, FileSystemTypeArbitrary._
import quasar.sql._, ScopedExprArbitrary._, ExprArbitrary._

import matryoshka.data.Fix
import org.scalacheck.{Arbitrary, Gen}

trait MountConfigArbitrary {
  import MountConfig._, ConnectionUriArbitrary._

  implicit val mountConfigArbitrary: Arbitrary[MountConfig] =
    Arbitrary(Gen.oneOf(genFileSystemConfig, genViewConfig, genModuleConfig))

  implicit val viewConfigArbitrary: Arbitrary[ViewConfig] =
    Arbitrary(genViewConfig)

  implicit val moduleConfigArbitrary: Arbitrary[ModuleConfig] =
    Arbitrary(genModuleConfig)

  private[mount] def genFileSystemConfig: Gen[MountConfig] =
    for {
      typ <- Arbitrary.arbitrary[FileSystemType]
      uri <- Arbitrary.arbitrary[ConnectionUri]
    } yield fileSystemConfig(typ, uri)

  private[fs] def genViewConfig: Gen[ViewConfig] =
    for {
      scopedExpr <- Arbitrary.arbitrary[ScopedExpr[Fix[Sql]]]
      vars <- Arbitrary.arbitrary[Variables]
    } yield ViewConfig(scopedExpr, vars)

  private[fs] def genModuleConfig: Gen[ModuleConfig] =
    for {
      scopedExpr <- Arbitrary.arbitrary[Fix[Sql]]
    } yield ModuleConfig(List(FunctionDecl(CIName("foo"), Nil, scopedExpr)))
}

object MountConfigArbitrary extends MountConfigArbitrary
