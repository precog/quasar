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

package quasar.fs

import quasar.contrib.pathy._
import quasar.fp.free.flatMapSNT

import scalaz._, NaturalTransformation.refl

object chroot {

  /** Rebases all paths in `ReadFile` operations onto the given prefix. */
  def readFile[S[_]](prefix: ADir)(implicit S: ReadFile :<: S): S ~> Free[S, ?] =
    transformPaths.readFile[S](rebaseA(prefix), stripPrefixA(prefix))

  /** Rebases all paths in `WriteFile` operations onto the given prefix. */
  def writeFile[S[_]](prefix: ADir)(implicit S: WriteFile :<: S): S ~> Free[S, ?] =
    transformPaths.writeFile[S](rebaseA(prefix), stripPrefixA(prefix))

  /** Rebases all paths in `ManageFile` operations onto the given prefix. */
  def manageFile[S[_]](prefix: ADir)(implicit S: ManageFile :<: S): S ~> Free[S, ?] =
    transformPaths.manageFile[S](rebaseA(prefix), stripPrefixA(prefix))

  /** Rebases paths in `QueryFile` onto the given prefix. */
  def queryFile[S[_]](prefix: ADir)(implicit S: QueryFile :<: S): S ~> Free[S, ?] =
    transformPaths.queryFile[S](rebaseA(prefix), stripPrefixA(prefix), refl)

    /** Rebases paths in `Analyze` onto the given prefix. */
  def analyze[S[_]](prefix: ADir)(implicit S: Analyze :<: S): S ~> Free[S, ?] =
    transformPaths.analyze[S](rebaseA(prefix), stripPrefixA(prefix), refl)

  /** Rebases all paths in `FileSystem` operations onto the given prefix. */
  def fileSystem[S[_]](
    prefix: ADir
  )(implicit
    S0: ReadFile :<: S,
    S1: WriteFile :<: S,
    S2: ManageFile :<: S,
    S3: QueryFile :<: S
  ): S ~> Free[S, ?] = {
    flatMapSNT(readFile[S](prefix))   compose
    flatMapSNT(writeFile[S](prefix))  compose
    flatMapSNT(manageFile[S](prefix)) compose
    queryFile[S](prefix)
  }

  /** Rebases all paths in `FileSystem` operations onto the given prefix. */
  def backendEffect[S[_]](
    prefix: ADir
  )(implicit
    S0: ReadFile :<: S,
    S1: WriteFile :<: S,
    S2: ManageFile :<: S,
    S3: QueryFile :<: S,
    S4: Analyze :<: S
  ): S ~> Free[S, ?] = {
    flatMapSNT(fileSystem[S](prefix)) compose analyze[S](prefix)
  }
}
