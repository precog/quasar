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

object mounted {

  /** Strips the `mountPoint` off of all input paths in `ReadFile` operations
    * and restores it on output paths.
    */
   def readFile[S[_]](mountPoint: ADir)(implicit S: ReadFile :<: S): S ~> Free[S, ?] =
     transformPaths.readFile[S](stripPrefixA(mountPoint), rebaseA(mountPoint))

  /** Strips the `mountPoint` off of all input paths in `WriteFile` operations
    * and restores it on output paths.
    */
  def writeFile[S[_]](mountPoint: ADir)(implicit S: WriteFile :<: S): S ~> Free[S, ?] =
    transformPaths.writeFile[S](stripPrefixA(mountPoint), rebaseA(mountPoint))

  /** Strips the `mountPoint` off of all input paths in `ManageFile` operations
    * and restores it on output paths.
    */
  def manageFile[S[_]](mountPoint: ADir)(implicit S: ManageFile :<: S): S ~> Free[S, ?] =
    transformPaths.manageFile[S](stripPrefixA(mountPoint), rebaseA(mountPoint))

  /** Strips the `mountPoint` off of all input paths in `QueryFile` operations
    * and restores it on output paths.
    */
  def queryFile[S[_]](mountPoint: ADir)(implicit S: QueryFile :<: S): S ~> Free[S, ?] =
    transformPaths.queryFile[S](stripPrefixA(mountPoint), rebaseA(mountPoint), refl)

  def analyze[S[_]](mountPoint: ADir)(implicit S: Analyze :<: S): S ~> Free[S, ?] =
    transformPaths.analyze[S](stripPrefixA(mountPoint), rebaseA(mountPoint), refl)


  def fileSystem[S[_]](
    mountPoint: ADir
  )(implicit
    S0: ReadFile :<: S,
    S1: WriteFile :<: S,
    S2: ManageFile :<: S,
    S3: QueryFile :<: S
  ): S ~> Free[S, ?] = {
    flatMapSNT(readFile[S](mountPoint))   compose
    flatMapSNT(writeFile[S](mountPoint))  compose
    flatMapSNT(manageFile[S](mountPoint)) compose
    queryFile[S](mountPoint)
  }
}
