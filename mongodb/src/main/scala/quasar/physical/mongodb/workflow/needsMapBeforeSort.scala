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

package quasar.physical.mongodb.workflow

import slamdata.Predef._

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._

object needsMapBeforeSort {

  def apply[T[_[_]]: BirecursiveT, F[_]: Traverse](wf: T[F])
    (implicit I: WorkflowOpCoreF :<: F)
      : Boolean = {
    val alg: AlgebraM[Option, F, T[F]] = {
      case I($SimpleMapF(Embed(I($SortF(_, _))), _, _)) => none
      case x => x.embed.some
    }
    wf.cataM(alg).isEmpty
  }
}
