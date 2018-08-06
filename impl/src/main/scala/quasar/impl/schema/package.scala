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

package quasar.impl

import slamdata.Predef._
import quasar.common.data.Data
import quasar.contrib.matryoshka._
import quasar.ejson.EJson
import quasar.contrib.iota.copkTraverse
import quasar.sst._

import fs2.Pipe
import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._
import scalaz.syntax.tag._
import spire.algebra.Field
import spire.math.ConvertableTo

package object schema {

  /** Reduces the input to an `SST` summarizing its structure. */
  def extractSst[F[_], J: Order, A: ConvertableTo: Field: Order](
      config: SstConfig[J, A])(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : Pipe[F, Data, SST[J, A]] = {

    type W[A] = Writer[Boolean @@ Tags.Disjunction, A]

    val thresholding: ElgotCoalgebra[SST[J, A] \/ ?, SSTF[J, A, ?], SST[J, A]] = {
      val independent =
        orOriginal(applyTransforms(
          compression.limitStrings[J, A](config.stringMaxLength)))

      compression.limitArrays[J, A](config.arrayMaxLength)
        .andThen(_.bimap(_.transAna[SST[J, A]](independent), independent))
    }

    val reduction: SSTF[J, A, SST[J, A]] => W[SSTF[J, A, SST[J, A]]] = {
      val f = applyTransforms(
        compression.coalesceWithUnknown[J, A](config.retainKeysSize),
        compression.coalesceKeys[J, A](config.mapMaxSize, config.retainKeysSize),
        compression.coalescePrimary[J, A],
        compression.narrowUnion[J, A](config.unionMaxSize))

      sstf => f(sstf).fold(sstf.point[W])(r => WriterT.writer((true.disjunction, r)))
    }

    def iterate(sst: SST[J, A]): Option[SST[J, A]] = {
      val (changed, compressed) =
        sst.transAnaM[W, SST[J, A], SSTF[J, A, ?]](reduction).run

      changed.unwrap option compressed
    }

    _.map(d => SST.fromData[J, A](Field[A].one, d).elgotApo[SST[J, A]](thresholding))
      .reduce((x, y) => repeatedly(iterate)(x |+| y))
  }
}
