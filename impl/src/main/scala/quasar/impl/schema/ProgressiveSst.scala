/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.impl.schema

import slamdata.Predef._
import quasar.ejson.EJson
import quasar.contrib.iota.copkTraverse
import quasar.sst._

import cats.effect.Concurrent
import fs2.{Chunk, Pipe}
import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._
import spire.algebra.Field
import spire.math.ConvertableTo

object ProgressiveSst {

  /** Merges input chunks into increasingly accurate SSTs. */
  def apply[F[_], J: Order, A: ConvertableTo: Field: Order](
      config: SstConfig[J, A])(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : Pipe[F, Chunk[SST[J, A]], SST[J, A]] =
    progressiveSst0[F, J, A](config)(f => _.map(f))

  /** Merges input chunks, in parallel, into increasingly accurate SSTs. */
  def async[F[_]: Concurrent, J: Order, A: ConvertableTo: Field: Order](
      config: SstConfig[J, A],
      paralellism: Int)(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : Pipe[F, Chunk[SST[J, A]], SST[J, A]] =
    progressiveSst0[F, J, A](config)(f => _.mapAsyncUnordered(paralellism)(c => Concurrent[F].delay(f(c))))

  ////

  private def progressiveSst0[F[_], J: Order, A: ConvertableTo: Field: Order](
      config: SstConfig[J, A])(
      f: (Chunk[SST[J, A]] => Option[SST[J, A]]) => Pipe[F, Chunk[SST[J, A]], Option[SST[J, A]]])(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : Pipe[F, Chunk[SST[J, A]], SST[J, A]] = {

    val reduction: SSTF[J, A, SST[J, A]] => Option[SSTF[J, A, SST[J, A]]] = {
      val coalesceKeysWidened =
        compression.coalesceKeysWidened[J, A](
          config.mapMaxSize,
          config.retainKeysSize,
          config.stringPreserveStructure)

      val coalesceWhenUnknown =
        compression.coalesceWhenUnknown[J, A](
          config.retainKeysSize,
          config.stringPreserveStructure)

      sstf => coalesceWhenUnknown(sstf) orElse coalesceKeysWidened(sstf)
    }

    def reduce(sst: SST[J, A]): SST[J, A] =
      sst.transAna[SST[J, A]](orOriginal(reduction))

    @SuppressWarnings(Array(
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Var",
      "org.wartremover.warts.While"))
    def reduceChunk(c: Chunk[SST[J, A]]): Option[SST[J, A]] =
      if (c.isEmpty) none
      else if (c.size == 1) some(c(0))
      else {
        var i = 1
        var acc = c(0)
        while (i < c.size) {
          acc = reduce(acc |+| c(i))
          i = i + 1
        }
        some(reduce(acc))
      }

    _.through(f(reduceChunk))
      .unNone
      .scan1((x, y) => reduce(x |+| y))
  }
}
