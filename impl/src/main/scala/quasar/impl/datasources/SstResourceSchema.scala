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

package quasar.impl.datasources

import slamdata.Predef.Option

import quasar.ejson.EJson
import quasar.impl.schema._
import quasar.sst.SST

import scala.concurrent.duration.FiniteDuration

import cats.effect.{Concurrent, Timer}
import cats.syntax.functor._

import fs2.Stream

import matryoshka.{Corecursive, Recursive}

import scalaz.Order

import spire.algebra.Field
import spire.math.ConvertableTo

final class SstResourceSchema[
    F[_]: Concurrent,
    J: Order,
    N: ConvertableTo: Field: Order] private (
    evalConfig: SstEvalConfig)(
    implicit
    timer: Timer[F],
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson])
    extends ResourceSchema[F, SstConfig[J, N], Stream[F, SST[J, N]]] {

  def apply(
      sstConfig: SstConfig[J, N],
      ssts: Stream[F, SST[J, N]],
      timeLimit: FiniteDuration)
      : F[Option[sstConfig.Schema]] = {

    val k: N = ConvertableTo[N].fromLong(evalConfig.sampleSize.value)

    ssts
      .take(evalConfig.sampleSize.value)
      .chunkLimit(evalConfig.chunkSize.value.toInt)
      .through(ProgressiveSst.async(sstConfig, evalConfig.parallelism.value.toInt))
      .interruptWhen(Concurrent[F].attempt(timer.sleep(timeLimit)))
      .compile.last
      .map(_.map(SstSchema.fromSampled(k, _)))
  }
}

object SstResourceSchema {
  def apply[
    F[_]: Concurrent,
    J: Order,
    N: ConvertableTo: Field: Order](
    evalConfig: SstEvalConfig)(
    implicit
    timer: Timer[F],
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson])
    : ResourceSchema[F, SstConfig[J, N], Stream[F, SST[J, N]]] =
  new SstResourceSchema[F, J, N](evalConfig)
}
