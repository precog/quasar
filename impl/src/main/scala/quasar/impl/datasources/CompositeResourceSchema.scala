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

import slamdata.Predef.{Double, List, Option, String}

import quasar.api.resource.ResourcePath
import quasar.connector.{MonadResourceErr, QueryResult}
import quasar.ejson.EJson
import quasar.impl.datasource.CompositeResult
import quasar.impl.parsing.ResourceParser
import quasar.impl.schema._
import quasar.sst.SST

import scala.Predef.ArrowAssoc
import scala.concurrent.duration.FiniteDuration
import scala.util.{Left, Right}

import cats.effect.{Concurrent, Timer}

import matryoshka.{Corecursive, Recursive}

import pathy.Path.posixCodec

import qdata.QDataEncode

import scalaz.Order

import spire.algebra.Field
import spire.math.ConvertableTo

final class CompositeResourceSchema[
    F[_]: Concurrent: MonadResourceErr,
    J: Order,
    N: ConvertableTo: Field: Order] private (
    evalConfig: SstEvalConfig)(
    implicit
    timer: Timer[F],
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson])
    extends ResourceSchema[F, SstConfig[J, N], (ResourcePath, CompositeResult[F, QueryResult[F]])] {

  import CompositeResourceSchema.{ComponentSampleFactor, SourceKey, ValueKey}

  private val sstResourceSchema = SstResourceSchema[F, J, N](evalConfig)

  def apply(
      sstConfig: SstConfig[J, N],
      resource: (ResourcePath, CompositeResult[F, QueryResult[F]]),
      timeLimit: FiniteDuration)
      : F[Option[sstConfig.Schema]] = {

    type S = SST[J, N]

    implicit val sstQDataEncode: QDataEncode[S] =
      QDataCompressedSst.encode[J, N](sstConfig)

    // Max number of rows to sample from each component of the aggregate
    val componentSampleSize =
      (evalConfig.sampleSize.value * ComponentSampleFactor).toLong

    // Number of component streams to open concurrently, assumed not to be
    // compute bound
    val readParallelism = evalConfig.parallelism.value.toInt * 2

    def attributeSource(rp: ResourcePath)(valSst: S): S = {
      val srcSst = sstQDataEncode.makeString(posixCodec.printPath(rp.toPath))

      val assocs = List(SourceKey -> srcSst, ValueKey -> valSst)

      sstQDataEncode.makeObject(
        assocs.foldLeft(sstQDataEncode.prepObject) {
          case (o, (k, v)) => sstQDataEncode.pushObject(k, v, o)
        })
    }

    val ssts = resource match {
      case (rp, Left(qr)) =>
        ResourceParser[F, S](rp, qr)

      case (_, Right(components)) =>
        val compSsts =
          components map { case (rp, qr) =>
            ResourceParser[F, S](rp, qr)
              .map(attributeSource(rp))
              .take(componentSampleSize)
          }

        compSsts.parJoin(readParallelism)
    }

    sstResourceSchema(sstConfig, ssts, timeLimit)
  }
}

object CompositeResourceSchema {
  // Each component of the aggregate will be sampled this factor of the total
  // sample size.
  val ComponentSampleFactor: Double = 0.1

  val SourceKey: String = "source"
  val ValueKey: String = "value"

  def apply[
      F[_]: Concurrent: MonadResourceErr,
      J: Order,
      N: ConvertableTo: Field: Order](
      evalConfig: SstEvalConfig)(
      implicit
      timer: Timer[F],
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : ResourceSchema[F, SstConfig[J, N], (ResourcePath, CompositeResult[F, QueryResult[F]])] =
    new CompositeResourceSchema[F, J, N](evalConfig)
}
