/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.impl.datasource

import slamdata.Predef._
import quasar.api.datasource.DatasourceType
import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.connector.{Datasource, MonadResourceErr, ResourceError}
import quasar.contrib.scalaz._

import scala.util.{Left, Right}

import cats.effect.Sync
import cats.instances.option._
import cats.syntax.applicative._
import cats.syntax.eq._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.option._
import cats.syntax.traverse._

import fs2.{Pull, Stream}

import monocle.Lens

import shims.{equalToCats, monadToScalaz}

/** A datasource transformer that augments underlying datasources by adding an aggregate resource
  * `p / **` for every prefix `p`. An aggregate resource `p / **` will aggregate
  * all descendant resources of the prefix `p`.
  */
final class AggregatingDatasource[F[_]: MonadResourceErr: Sync, Q, R] private(
    underlying: Datasource[F, Stream[F, ?], Q, R, ResourcePathType.Physical],
    queryPath: Lens[Q, ResourcePath])
    extends Datasource[F, Stream[F, ?], Q, CompositeResult[F, R], ResourcePathType] {

  def kind: DatasourceType =
    underlying.kind

  def evaluate(q: Q): F[CompositeResult[F, R]] = {

    def aggregate(p: ResourcePath): F[AggregateResult[F, R]] =
      aggPath(p).map(doAggregate).getOrElse(MonadResourceErr[F].raiseError(ResourceError.pathNotFound(p)))

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def doAggregate(p: ResourcePath): F[AggregateResult[F, R]] =
      underlying.prefixedChildPaths(p) flatMap {
        case Some(s) =>
          val children: AggregateResult[F, R] =
            s.collect {
              case (n, ResourcePathType.LeafResource) => p / n
              case (n, ResourcePathType.PrefixResource) => p / n
            } .evalMap(r => underlying.evaluate(queryPath.set(r)(q)).tupleLeft(r).attempt)
              .map(_.toOption)
              .unNone

          val nested: AggregateResult[F, R] =
            s.collect {
              case (n, ResourcePathType.Prefix) => p / n
              case (n, ResourcePathType.PrefixResource) => p / n
            } .evalMap(doAggregate).flatten

          (children ++ nested).pure[F]

        case None =>
          MonadResourceErr[F].raiseError(ResourceError.pathNotFound(p))
      }

    val qpath = queryPath.get(q)

    underlying.pathIsResource(qpath).ifM(
      underlying.evaluate(q).map(Left(_)),
      aggregate(qpath).map(Right(_)))
  }

  def pathIsResource(path: ResourcePath): F[Boolean] =
    underlying.pathIsResource(path).ifM(true.pure[F], aggPathExists(path))

  def prefixedChildPaths(prefixPath: ResourcePath)
      : F[Option[Stream[F, (ResourceName, ResourcePathType)]]] =
    aggPath(prefixPath) match {
      case None =>
        underlying.prefixedChildPaths(prefixPath)
          .flatMap(os => ofPrefix(os).ifM(
            os.map(s => Stream.emit((AggName, ResourcePathType.AggregateResource)) ++ s).pure[F],
            os.map(_.covaryOutput[(ResourceName, ResourcePathType)]).pure[F]))
      case Some(_) =>
        pathIsResource(prefixPath).ifM(
          Stream.empty.covary[F].covaryOutput[(ResourceName, ResourcePathType)].some.pure[F],
          none.pure[F])
    }

  ////

  private val AggName: ResourceName = ResourceName("**")

  private def aggPath(path: ResourcePath): Option[ResourcePath] =
    path.unsnoc.flatMap {
      case (p, n) => if (n === AggName) Some(p) else None
    }

  // `p / *` exists iff underlying `p` is prefix/prefixresource
  private def aggPathExists(path: ResourcePath): F[Boolean] =
    aggPath(path)
      .map(p => underlying.prefixedChildPaths(p).flatMap(ofPrefix))
      .getOrElse(false.pure[F])

  // checks whether the provided stream is that of a prefix/prefixresource
  private def ofPrefix[A](os: Option[Stream[F, A]]): F[Boolean] =
    os.traverse(s => s.pull.peek1.flatMap {
      case None => Pull.output1(false)
      case _ => Pull.output1(true)
    }.stream.compile.last).map(_.flatten getOrElse false)
}

object AggregatingDatasource {
  def apply[F[_]: MonadResourceErr: Sync, Q, R](
      underlying: Datasource[F, Stream[F, ?], Q, R, ResourcePathType.Physical],
      queryPath: Lens[Q, ResourcePath])
      : Datasource[F, Stream[F, ?], Q, CompositeResult[F, R], ResourcePathType] =
    new AggregatingDatasource(underlying, queryPath)
}
