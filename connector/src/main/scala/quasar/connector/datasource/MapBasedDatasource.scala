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

package quasar.connector.datasource

import slamdata.Predef._
import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.{MonadResourceErr, PhysicalDatasource, ResourceError}

import scalaz.{Applicative, ApplicativePlus, ICons, IList, IMap, INil, ISet, Scalaz}
import Scalaz._

final class MapBasedDatasource[F[_]: Applicative: MonadResourceErr, G[_]: ApplicativePlus, R] private(
    val kind: DatasourceType,
    content: IMap[ResourcePath, F[R]])
    extends PhysicalDatasource[F, G, ResourcePath, R] {

  import ResourceError._

  def evaluate(rp: ResourcePath): F[R] =
    content.lookup(rp) getOrElse {
        MonadResourceErr[F].raiseError(
          if (prefixes.contains(rp)) notAResource(rp)
          else pathNotFound(rp))
    }

  def pathIsResource(path: ResourcePath): F[Boolean] =
    content.member(path).point[F]

  def prefixedChildPaths(prefixPath: ResourcePath)
      : F[Option[G[(ResourceName, ResourcePathType.Physical)]]] =
    prefixedChildPaths0(prefixPath).point[F]

  ////

  private val prefixes =
    content.keySet.foldLeft(ISet.empty[ResourcePath])
      { case (acc, key) =>
          acc union ancestors(key)
      }

  private def ancestors(p: ResourcePath): ISet[ResourcePath] = {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def go(ns: IList[ResourceName]): ISet[ResourcePath] = ns match {
      case INil() => ISet.singleton(ResourcePath.root())
      case l => ISet.singleton(ResourcePath.resourceNamesIso(l)) union go(l. dropRight(1))
    }

    go(ResourcePath.resourceNamesIso.get(p).dropRight(1))
  }

  private def children(s: ISet[ResourcePath], prefix: ResourcePath): ISet[ResourceName] =
    ISet.fromFoldable {
      s.toIList.flatMap(_.relativeTo(prefix).toIList).flatMap(p =>
        ResourcePath.resourceNamesIso.get(p) match {
          case ICons(h, INil()) => IList[ResourceName](h)
          case _ => INil[ResourceName]()
        })
    }

  private def prefixedChildPaths0(pfx: ResourcePath)
      : Option[G[(ResourceName, ResourcePathType.Physical)]] =
    if (!prefixes.contains(pfx))
      if (content.keySet.contains(pfx)) some(mempty[G, (ResourceName, ResourcePathType.Physical)])
      else None
    else {
      val childResources = children(content.keySet, pfx)
      val childPrefixes = children(prefixes, pfx)
      val both = childPrefixes intersection childResources

      val res = both.map((_, ResourcePathType.prefixResource)) union
        (childResources \\ both).map((_, ResourcePathType.leafResource)) union
        (childPrefixes \\ both).map((_, ResourcePathType.prefix))

      res.foldLeft(
        ApplicativePlus[G].empty[(ResourceName, ResourcePathType.Physical)]) {
        case (acc, p) =>
          ApplicativePlus[G].plus(acc, p.pure[G])
        }.some
    }
}

object MapBasedDatasource {
  def apply[F[_], G[_]]: PartiallyApplied[F, G] =
    new PartiallyApplied[F, G]

  final class PartiallyApplied[F[_], G[_]] {
    def apply[R](
        kind: DatasourceType, content: IMap[ResourcePath, F[R]])(
        implicit
        F0: Applicative[F],
        F1: MonadResourceErr[F],
        G0: ApplicativePlus[G])
        : PhysicalDatasource[F, G, ResourcePath, R] =
      new MapBasedDatasource[F, G, R](kind, content)
  }

  def pure[F[_], G[_]]: PartiallyAppliedPure[F, G] =
    new PartiallyAppliedPure[F, G]

  final class PartiallyAppliedPure[F[_], G[_]] {
    def apply[R](
        kind: DatasourceType, content: IMap[ResourcePath, R])(
        implicit
        F0: Applicative[F],
        F1: MonadResourceErr[F],
        G0: ApplicativePlus[G])
        : PhysicalDatasource[F, G, ResourcePath, R] =
      new MapBasedDatasource[F, G, R](kind, content.map(_.point[F]))
  }

}
