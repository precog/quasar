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

package quasar.api

import slamdata.Predef.{???, None, Some}
import quasar.Condition
import scalaz.{IMap, ISet, Monad, \/}
import scalaz.syntax.either._
import quasar.contrib.scalaz.{MonadReader_, MonadState_}
import MockDataSources._
import quasar.api.DataSourceError.{CommonError, DataSourceNotFound}
import scalaz.syntax.monad._


final class MockDataSources[F[_]: Monad: DSMockState[?[_], C]: DSSupported, C, S] extends DataSources[F, C, S] {

  val store = MonadState_[F, IMap[ResourceName, \/[StaticDataSource, (ExternalMetadata, C)]]]

   def createExternal(name: ResourceName, kind: DataSourceType, config: C, onConflict: ConflictResolution): F[Condition[DataSourceError.ExternalError[C]]] = ???

   def createStatic(name: ResourceName, content: S, onConflict: ConflictResolution): F[Condition[DataSourceError.StaticError]] = ???



   def lookup(name: ResourceName): F[\/[DataSourceError.CommonError[C], \/[StaticDataSource, (ExternalMetadata, C)]]] =
     store.gets(m => m.lookup(name) match {
       case Some(a) => a.right
       case None    => DataSourceNotFound(name).left
     })

   def remove(name: ResourceName): F[Condition[DataSourceError.CommonError[C]]] =
     store.gets(x => x.updateLookupWithKey(name, (_, _) => None)).flatMap {
       case (Some(_), m) =>
         store.put(m).as(Condition.normal())
       case (None, _) =>
         Condition.abnormal[CommonError[C]](DataSourceNotFound[C](name)).point[F]
     }

   def rename(src: ResourceName, dst: ResourceName, onConflict: ConflictResolution): F[Condition[DataSourceError.CreateError[C]]] = ???

   def status: F[IMap[ResourceName, \/[StaticDataSource, ExternalMetadata]]] =
     store.gets(x => x.map {
       t => t.map(_._1)
     })

   def supportedExternal: F[ISet[DataSourceType]] =
     MonadReader_[F, ISet[DataSourceType]].ask

}

object MockDataSources {

  type DSMockState[F[_], C] = MonadState_[F, IMap[ResourceName, \/[StaticDataSource, (ExternalMetadata, C)]]]
  type DSSupported[F[_]] = MonadReader_[F, ISet[DataSourceType]]

  def apply[F[_], C, S]: MockDataSources[F, C, S] = ???

}


