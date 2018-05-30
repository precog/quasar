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

package quasar.impl.datasources

import slamdata.Predef.Unit
import quasar.Condition
import quasar.api._
import quasar.api.DataSourceError._

import scalaz.{\/, EitherT, IMap, ISet, Liskov, Monad, OptionT}, Liskov.<~<
import scalaz.syntax.equal._
import scalaz.syntax.monad._

final class DefaultDataSources[F[_]: Monad, C] private (
    configs: DataSourceConfigs[F, C],
    errors: DataSourceErrors[F],
    control: DataSourceControl[F, C])
    extends DataSources[F, C] {

  def add(
      name: ResourceName,
      kind: DataSourceType,
      config: C,
      onConflict: ConflictResolution)
      : F[Condition[CreateError[C]]] = {

    val dataSourceConfig =
      DataSourceConfig(kind, config)

    lookupConfig(name) flatMap { r =>
      if (r.isRight && onConflict === ConflictResolution.Preserve)
        Condition.abnormal[CreateError[C]](DataSourceExists(name)).point[F]
      else
        control.init(name, dataSourceConfig) flatMap {
          case Condition.Abnormal(e) =>
            Condition.abnormal[CreateError[C]](e).point[F]

          case Condition.Normal() =>
            configs.add(name, dataSourceConfig)
              .as(Condition.normal[CreateError[C]]())
        }
    }
  }

  def lookup(name: ResourceName): F[CommonError \/ (DataSourceMetadata, C)] = {
    val toResult: DataSourceConfig[C] => F[(DataSourceMetadata, C)] = {
      case DataSourceConfig(t, c) =>
        errors.lookup(name)
          .map(DataSourceMetadata.fromOption(t, _))
          .strengthR(c)
    }

    EitherT(lookupConfig(name))
      .flatMap(cfg => EitherT.rightT(toResult(cfg)))
      .run
  }

  def metadata: F[IMap[ResourceName, DataSourceMetadata]] =
    (configs.configured |@| errors.errored) {
      case (cfgs, errs) =>
        cfgs.mapWithKey { (n, t) =>
          DataSourceMetadata.fromOption(t, errs.lookup(n))
        }
    }

  def remove(name: ResourceName): F[Condition[CommonError]] =
    configs.remove(name).ifM(
      control.shutdown(name).as(Condition.normal[CommonError]()),
      Condition.abnormal[CommonError](DataSourceNotFound(name)).point[F])

  def rename(
      src: ResourceName,
      dst: ResourceName,
      onConflict: ConflictResolution)
      : F[Condition[ExistentialError]] = {

    val renamed: EitherT[F, ExistentialError, Unit] = for {
      rsrc <- EitherT(lookupConfig(src)).leftMap(commonIsExistential)

      rdst <- EitherT.rightT(lookupConfig(dst))

      _ <- if (rdst.isRight && onConflict === ConflictResolution.Preserve)
        EitherT.pureLeft[F, ExistentialError, Unit](DataSourceExists(dst))
      else
        EitherT.rightT[F, ExistentialError, Unit](
          configs.rename(src, dst) >> control.rename(src, dst))

    } yield ()

    renamed.run.map(Condition.disjunctionIso.reverseGet(_))
  }

  def supported: F[ISet[DataSourceType]] =
    control.supported

  ////

  private val commonIsExistential: CommonError <~< ExistentialError =
    Liskov.isa[CommonError, ExistentialError]

  private def lookupConfig(name: ResourceName)
      : F[CommonError \/ DataSourceConfig[C]] =
    OptionT(configs.lookup(name))
      .toRight(DataSourceNotFound(name): CommonError)
      .run
}

object DefaultDataSources {
  def apply[F[_]: Monad, C](
      configs: DataSourceConfigs[F, C],
      errors: DataSourceErrors[F],
      control: DataSourceControl[F, C])
      : DataSources[F, C] =
    new DefaultDataSources(configs, errors, control)
}
