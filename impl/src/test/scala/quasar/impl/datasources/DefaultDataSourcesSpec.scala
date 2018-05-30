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

import slamdata.Predef._
import quasar.Condition
import quasar.api._
import quasar.api.DataSourceError._
import DefaultDataSourcesSpec.DefaultM

import java.io.IOException

import eu.timepit.refined.auto._
import scalaz.{~>, Id, IMap, ISet, StateT, Writer}, Id.Id
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.syntax.monad._

final class DefaultDataSourcesSpec extends DataSourcesSpec[DefaultM, Int] {
  def datasources = mkDataSources(IMap.empty)(_ => None)

  def supportedType = DataSourceType("test-type", 3L)

  def validConfigs = (5, 7)

  def run =
    λ[DefaultM ~> Id](_.eval(IMap.empty).eval(ISet.empty).value)

  def mkDataSources(
      errs: IMap[ResourceName, Exception])(
      init: Int => Option[InitializationError[Int]])
      : DataSources[DefaultM, Int] = {

    val configs =
      MockDataSourceConfigs[Int, DefaultM]

    val errors =
      DataSourceErrors.fromMap(errs.point[DefaultM])

    val control =
      MockDataSourceControl[DefaultM, Int](
        ISet.singleton(supportedType),
        init)

    DefaultDataSources(configs, errors, control)
  }

  "implementation specific" >> {
    "add" >> {
      "initializes datasource" >> {
        val addfoo = datasources.add(
          foo,
          supportedType,
          configB,
          ConflictResolution.Preserve)

        addfoo.eval(IMap.empty).run(ISet.empty).value match {
          case (inits, cond) =>
            inits.member(foo) must beTrue
            cond must beNormal
        }
      }

      "doesn't store config when initialization fails" >> {
        val err3 =
          MalformedConfiguration(supportedType, 3, "3 isn't a config!")

        val ds = mkDataSources(IMap.empty) {
          case 3 => Some(err3)
          case _ => None
        }

        val addfoo = ds.add(
          foo,
          supportedType,
          3,
          ConflictResolution.Preserve)

        addfoo.run(IMap.empty).run(ISet.empty).value match {
          case (inits, (cfgs, cond)) =>
            cfgs.member(foo) must beFalse
            inits.member(foo) must beFalse
            cond must beAbnormal(equal[DataSourceError[Int]](err3))
        }
      }
    }

    "lookup" >> {
      "includes errors" >> {
        val errs =
          IMap[ResourceName, Exception](bar -> new IOException())

        val ds = mkDataSources(errs)(_ => None)

        val lbar = for {
          _ <- ds.add(foo, supportedType, 7, ConflictResolution.Preserve)
          _ <- ds.add(bar, supportedType, 8, ConflictResolution.Preserve)
          b <- ds.lookup(bar)
        } yield b

        lbar.eval(IMap.empty).eval(ISet.empty).value must be_\/-.like {
          case (DataSourceMetadata(t, Condition.Abnormal(ex)), 8) =>
            (t must_= supportedType) and (ex must beAnInstanceOf[IOException])
        }
      }
    }

    "metadata" >> {
      "includes errors" >> {
        val errs =
          IMap[ResourceName, Exception](foo -> new IOException())

        val ds = mkDataSources(errs)(_ => None)

        val lbar = for {
          _ <- ds.add(foo, supportedType, 7, ConflictResolution.Preserve)
          _ <- ds.add(bar, supportedType, 8, ConflictResolution.Preserve)
          m <- ds.metadata
        } yield m

        lbar.eval(IMap.empty).eval(ISet.empty).value.lookup(foo) must beLike {
          case Some(DataSourceMetadata(t, Condition.Abnormal(ex))) =>
            (t must_= supportedType) and (ex must beAnInstanceOf[IOException])
        }
      }
    }

    "remove" >> {
      "shutdown existing" >> {
        val sdown = for {
          _ <- datasources.add(foo, supportedType, 7, ConflictResolution.Preserve)
          cond <- datasources.remove(foo)
        } yield cond

        sdown.eval(IMap.empty).run(ISet.empty).run must beLike {
          case (sdowns, (inits, Condition.Normal())) =>
            inits must_= ISet.empty
            sdowns must_= List(foo)
        }
      }
    }

    "rename" >> {
      "updates control" >> {
        val rn = for {
          _ <- datasources.add(foo, supportedType, 5, ConflictResolution.Preserve)
          _ <- datasources.add(bar, supportedType, 7, ConflictResolution.Preserve)
          r <- datasources.rename(foo, bar, ConflictResolution.Replace)
        } yield r

        rn.eval(IMap.empty).run(ISet.empty).run must beLike {
          case (sdowns, (inits, Condition.Normal())) =>
            inits must_= ISet.singleton(bar)
            sdowns must_= List(bar)
        }
      }
    }
  }
}

object DefaultDataSourcesSpec {
  import MockDataSourceConfigs.Configs
  import MockDataSourceControl.{Initialized, Shutdowns}

  type DefaultM[A] = StateT[StateT[Writer[Shutdowns, ?], Initialized, ?], Configs[Int], A]
}
