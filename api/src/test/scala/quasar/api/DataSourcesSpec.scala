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

import slamdata.Predef.{Some, String}
import quasar.{Condition, ConditionMatchers, Qspec}

import scala.Predef.assert

import eu.timepit.refined.auto._
import org.specs2.execute.AsResult
import org.specs2.matcher.Matcher
import org.specs2.specification.core.Fragment
import scalaz.{\/, \/-, ~>, Equal, Id, Monad, Show}, Id.Id
import scalaz.syntax.equal._
import scalaz.syntax.monad._

abstract class DataSourcesSpec[F[_]: Monad, C: Equal: Show]
    extends Qspec
    with ConditionMatchers {

  import DataSourceError._

  def datasources: DataSources[F, C]

  def supportedType: DataSourceType

  // Must be distinct.
  def validConfigs: (C, C)

  def run: F ~> Id

  assert(configA =/= configB, "validConfigs must be distinct!")

  "add" >> {
    "lookup on success" >>* {
      for {
        cond <- datasources.add(
          foo,
          supportedType,
          configA,
          ConflictResolution.Preserve)

        r <- datasources.lookup(foo)
      } yield {
        cond must beNormal
        r must beConfigured(configA)
      }
    }

    "metadata on success" >>* {
      for {
        cond <- datasources.add(
          foo,
          supportedType,
          configA,
          ConflictResolution.Preserve)

        m <- datasources.metadata
      } yield {
        cond must beNormal

        m.lookup(foo) must beLike {
          case Some(DataSourceMetadata(t, Condition.Normal())) =>
            t must_= supportedType
        }
      }
    }

    "replace existing" >>* {
      for {
        afoo <- datasources.add(
          foo,
          supportedType,
          configA,
          ConflictResolution.Preserve)

        a <- datasources.lookup(foo)

        bfoo <- datasources.add(
          foo,
          supportedType,
          configB,
          ConflictResolution.Replace)

        b <- datasources.lookup(foo)
      } yield {
        a must beConfigured(configA)
        b must beConfigured(configB)
      }
    }

    "preserve existing" >>* {
      for {
        afoo <- datasources.add(
          foo,
          supportedType,
          configA,
          ConflictResolution.Preserve)

        a <- datasources.lookup(foo)

        bfoo <- datasources.add(
          foo,
          supportedType,
          configB,
          ConflictResolution.Preserve)

        b <- datasources.lookup(foo)
      } yield {
        a must beConfigured(configA)
        bfoo must beAbnormal(equal[Err](DataSourceExists(foo)))
        b must beConfigured(configA)
      }
    }

    "unsupported" >>* {
      val unsup = DataSourceType("--unsupported--", 17L)

      for {
        ubar <- datasources.add(
          bar,
          unsup,
          configB,
          ConflictResolution.Replace)

        types <- datasources.supported
      } yield {
        ubar must beAbnormal(equal[Err](DataSourceUnsupported(unsup, types)))
      }
    }
  }

  "lookup" >> {
    "error when not found" >>* {
      datasources.lookup(nope).map(_ must beNotFound(nope))
    }
  }

  "remove" >> {
    "error when not found" >>* {
      datasources.remove(nope)
        .map(_ must beAbnormal(equal[Err](DataSourceNotFound(nope))))
    }

    "lookup fails on success" >>* {
      for {
        afoo <- datasources.add(
          foo,
          supportedType,
          configA,
          ConflictResolution.Preserve)

        rfoo <- datasources.remove(foo)

        a <- datasources.lookup(foo)
      } yield {
        afoo must beNormal
        rfoo must beNormal
        a must beNotFound(foo)
      }
    }

    "not in metadata on success" >>* {
      for {
        afoo <- datasources.add(
          foo,
          supportedType,
          configA,
          ConflictResolution.Preserve)

        rfoo <- datasources.remove(foo)

        meta <- datasources.metadata
      } yield {
        afoo must beNormal
        rfoo must beNormal
        meta.member(foo) must beFalse
      }
    }
  }

  "rename" >> {
    "source nonexistent" >>* {
      datasources.rename(foo, bar, ConflictResolution.Preserve)
        .map(_ must beAbnormal(equal[Err](DataSourceNotFound(foo))))
    }

    "destination nonexistent" >>* {
      for {
        afoo <- datasources.add(
          foo,
          supportedType,
          configA,
          ConflictResolution.Preserve)

        rn <- datasources.rename(foo, bar, ConflictResolution.Preserve)

        x <- datasources.lookup(foo)
        y <- datasources.lookup(bar)
      } yield {
        afoo must beNormal
        rn must beNormal
        x must beNotFound(foo)
        y must beConfigured(configA)
      }
    }

    "replace existing destination" >>* {
      for {
        afoo <- datasources.add(
          foo,
          supportedType,
          configA,
          ConflictResolution.Preserve)

        bbar <- datasources.add(
          bar,
          supportedType,
          configB,
          ConflictResolution.Preserve)

        rn <- datasources.rename(foo, bar, ConflictResolution.Replace)

        a <- datasources.lookup(bar)
      } yield {
        afoo must beNormal
        bbar must beNormal
        rn must beNormal
        a must beConfigured(configA)
      }
    }

    "preserve existing destination" >>* {
      for {
        afoo <- datasources.add(
          foo,
          supportedType,
          configA,
          ConflictResolution.Preserve)

        bbar <- datasources.add(
          bar,
          supportedType,
          configB,
          ConflictResolution.Preserve)

        rn <- datasources.rename(foo, bar, ConflictResolution.Preserve)

        b <- datasources.lookup(bar)
      } yield {
        afoo must beNormal
        bbar must beNormal
        rn must beAbnormal(equal[Err](DataSourceExists(bar)))
        b must beConfigured(configB)
      }
    }

    "metadata reflects change" >>* {
      for {
        afoo <- datasources.add(
          foo,
          supportedType,
          configA,
          ConflictResolution.Preserve)

        rn <- datasources.rename(foo, bar, ConflictResolution.Preserve)

        meta <- datasources.metadata
      } yield {
        afoo must beNormal
        rn must beNormal
        meta.member(foo) must beFalse
        meta.member(bar) must beTrue
      }
    }
  }

  ////

  implicit class RunExample(s: String) {
    def >>*[A: AsResult](fa: => F[A]): Fragment =
      s >> run(fa)
  }

  type Err = DataSourceError[C]

  val foo = ResourceName("foo")
  val bar = ResourceName("bar")
  val nope = ResourceName("nope")

  def configA: C =
    validConfigs._1

  def configB: C =
    validConfigs._2

  def beConfigured(cfg: C): Matcher[CommonError \/ (DataSourceMetadata, C)] =
    beLike {
      case \/-((DataSourceMetadata(t, Condition.Normal()), c)) =>
        (t must_= supportedType) and (c must_= cfg)
    }

  def beNotFound[A](name: ResourceName): Matcher[CommonError \/ A] =
    be_-\/(equal[Err](DataSourceNotFound(name)))
}
