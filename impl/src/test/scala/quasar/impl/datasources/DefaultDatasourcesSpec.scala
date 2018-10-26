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

import slamdata.Predef.{Stream => _, _}
import quasar.Condition
import quasar.api.MockSchemaConfig
import quasar.api.datasource._
import quasar.api.datasource.DatasourceError._
import quasar.contrib.cats.stateT._
import quasar.contrib.cats.writerT._
import quasar.contrib.cats.effect.stateT.catsStateTEffect
import quasar.contrib.fs2.stream._
import quasar.contrib.scalaz.MonadState_
import quasar.impl.storage.PureIndexedStore
import DefaultDatasourcesSpec._

import java.io.IOException
import scala.concurrent.ExecutionContext.Implicits.global

import cats.data.{StateT, WriterT}
import cats.effect.IO
import cats.instances.list._
import cats.syntax.applicative._
import cats.syntax.flatMap._
import eu.timepit.refined.auto._
import fs2.Stream
import monocle.macros.Lenses
import scalaz.{-\/, \/-, IMap, ISet, Monoid}
import scalaz.std.anyVal._
import scalaz.std.string._
import shims._

final class DefaultDatasourcesSpec
    extends DatasourcesSpec[DefaultM, Stream[DefaultM, ?], Int, String, MockSchemaConfig.type] {

  val monadIdx: MonadState_[DefaultM, Int] =
    MonadState_.zoom[DefaultM](DefaultState.idx)

  implicit val monadInitd: MonadState_[DefaultM, ISet[Int]] =
    MonadState_.zoom[DefaultM](DefaultState.initd)

  implicit val monadRefs: MonadState_[DefaultM, Refs] =
    MonadState_.zoom[DefaultM](DefaultState.refs)

  def datasources = mkDatasources(IMap.empty, c => c)(_ => None)
  def sanitizedDatasources = mkDatasources(IMap.empty, _ => "sanitized")(_ => None)

  def supportedType = DatasourceType("test-type", 3L)

  def validConfigs = ("one", "two")

  val schemaConfig = MockSchemaConfig

  def gatherMultiple[A](as: Stream[DefaultM, A]) = as.compile.toList

  def mkDatasources(
      errs: IMap[Int, Exception], sanitize: String => String)(
      init: String => Option[InitializationError[String]])
      : Datasources[DefaultM, Stream[DefaultM, ?], Int, String, MockSchemaConfig.type] = {

    val freshId =
      for {
        i <- monadIdx.get
        _ <- monadIdx.put(i + 1)
      } yield i

    val refs =
      PureIndexedStore[DefaultM, Int, DatasourceRef[String]]

    val errors =
      DatasourceErrors.fromMap(errs.pure[DefaultM])

    val control =
      MockDatasourceControl[DefaultM, Stream[DefaultM, ?], Int, String](
        ISet.singleton(supportedType),
        init, sanitize)

    DefaultDatasources(freshId, refs, errors, control)
  }

  "implementation specific" >> {
    "add datasource" >> {
      "initializes datasource" >> {
        val addB =
          refB >>= datasources.addDatasource

        addB.runEmpty.value.map(_ must beLike {
          case (s, \/-(i)) => s.initd.member(i) must beTrue
        }).unsafeRunSync()
      }

      "doesn't store config when initialization fails" >> {
        val err3 =
          MalformedConfiguration(supportedType, "three", "3 isn't a config!")

        val ds = mkDatasources(IMap.empty, _ => "") {
          case "three" => Some(err3)
          case _ => None
        }

        val add =
          refA
            .map(DatasourceRef.config.set("three"))
            .flatMap(ds.addDatasource)

        add.runEmpty.value.map(_ must beLike {
          case (s, -\/(e)) =>
            s.refs.isEmpty must beTrue
            s.initd.isEmpty must beTrue
            (e: DatasourceError[Int, String]) must_= err3
        }).unsafeRunSync()
      }
    }

    "lookup status" >> {
      "includes errors" >> {
        val errs =
          IMap[Int, Exception](1 -> new IOException())

        val ds = mkDatasources(errs, _ => "")(_ => None)

        val lbar = for {
          a <- refA
          _ <- ds.addDatasource(a)

          b <- refB
          _ <- ds.addDatasource(b)

          r <- ds.datasourceStatus(1)
        } yield r

        lbar.runEmptyA.value.map(_ must beLike {
          case \/-(Condition.Abnormal(ex)) => ex must beAnInstanceOf[IOException]
        }).unsafeRunSync()
      }
    }

    "all metadata" >> {
      "includes errors" >> {
        val errs =
          IMap[Int, Exception](0 -> new IOException())

        val ds = mkDatasources(errs, _ => "")(_ => None)

        val lbar = for {
          a <- refA
          _ <- ds.addDatasource(a)

          b <- refB
          _ <- ds.addDatasource(b)

          g <- ds.allDatasourceMetadata
          l <- gatherMultiple(g)
          m = IMap.fromList(l)
        } yield m

        lbar.runEmptyA.value.map(_.lookup(0) must beLike {
          case Some(DatasourceMeta(t, _, Condition.Abnormal(ex))) =>
            (t must_= supportedType) and (ex must beAnInstanceOf[IOException])
        }).unsafeRunSync()
      }
    }

    "remove datasource" >> {
      "shutdown existing" >> {
        val sdown = for {
          a <- refA
          _ <- datasources.addDatasource(a)
          cond <- datasources.removeDatasource(0)
        } yield cond

        sdown.runEmpty.run.map(_ must beLike {
          case (sdowns, (s, Condition.Normal())) =>
            s.initd must_= ISet.empty
            sdowns must_= List(0)
        }).unsafeRunSync()
      }
    }

    "replace datasource" >> {
      "updates control" >> {
        val replaced = for {
          a <- refA
          b <- refB

          r <- datasources.addDatasource(a)
          i = r.toOption.get

          c <- datasources.replaceDatasource(i, b)
        } yield c

        replaced.runEmpty.run.map(_ must beLike {
          case (sdowns, (s, Condition.Normal())) =>
            s.initd must_= ISet.singleton(0)
            sdowns must_= List(0)
        }).unsafeRunSync()
      }

      "doesn't update control when only name changed" >> {
        val renamed = for {
          a <- refA
          n <- randomName

          b = DatasourceRef.name.set(n)(a)

          r <- datasources.addDatasource(a)
          i = r.toOption.get

          c <- datasources.replaceDatasource(i, b)
        } yield c

        renamed.runEmpty.run.map(_ must beLike {
          case (sdowns, (s, Condition.Normal())) =>
            s.initd must_= ISet.singleton(0)
            sdowns must_= Nil
        }).unsafeRunSync()
      }
    }

    "sanitize config" >> {
      "ref is sanitized" >> {
        val ds = for {
          a <- refA
          _ <- sanitizedDatasources.addDatasource(a)
          l <- sanitizedDatasources.datasourceRef(0)
        } yield l

        ds.runEmpty.run.map(x => x must beLike {
          case (_, (_, \/-(ref))) => {
            ref.config must_= "sanitized"
          }
        }).unsafeRunSync()
      }
    }

  }
}

object DefaultDatasourcesSpec {
  import MockDatasourceControl.{Initialized, Shutdowns}

  type Refs = IMap[Int, DatasourceRef[String]]
  type DefaultM[A] = StateT[WriterT[IO, Shutdowns[Int], ?], DefaultState, A]

  @Lenses
  final case class DefaultState(idx: Int, initd: Initialized[Int], refs: Refs)

  object DefaultState {
    implicit val monoid: Monoid[DefaultState] =
      new Monoid[DefaultState] {
        val zero = DefaultState(0, ISet.empty, IMap.empty)

        def append(x: DefaultState, y: => DefaultState) =
          DefaultState(
            x.idx + y.idx,
            x.initd union y.initd,
            x.refs union y.refs)
      }
  }
}
