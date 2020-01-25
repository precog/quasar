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

import slamdata.Predef.Int

import quasar.Condition

import java.lang.RuntimeException

import scala.concurrent.ExecutionContext.Implicits.global

import cats.effect.IO

import scalaz.std.anyVal._

object DefaultDatasourceErrorsSpec extends quasar.EffectfulQSpec[IO] {

  "no errors by default" >>* {
    for {
      (errors, onChange) <- DefaultDatasourceErrors[IO, Int]

      initial <- errors.erroredDatasources

      initial89 <- errors.datasourceError(89)
    } yield {
      initial.size must_= 0
      initial89 must beNone
    }
  }

  "abnormal condition introduces error" >>* {
    for {
      (errors, onChange) <- DefaultDatasourceErrors[IO, Int]

      x = new RuntimeException("WHOOPS!")
      y = new RuntimeException("POW!")

      _ <- onChange(16, Condition.abnormal(x))
      _ <- onChange(19, Condition.abnormal(y))

      all <- errors.erroredDatasources

      one <- errors.datasourceError(16)
      two <- errors.datasourceError(19)
    } yield {
      all.size must_= 2
      one.exists(_ eq x) must beTrue
      two.exists(_ eq y) must beTrue
    }
  }

  "normal condition clears error" >>* {
    for {
      (errors, onChange) <- DefaultDatasourceErrors[IO, Int]

      ex = new RuntimeException("WHOOPS!")

      _ <- onChange(10, Condition.abnormal(ex))

      pre <- errors.datasourceError(10)

      _ <- onChange(10, Condition.normal())

      post <- errors.datasourceError(10)
    } yield {
      pre.exists(_ eq ex) must beTrue
      post must beNone
    }
  }
}
