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

package quasar.impl

import slamdata.Predef._

import quasar.EffectfulQSpec

import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.instances.int._

import scala.concurrent.ExecutionContext

class CachedGetterSpec(implicit ec: ExecutionContext) extends EffectfulQSpec[IO] {
  import CachedGetter.Signal._
  "CachedGetter calculates current value based on two previous Options" >>* {
    for {
      ref <- Ref.of[IO, Option[Int]](None)
      func = (i: Unit) => ref.get
      getter <- CachedGetter(func)
      r0 <- getter(())
      _ <- ref.set(Some(1))
      r1 <- getter(())
      _ <- ref.set(Some(2))
      r2 <- getter(())
      r3 <- getter(())
      _ <- ref.set(None)
      r4 <- getter(())
      _ <- ref.set(None)
      r5 <- getter(())
      _ <- ref.set(Some(3))
      r6 <- getter(())
      _ <- ref.set(Some(3))
      r7 <- getter(())
    } yield {
      r0 === Empty
      r1 === Inserted(1)
      r2 === Updated(2, 1)
      r3 === Preserved(2)
      r4 === Removed(2)
      r5 === Empty
      r6 === Inserted(3)
      r7 === Preserved(3)
    }
  }
}
