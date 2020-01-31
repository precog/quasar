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
import cats.syntax.applicative._

import scala.concurrent.ExecutionContext

class ResourceManagerSpec(implicit ec: ExecutionContext) extends EffectfulQSpec[IO] {
  "ResourceManager works" >>* {
    ResourceManager[IO, Int, String].use { mgr =>
      for {
        r0 <- mgr.get(0)
        _ <- mgr.manage(0, ("foo", ().pure[IO]))
        r1 <- mgr.get(0)
        _ <- mgr.shutdown(0)
        r2 <- mgr.get(0)
      } yield {
        r0 must beNone
        r1 must beSome("foo")
        r2 must beNone
      }
    }
  }
  "finalizer is called on shutdown" >>* {
    for {
      ref <- Ref.of[IO, List[Int]](List.empty)
      (mgr, finalizer) <- ResourceManager[IO, Int, String].allocated
      _ <- mgr.manage(0, ("whatever", ref.update(0 :: _)))
      r0 <- mgr.get(0)
      d0 <- ref.get
      _ <- mgr.shutdown(0)
      r1 <- mgr.get(0)
      d1 <- ref.get
      _ <- finalizer
    } yield {
      r0 must beSome
      r1 must beNone
      d0 === List()
      d1 === List(0)
    }
  }
  "finalizers are called on manager deallocation" >>* {
    for {
      ref <- Ref.of[IO, List[Int]](List.empty)
      (mgr, finalizer) <- ResourceManager[IO, Int, String].allocated
      _ <- mgr.manage(0, ("a", ref.update(0 :: _)))
      _ <- mgr.manage(1, ("b", ref.update(1 :: _)))
      _ <- mgr.manage(2, ("c", ref.update(2 :: _)))
      _ <- finalizer
      ds <- ref.get
    } yield {
      ds === List(2, 1, 0)
    }
  }
  "overwritting calls previous finalizer" >>* {
    for {
      ref <- Ref.of[IO, List[Int]](List.empty)
      (mgr, finalizer) <- ResourceManager[IO, Int, String].allocated
      _ <- mgr.manage(0, ("whatever", ref.update(0 :: _)))
      d0 <- ref.get
      r0 <- mgr.get(0)
      _ <- mgr.manage(0, ("whatever else", ref.update(1 :: _)))
      d1 <- ref.get
      r1 <- mgr.get(0)
      _ <- finalizer
      d2 <- ref.get
    } yield {
      r0 must beSome("whatever")
      r1 must beSome("whatever else")
      d0 === List()
      d1 === List(0)
      d2 === List(1, 0)
    }
  }
}
