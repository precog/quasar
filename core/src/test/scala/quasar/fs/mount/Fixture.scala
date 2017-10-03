/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.fs.mount

import slamdata.Predef._

import quasar.contrib.pathy._
import quasar.effect.KeyValueStore
import quasar.fs.mount.cache.{VCache, ViewCache}
import quasar.fp._

import monocle.Lens
import scalaz._, Id._

object Fixture {

  def constant[F[_]: Applicative, K, V](m: Map[K, V]): KeyValueStore[K, V, ?] ~> F =
    KeyValueStore.impl.toState[State[Map[K, V], ?]](Lens.id[Map[K, V]]) andThen
    evalNT[Id, Map[K, V]](m) andThen pointNT[F]

  def runConstantVCache[F[_]: Applicative](vcache: Map[AFile, ViewCache]): VCache ~> F =
    constant[F, AFile, ViewCache](vcache)
}
