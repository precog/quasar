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

package quasar

import scala.{Array, Byte}

import quasar.contrib.scalaz.MonadError_

import java.lang.String

package object connector {
  type ByteStore[F[_]] = Store[F, String, Array[Byte]]

  type MonadResourceErr[F[_]] = MonadError_[F, ResourceError]

  def MonadResourceErr[F[_]](implicit ev: MonadResourceErr[F])
      : MonadResourceErr[F] = ev
}
