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

package quasar.mimir.evaluate

import slamdata.Predef.Option
import quasar.contrib.pathy.AFile
import quasar.impl.evaluate.Source

import scalaz.Kleisli

object Config {
  type Associates[T[_[_]], F[_]] = AFile => Option[Source[QueryAssociate[T, F]]]
  type AssociatesT[T[_[_]], F[_], G[_], A] = Kleisli[F, EvaluatorConfig[T, G], A]

  final case class EvaluatorConfig[T[_[_]], F[_]](
    associates: Associates[T, F],
    pushdown: Pushdown)
}
