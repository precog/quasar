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

package quasar.evaluate

import slamdata.Predef.Option
import quasar.contrib.pathy.AFile
import quasar.qscript.QScriptRead

import monocle.macros.Lenses
import scalaz.{Functor, Show}

/** A QScript query over possibly many sources.
  *
  * NB: This encoding exists due to the cost incurred when extending
  *     `QScriptTotal`, both in compilation time and boilerplate.
  *
  *     If we're ever able to get rid of `QScriptTotal`, we could just use
  *     a variant of QScript containing `Const[Read[Source[S]], ?]` instead.
  */
@Lenses
final case class FederatedQuery[T[_[_]], S](
    query: T[QScriptRead[T, ?]],
    sources: AFile => Option[Source[S]])

object FederatedQuery extends FederatedQueryInstances

sealed abstract class FederatedQueryInstances {
  implicit def functor[T[_[_]]]: Functor[FederatedQuery[T, ?]] =
    new Functor[FederatedQuery[T, ?]] {
      def map[A, B](fa: FederatedQuery[T, A])(f: A => B) =
        FederatedQuery(fa.query, p => fa.sources(p).map(_.map(f)))
    }

  implicit def show[T[_[_]], A]: Show[FederatedQuery[T, A]] =
    Show.shows(_ => "FederatedQuery")
}
