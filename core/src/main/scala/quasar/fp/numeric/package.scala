/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.fp

import quasar.Predef._

import eu.timepit.refined.numeric.{NonNegative, Positive => RPositive}
import eu.timepit.refined.refineV
import eu.timepit.refined.api.{RefType, Refined}
import scalaz.{Equal, Show, Monoid}
import scalaz.syntax.show._

package object numeric {

  type Natural = Long Refined NonNegative
  type Positive = Long Refined RPositive

  def Positive(a: Long): Option[Positive] = refineV[RPositive](a).right.toOption
  def Natural(a: Long): Option[Natural] = refineV[NonNegative](a).right.toOption

  implicit def widenInt[F[_,_],M](a: F[Int,M])(implicit rt: RefType[F]): F[Long,M] = rt.unsafeWrap(rt.unwrap(a).toLong)

  implicit def monoid[F[_,_],T](implicit rt: RefType[F], num: scala.Numeric[T]): Monoid[F[T,NonNegative]] =
    Monoid.instance(
      (a,b) => rt.unsafeWrap(num.plus(rt.unwrap(a), rt.unwrap(b))),
      rt.unsafeWrap(num.zero))

  implicit def equal[F[_,_],T:Equal,M](implicit rt: RefType[F]): Equal[F[T,M]] = Equal.equalBy(rt.unwrap)

  implicit def show[F[_,_],T:Show,M](implicit rt: RefType[F]): Show[F[T,M]] = Show.shows(f => rt.unwrap(f).shows)
}
