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

package quasar.fp

import slamdata.Predef._
import scalaz._

/** This is like [[scalaz.Inject]], but for injecting an arbitrary coproduct
  * where all of the components are in the `OUT` coproduct in any order.
  *
  * It _may_ be unprincipled (otherwise, why not allow [[scalaz.Inject]] to work
  * this way directly?) But it is temporarily necessary in order to “inject” our
  * more constrained versions of QScript into [[QScriptTotal]].
  */
trait Injectable[IN[_], OUT[_]] {
  def inject: IN ~> OUT
  def project: OUT ~> λ[A => Option[IN[A]]]
}

object Injectable {

  implicit def id[F[_]]: Injectable[F, F] = make(
    NaturalTransformation.refl[F],
    λ[F ~> λ[A => Option[F[A]]]](Some(_))
  )

  def make[F[_], G[_]](inj: F ~> G, prj: G ~> λ[A => Option[F[A]]]): Injectable[F, G] = new Injectable[F, G] {
    val inject  = inj
    val project = prj
  }

  implicit def inject[F[_], G[a] <: ACopK[a]](implicit IN: F :<<: G): Injectable[F, G] =
    make[F, G](IN.inj, IN.prj)

}
