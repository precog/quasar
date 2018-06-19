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

package quasar.yggdrasil.vfs

import quasar.contrib.iota.{ACopK, :<<:}

import cats.effect.IO

import iotaz.CopK

import scalaz.{~>, Free}

object POSIXWithIO {
  def generalize[S[a] <: ACopK[a]]: GeneralizeSyntax[S] = new GeneralizeSyntax[S] {}

  private val JP = CopK.Inject[POSIXOp, POSIXWithIOCopK]
  private val JI = CopK.Inject[IO, POSIXWithIOCopK]

  trait GeneralizeSyntax[S[a] <: ACopK[a]] {
    def apply[A](pwt: POSIXWithIO[A])(implicit IP: POSIXOp :<<: S, II: IO :<<: S): Free[S, A] =
      pwt.mapSuspension(λ[POSIXWithIOCopK ~> S] {
        case JP(p) => IP(p)
        case JI(t) => II(t)
      })
  }
}
