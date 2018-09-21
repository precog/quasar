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

package quasar.qscript.rewrites

import quasar.contrib.iota._
import quasar.fp.liftFG
import quasar.qscript._
import quasar.qscript.MapFuncCore._

import matryoshka._
import matryoshka.data._

import scalaz.Functor
import scalaz.syntax.equal._

final class Optimize[T[_[_]]: BirecursiveT: EqualT] extends TTypes[T] {

  def elideNopMap[F[a] <: ACopK[a]: Functor](implicit QC: QScriptCore :<<: F)
      : QScriptCore[T[F]] => F[T[F]] = {
    case Map(Embed(src), mf) if mf === HoleR => src
    case qc => QC.inj(qc)
  }
}

object Optimize {
  def apply[T[_[_]]: BirecursiveT: EqualT, F[a] <: ACopK[a]: Functor](
      implicit QC: QScriptCore[T, ?] :<<: F)
      : F[T[F]] => F[T[F]] = {
    val opt = new Optimize[T]
    liftFG[QScriptCore[T, ?], F, T[F]](opt.elideNopMap)
  }
}
