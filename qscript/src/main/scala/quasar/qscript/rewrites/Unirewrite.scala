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

import quasar.contrib.pathy._
import quasar.fp._
import quasar.fs.Planner.PlannerErrorME
import quasar.qscript._

import matryoshka._
import matryoshka.implicits._
import scalaz._
import scalaz.syntax.all._

sealed trait Unirewrite[T[_[_]], C <: CoM] {
  def apply[F[_]: Monad: PlannerErrorME](r: Rewrite[T], lc: DiscoverPath.ListContents[F]): T[QScriptRead[T, ?]] => F[T[C#M]]
}

private[qscript] trait UnirewriteLowPriorityImplicits {
  private type C0[C <: CoM, A] = (Const[ShiftedRead[ADir], ?] :/: C#M)#M[A]

  implicit def fileRead[T[_[_]]: BirecursiveT, C <: CoM](
    implicit
      FC: Functor[C#M],
      TC0: Traverse[C0[C, ?]],
      J: SimplifyJoin.Aux[T, QScriptShiftRead[T, ?], C0[C, ?]],
      C: Coalesce.Aux[T, QScriptShiftRead[T, ?], QScriptShiftRead[T, ?]],
      N: Normalizable[QScriptShiftRead[T, ?]],
      E: ExpandDirs.Aux[T, C0[C, ?], C#M]): Unirewrite[T, C] = new Unirewrite[T, C] {

    def apply[F[_]: Monad: PlannerErrorME](r: Rewrite[T], lc: DiscoverPath.ListContents[F])
        : T[QScriptRead[T, ?]] => F[T[C#M]] = { qs =>
      r.simplifyJoinOnShiftRead[QScriptRead[T, ?], QScriptShiftRead[T, ?], C0[C, ?]]
        .apply(qs)
        .transCataM[F, T[C#M], C#M](E.expandDirs(reflNT[C#M], lc))
    }
  }
}

object Unirewrite extends UnirewriteLowPriorityImplicits {

  implicit def dirRead[T[_[_]], C <: CoM](
    implicit
      D: Const[ShiftedRead[ADir], ?] :<: C#M,
      T: Traverse[C#M],
      QC: QScriptCore[T, ?] :<: C#M,
      TJ: ThetaJoin[T, ?] :<: C#M,
      GI: Injectable.Aux[C#M, QScriptTotal[T, ?]],
      S: ShiftReadDir.Aux[T, QScriptRead[T, ?], C#M],
      C: Coalesce.Aux[T, C#M, C#M],
      N: Normalizable[C#M]): Unirewrite[T, C] = new Unirewrite[T, C] {

    def apply[F[_]: Monad: PlannerErrorME](r: Rewrite[T], lc: DiscoverPath.ListContents[F]): T[QScriptRead[T, ?]] => F[T[C#M]] =
      r.shiftReadDir[QScriptRead[T, ?], C#M] andThen (_.point[F])
  }

  def apply[T[_[_]], C <: CoM, F[_]: Monad: PlannerErrorME](r: Rewrite[T], lc: DiscoverPath.ListContents[F])(implicit U: Unirewrite[T, C]) =
    U(r, lc)
}
