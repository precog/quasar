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
import quasar.contrib.iota._
import quasar.qscript._

import matryoshka._
import matryoshka.implicits._
import scalaz._
import scalaz.syntax.all._
import iotaz.{CopK, TListK}
import iotaz.TListK.:::

sealed trait Unirewrite[T[_[_]], L <: TListK] {
  def apply[F[_]: Monad: MonadPlannerErr](r: Rewrite[T], lc: DiscoverPath.ListContents[F]): T[QScriptRead[T, ?]] => F[T[CopK[L, ?]]]
}

private[qscript] trait UnirewriteLowPriorityImplicits {
  private type C0[L <: TListK, A] = CopK[Const[ShiftedRead[ADir], ?] ::: L, A]

  implicit def fileRead[T[_[_]]: BirecursiveT, L <: TListK](
    implicit
      FC: Functor[CopK[L, ?]],
      TC0: Traverse[C0[L, ?]],
      J: SimplifyJoin.Aux[T, QScriptShiftRead[T, ?], C0[L, ?]],
      C: Coalesce.Aux[T, QScriptShiftRead[T, ?], QScriptShiftRead[T, ?]],
      N: Normalizable[QScriptShiftRead[T, ?]],
      E: ExpandDirs.Aux[T, C0[L, ?], CopK[L, ?]]): Unirewrite[T, L] = new Unirewrite[T, L] {

    def apply[F[_]: Monad: MonadPlannerErr](r: Rewrite[T], lc: DiscoverPath.ListContents[F])
        : T[QScriptRead[T, ?]] => F[T[CopK[L, ?]]] = { qs =>
      r.simplifyJoinOnShiftRead[QScriptRead[T, ?], QScriptShiftRead[T, ?], C0[L, ?]]
        .apply(qs)
        .transCataM[F, T[CopK[L, ?]], CopK[L, ?]](E.expandDirs(reflNT[CopK[L, ?]], lc))
    }
  }
}

object Unirewrite extends UnirewriteLowPriorityImplicits {

  implicit def dirRead[T[_[_]], L <: TListK](
    implicit
      D: Const[ShiftedRead[ADir], ?] :<<: CopK[L, ?],
      T: Traverse[CopK[L, ?]],
      QC: QScriptCore[T, ?] :<<: CopK[L, ?],
      TJ: ThetaJoin[T, ?] :<<: CopK[L, ?],
      S: ShiftReadDir.Aux[T, QScriptRead[T, ?], CopK[L, ?]],
      C: Coalesce.Aux[T, CopK[L, ?], CopK[L, ?]],
      N: Normalizable[CopK[L, ?]]): Unirewrite[T, L] = new Unirewrite[T, L] {

    def apply[F[_]: Monad: MonadPlannerErr](r: Rewrite[T], lc: DiscoverPath.ListContents[F]): T[QScriptRead[T, ?]] => F[T[CopK[L, ?]]] =
      r.shiftReadDir[QScriptRead[T, ?], CopK[L, ?]] andThen (_.point[F])
  }

  def apply[T[_[_]], L <: TListK, F[_]: Monad: MonadPlannerErr](r: Rewrite[T], lc: DiscoverPath.ListContents[F])(implicit U: Unirewrite[T, L]) =
    U(r, lc)
}
