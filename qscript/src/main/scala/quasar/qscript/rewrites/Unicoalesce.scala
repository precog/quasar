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

import slamdata.Predef._
import quasar.contrib.matryoshka._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.ski._
import quasar.qscript._

import matryoshka._
import scalaz._
import iotaz.{CopK, TListK}

sealed trait Unicoalesce[T[_[_]], L <: TListK] {
  def apply(C: Coalesce.Aux[T, CopK[L, ?], CopK[L, ?]])(implicit F: Functor[CopK[L, ?]]): CopK[L, T[CopK[L, ?]]] => Option[CopK[L, T[CopK[L, ?]]]]
}

object Unicoalesce {

  /**
   * Provides a convenient way of capturing all of the Unicoalesce
   * dependencies in a single implicit, which can then be passed
   * along as a value (e.g. in BackendModule).
   */
  trait Capture[T[_[_]], L <: TListK] {
    val C: Coalesce.Aux[T, CopK[L, ?], CopK[L, ?]]
    val F: Functor[CopK[L, ?]]
    val QC: UnicoalesceQC[T, L]
    val SR: UnicoalesceSR[T, L]
    val EJ: UnicoalesceEJ[T, L]
    val TJ: UnicoalesceTJ[T, L]
    val N: Normalizable[CopK[L, ?]]

    def run: CopK[L, T[CopK[L, ?]]] => CopK[L, T[CopK[L, ?]]] =
      apply[T, L](C, F, QC, SR, EJ, TJ, N)
  }

  object Capture {

    implicit def materialize[T[_[_]], L <: TListK](
      implicit
        _C: Coalesce.Aux[T, CopK[L, ?], CopK[L, ?]],
        _F: Functor[CopK[L, ?]],
        _QC: UnicoalesceQC[T, L],
        _SR: UnicoalesceSR[T, L],
        _EJ: UnicoalesceEJ[T, L],
        _TJ: UnicoalesceTJ[T, L],
        _N: Normalizable[CopK[L, ?]]): Capture[T, L] = new Capture[T, L] {

      val C = _C
      val F = _F
      val QC = _QC
      val SR = _SR
      val EJ = _EJ
      val TJ = _TJ
      val N = _N
    }

    def apply[T[_[_]], L <: TListK](implicit C: Capture[T, L]) = C
  }

  def apply[T[_[_]], L <: TListK](
    implicit
      C: Coalesce.Aux[T, CopK[L, ?], CopK[L, ?]],
      F: Functor[CopK[L, ?]],
      QC: UnicoalesceQC[T, L],
      SR: UnicoalesceSR[T, L],
      EJ: UnicoalesceEJ[T, L],
      TJ: UnicoalesceTJ[T, L],
      N: Normalizable[CopK[L, ?]]): CopK[L, T[CopK[L, ?]]] => CopK[L, T[CopK[L, ?]]] =
    repeatedly(
      applyTransforms(
        QC(C),
        SR(C),
        EJ(C),
        TJ(C),
        N.normalizeF(_: CopK[L, T[CopK[L, ?]]])))
}

sealed trait UnicoalesceQC[T[_[_]], L <: TListK] extends Unicoalesce[T, L]

private[qscript] trait UnicoalesceQCLowPriorityImplicits {
  implicit def default[T[_[_]], L <: TListK]: UnicoalesceQC[T, L] = new UnicoalesceQC[T, L] {
    def apply(C: Coalesce.Aux[T, CopK[L, ?], CopK[L, ?]])(implicit F: Functor[CopK[L, ?]]) = κ(None)
  }
}

object UnicoalesceQC extends UnicoalesceQCLowPriorityImplicits {

  implicit def member[T[_[_]], L <: TListK](
    implicit
      QC: QScriptCore[T, ?] :<<: CopK[L, ?]): UnicoalesceQC[T, L] = new UnicoalesceQC[T, L] {

    def apply(C: Coalesce.Aux[T, CopK[L, ?], CopK[L, ?]])(implicit F: Functor[CopK[L, ?]]) =
      C.coalesceQC[CopK[L, ?]](idPrism)
  }
}

sealed trait UnicoalesceSR[T[_[_]], L <: TListK] extends Unicoalesce[T, L]

private[qscript] trait UnicoalesceSRLowPriorityImplicits {
  implicit def default[T[_[_]], L <: TListK]: UnicoalesceSR[T, L] = new UnicoalesceSR[T, L] {
    def apply(C: Coalesce.Aux[T, CopK[L, ?], CopK[L, ?]])(implicit F: Functor[CopK[L, ?]]) = κ(None)
  }
}

object UnicoalesceSR extends UnicoalesceSRLowPriorityImplicits {

  // TODO the A might not infer here
  implicit def member[T[_[_]], A, L <: TListK](
    implicit
      QC: QScriptCore[T, ?] :<<: CopK[L, ?],
      SR: Const[ShiftedRead[A], ?] :<<: CopK[L, ?]): UnicoalesceSR[T, L] = new UnicoalesceSR[T, L] {

    def apply(C: Coalesce.Aux[T, CopK[L, ?], CopK[L, ?]])(implicit F: Functor[CopK[L, ?]]) =
      C.coalesceSR[CopK[L, ?], A](idPrism)
  }
}

sealed trait UnicoalesceEJ[T[_[_]], L <: TListK] extends Unicoalesce[T, L]

private[qscript] trait UnicoalesceEJLowPriorityImplicits {
  implicit def default[T[_[_]], L <: TListK]: UnicoalesceEJ[T, L] = new UnicoalesceEJ[T, L] {
    def apply(C: Coalesce.Aux[T, CopK[L, ?], CopK[L, ?]])(implicit F: Functor[CopK[L, ?]]) = κ(None)
  }
}

object UnicoalesceEJ extends UnicoalesceEJLowPriorityImplicits {

  implicit def member[T[_[_]], L <: TListK](
    implicit
      QC: EquiJoin[T, ?] :<<: CopK[L, ?]): UnicoalesceEJ[T, L] = new UnicoalesceEJ[T, L] {

    def apply(C: Coalesce.Aux[T, CopK[L, ?], CopK[L, ?]])(implicit F: Functor[CopK[L, ?]]) =
      C.coalesceEJ[CopK[L, ?]](idPrism.get)
  }
}

sealed trait UnicoalesceTJ[T[_[_]], L <: TListK] extends Unicoalesce[T, L]

private[qscript] trait UnicoalesceTJLowPriorityImplicits {
  implicit def default[T[_[_]], L <: TListK]: UnicoalesceTJ[T, L] = new UnicoalesceTJ[T, L] {
    def apply(C: Coalesce.Aux[T, CopK[L, ?], CopK[L, ?]])(implicit F: Functor[CopK[L, ?]]) = κ(None)
  }
}

object UnicoalesceTJ extends UnicoalesceTJLowPriorityImplicits {

  implicit def member[T[_[_]], L <: TListK](
    implicit
      QC: ThetaJoin[T, ?] :<<: CopK[L, ?]): UnicoalesceTJ[T, L] = new UnicoalesceTJ[T, L] {

    def apply(C: Coalesce.Aux[T, CopK[L, ?], CopK[L, ?]])(implicit F: Functor[CopK[L, ?]]) =
      C.coalesceTJ[CopK[L, ?]](idPrism.get)
  }
}
