/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.qsu

import slamdata.Predef.{Option, Set, Symbol}

import quasar.IdStatus, IdStatus.{ExcludeId, IdOnly}
import quasar.qscript.{HoleR, OnUndefined, RecFreeS, RightSideF}
import quasar.qsu.{QScriptUniform => QSU}
import quasar.qsu.ApplyProvenance.AuthenticatedQSU

import scalaz.Id.Id
import scalaz.syntax.bind._

final class CatchTranspose[T[_[_]], P] private () extends QSUTTypes[T] {
  import QSUGraph.Extractors._
  import RecFreeS.RecOps

  private final class UnaryUngrouped(grouped: Set[Symbol]) {
    def unapply(g: QSUGraph): Option[(QSUGraph, FreeMap)] =
      MappableRegion.MaximalUnary.extractUnary(g) { sym =>
        g.refocus(sym) match {
          case Map(_, _) => grouped(sym)
          case _ => true
        }
      }
  }

  def apply(qsu: AuthenticatedQSU[T, P]): AuthenticatedQSU[T, P] = {
    val AuthenticatedQSU(qgraph, qauth) = qsu
    val Ungrouped = new UnaryUngrouped(qauth.groupKeys.keySet.map(_._1))

    val caught = qgraph corewriteM[Id] {
      case g @ Ungrouped(Transpose(Ungrouped(src, struct), retain, rotation), repair) =>
        g.overwriteAtRoot(QSU.LeftShift(
          src.root,
          struct.asRec,
          retain.fold(IdOnly, ExcludeId),
          OnUndefined.Emit,
          repair >> RightSideF[T],
          rotation))

      case g @ Ungrouped(Transpose(src, retain, rotation), repair) =>
        g.overwriteAtRoot(QSU.LeftShift(
          src.root,
          HoleR[T],
          retain.fold(IdOnly, ExcludeId),
          OnUndefined.Emit,
          repair >> RightSideF[T],
          rotation))

      case g @ Transpose(Ungrouped(src, struct), retain, rotation) =>
        g.overwriteAtRoot(QSU.LeftShift(
          src.root,
          struct.asRec,
          retain.fold(IdOnly, ExcludeId),
          OnUndefined.Emit,
          RightSideF[T],
          rotation))

      case g @ Transpose(src, retain, rotation) =>
        g.overwriteAtRoot(QSU.LeftShift(
          src.root,
          HoleR[T],
          retain.fold(IdOnly, ExcludeId),
          OnUndefined.Emit,
          RightSideF[T],
          rotation))
    }

    AuthenticatedQSU(caught, qauth)
  }
}

object CatchTranspose {
  def apply[T[_[_]], P](aqsu: AuthenticatedQSU[T, P]): AuthenticatedQSU[T, P] =
    new CatchTranspose[T, P].apply(aqsu)
}
