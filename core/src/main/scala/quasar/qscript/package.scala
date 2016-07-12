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

package quasar

import quasar.Predef._

import scala.Predef.implicitly

import scalaz._, Scalaz._

/** Here we no longer care about provenance. Backends can’t do anything with
  * it, so we simply represent joins and crosses directly. This also means that
  * we don’t need to model certain things – project_d is just a data-level
  * function, nest_d & swap_d only modify provenance and so are irrelevant
  * here, and autojoin_d has been replaced with a lower-level join operation
  * that doesn’t include the cross portion.
  */
package object qscript {
  type Pathable[T[_[_]], A] = Coproduct[Const[DeadEnd, ?], SourcedPathable[T, ?], A]

  /** These are the operations included in all forms of QScript.
    */
  type QScriptPrim[T[_[_]], A] = Coproduct[QScriptCore[T, ?], Pathable[T, ?], A]

  /** This is the target of the core compiler. Normalization is applied to this
    * structure, and it contains no Read or EquiJoin.
    */
  type QScriptPure[T[_[_]], A] = Coproduct[ThetaJoin[T, ?], QScriptPrim[T, ?], A]

  type QScriptInternal[T[_[_]], A] = Coproduct[QScriptBucket[T, ?], QScriptPure[T, ?], A]

  /** These nodes exist in all QScript structures that a backend sees.
    */
  type QScriptCommon[T[_[_]], A] = Coproduct[Read, QScriptPrim[T, ?], A]

  // The following two types are the only ones that should be seen by a backend.

  /** This is the primary form seen by a backend. It contains reads of files.
    */
  type QScript[T[_[_]], A] = Coproduct[ThetaJoin[T, ?], QScriptCommon[T, ?], A]


  /** A variant with a simpler join type. A backend can choose to operate on
    * this structure by applying the `equiJoinsOnly` transformation. Backends
    * without true join support will likely find it easier to work with this
    * than to handle full ThetaJoins.
    */
  type EquiQScript[T[_[_]], A] = Coproduct[EquiJoin[T, ?], QScriptCommon[T, ?], A]

  val ExtEJson = implicitly[ejson.Extension :<: ejson.EJson]
  val CommonEJson = implicitly[ejson.Common :<: ejson.EJson]

  sealed trait JoinSide
  final case object LeftSide extends JoinSide
  final case object RightSide extends JoinSide

  object JoinSide {
    implicit val equal: Equal[JoinSide] = Equal.equalRef
    implicit val show: Show[JoinSide] = Show.showFromToString
  }

  type FreeUnit[F[_]] = Free[F, Unit]

  type FreeMap[T[_[_]]] = FreeUnit[MapFunc[T, ?]]
  type FreeQS[T[_[_]]] = FreeUnit[QScriptInternal[T, ?]]

  type JoinFunc[T[_[_]]] = Free[MapFunc[T, ?], JoinSide]

  def UnitF[T[_[_]]] = ().point[Free[MapFunc[T, ?], ?]]

  final case class SrcMerge[A, B](src: A, left: B, right: B)

  def rebase[M[_]: Bind, A](in: M[A], field: M[A]): M[A] = in >> field
}
