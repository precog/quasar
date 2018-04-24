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

package quasar.qscript.analysis

import slamdata.Predef.{Map => _, _}

import quasar.fp._
import quasar.fp.ski._
import quasar.qscript._

import matryoshka.{Hole => _, _}
import matryoshka.data.free._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
import simulacrum._

/** Determines if `IN[_]` preserves the shape of its underlying `ShiftedRead`.
  * If it does preserve that shape then it returns the `IdStatus` of that
  * `ShiftedRead`. If it doesn't then `none` is returned.
  * This can easily be extended to also include `Read`, possibly with extra info
  * to indicate whether the underlying read was `Read` or `ShiftedRead`.
  */
@typeclass
trait ShapePreserving[IN[_]] {
  def shapePreservingƒ: Algebra[IN, Option[IdStatus]]
}

object ShapePreserving {

  def shapePreserving[F[_]: Functor, T](t: T)(implicit
    RT: Recursive.Aux[T, F],
    SP: ShapePreserving[F])
      : Option[IdStatus]
    = t.cata(SP.shapePreservingƒ)

  def shapePreservingF[F[_]: Functor, A]
    (fa: Free[F, A])
    (f: A => Option[IdStatus])
    (implicit SP: ShapePreserving[F])
      : Option[IdStatus] =
    fa.cata(interpret(f, SP.shapePreservingƒ))

  def shapePreservingP[F[_], G[_]: Functor, T](t: T, GtoF: PrismNT[G, F])(implicit
    RT: Recursive.Aux[T, G],
    SP: ShapePreserving[F])
      : Option[IdStatus]
    = t.cata(prismNT(GtoF).shapePreservingƒ)

  def prismNT[F[_], G[_]](GtoF: PrismNT[G, F])
    (implicit F: ShapePreserving[F])
      : ShapePreserving[G] =
    new ShapePreserving[G] {
      def shapePreservingƒ: Algebra[G, Option[IdStatus]] =
        x => GtoF.get(x).flatMap(F.shapePreservingƒ)
    }

  implicit def coproduct[F[_], G[_]]
    (implicit F: ShapePreserving[F], G: ShapePreserving[G])
      : ShapePreserving[Coproduct[F, G, ?]] =
    new ShapePreserving[Coproduct[F, G, ?]] {
      def shapePreservingƒ: Algebra[Coproduct[F, G, ?], Option[IdStatus]] =
        _.run.fold(F.shapePreservingƒ, G.shapePreservingƒ)
    }

  implicit def constShiftedRead[A]: ShapePreserving[Const[ShiftedRead[A], ?]] =
    new ShapePreserving[Const[ShiftedRead[A], ?]] {
      def shapePreservingƒ: Algebra[Const[ShiftedRead[A], ?], Option[IdStatus]] =
        _.getConst.idStatus.some
    }

  implicit def constRead[A]: ShapePreserving[Const[Read[A], ?]] =
    notShapePreserving[Const[Read[A], ?]]

  implicit val constDeadEnd: ShapePreserving[Const[DeadEnd, ?]] =
    notShapePreserving[Const[DeadEnd, ?]]

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit def qscriptCore[T[_[_]]]: ShapePreserving[QScriptCore[T, ?]] =
    new ShapePreserving[QScriptCore[T, ?]] {
      def shapePreservingƒ: Algebra[QScriptCore[T, ?], Option[IdStatus]] = {
        case Map(_, _) => none
        case LeftShift(_, _, _, _, _, _) => none
        case Reduce(_, _, _, _) => none
        case Sort(src, _, _) => src
        case Union(src, l, r) =>
          val lId = shapePreservingF(l)(κ(src))
          val rId = shapePreservingF(r)(κ(src))
          if (lId === rId) lId else None
        case Filter(src, _) => src
        case Subset(src, from, _, _) => shapePreservingF(from)(κ(src))
        case Unreferenced() => none
      }
    }

  implicit def projectBucket[T[_[_]]]: ShapePreserving[ProjectBucket[T, ?]] =
    notShapePreserving[ProjectBucket[T, ?]]

  implicit def thetaJoin[T[_[_]]]: ShapePreserving[ThetaJoin[T, ?]] =
    notShapePreserving[ThetaJoin[T, ?]]

  implicit def equiJoin[T[_[_]]]: ShapePreserving[EquiJoin[T, ?]] =
    notShapePreserving[EquiJoin[T, ?]]

  implicit def coEnv[E, G[_], A](implicit SP: ShapePreserving[G]): ShapePreserving[CoEnv[E, G, ?]] =
    new ShapePreserving[CoEnv[E, G, ?]] {
      def shapePreservingƒ: Algebra[CoEnv[E, G, ?], Option[IdStatus]] =
        _.run.map(SP.shapePreservingƒ).getOrElse(none)
    }

  def notShapePreserving[F[_]] =
    new ShapePreserving[F] {
      def shapePreservingƒ: Algebra[F, Option[IdStatus]] = κ(none)
    }
}
