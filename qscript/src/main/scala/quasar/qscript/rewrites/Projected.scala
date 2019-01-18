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

import slamdata.Predef.{Option, Some}

import quasar.common.CPath
import quasar.contrib.iota._
import quasar.contrib.scalaz.free._
import quasar.qscript._
import quasar.qscript.RecFreeS._

import scala.util.{Either, Left, Right}

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns.interpret

import monocle.Lens

import scalaz.Foldable
import scalaz.std.anyVal._
import scalaz.std.tuple._
import scalaz.syntax.tag._
import scalaz.syntax.std.boolean._

/** Matches on a `FreeMap` where all direct access of `Hole` is via static projection. */
object Projected {
  import MapFuncsCore.{IntLit, ProjectIndex, ProjectKey, StrLit}

  def unapply[T[_[_]]: BirecursiveT](fm: FreeMap[T]): Option[FreeMapA[T, CPath]] =
    projected((_: Hole) => p => p, Lens.id, fm)

  def projected[T[_[_]]: BirecursiveT, S, A](
      f: A => CPath => S,
      l: Lens[S, CPath],
      fm: FreeMapA[T, A])
      : Option[FreeMapA[T, S]] =
    Some(reifyProjections(f, l, fm)) filter { r =>
      val (cnt, allPrj) = Foldable[FreeMapA[T, ?]].foldMap(r) { p =>
        (1, l.exist(_ != CPath.Identity)(p).conjunction)
      }
      cnt > 0 && allPrj.unwrap
    }

  // Reify a series of projections of `A` as `S`.
  def reifyProjections[T[_[_]]: BirecursiveT, S, A](
      f: A => CPath => S,
      l: Lens[S, CPath],
      fm: FreeMapA[T, A])
      : FreeMapA[T, S] = {

    val xformA: A => FreeMapA[T, S] =
      a => FreeA(f(a)(CPath.Identity))

    val xformFunc: Algebra[MapFunc[T, ?], FreeMapA[T, S]] = {
      case MFC(ProjectKey(FreeA(p), StrLit(key))) =>
        FreeA(l.modify(_ \ key)(p))

      case MFC(ProjectIndex(FreeA(p), IntLit(i))) if i.isValidInt =>
        FreeA(l.modify(_ \ i.toInt)(p))

      case other =>
        FreeF(other)
    }

    fm.cata(interpret(xformA, xformFunc))
  }
}

object ProjectedRec {
  def unapply[T[_[_]]: BirecursiveT](fm: RecFreeMap[T]): Option[FreeMapA[T, CPath]] =
    Projected.unapply(fm.linearize)
}

object Projected2 {
  def unapply[T[_[_]]: BirecursiveT](jf: JoinFunc[T]): Option[FreeMapA[T, Either[CPath, CPath]]] =
    Projected.projected(liftEither, uEither, jf)

  def uEither[A]: Lens[Either[A, A], A] =
    Lens[Either[A, A], A](_.merge)(a => {
      case Left(_) => Left(a)
      case Right(_) => Right(a)
    })

  def liftEither: JoinSide => (CPath => Either[CPath, CPath]) = {
    case LeftSide => Left(_)
    case RightSide => Right(_)
  }
}
