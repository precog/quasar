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

package quasar.qsu

import slamdata.Predef.{Boolean, Option, None, Symbol}
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.ski.κ
import quasar.qscript.{
  Center,
  FreeMap,
  FreeMapA,
  Hole,
  JoinFunc,
  JoinSide,
  LeftSide,
  LeftSide3,
  MapFunc,
  RightSide,
  RightSide3,
  SrcHole
}

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz.{Kleisli, Free, Scalaz, Traverse}, Scalaz._

/** The maximal "mappable" (expressable via MapFunc) region of a graph. */
object MappableRegion {
  import QScriptUniform._
  import QSUGraph.QSUPattern

  def apply[T[_[_]]](halt: Symbol => Boolean, g: QSUGraph[T]): FreeMapA[T, QSUGraph[T]] =
    Free.joinF[MapFunc[T, ?], QSUGraph[T]](
      g.ana[Free[FreeMapA[T, ?], QSUGraph[T]]](mappableRegionƒ[T](halt, _)))

  def binaryOf[T[_[_]]](left: Symbol, right: Symbol, g: QSUGraph[T]): Option[JoinFunc[T]] =
    funcOf(Kleisli(replaceWith[JoinSide](left, LeftSide)) <+> Kleisli(replaceWith(right, RightSide)), g)

  def funcOf[T[_[_]], A](f: Symbol => Option[A], g: QSUGraph[T]): Option[FreeMapA[T, A]] =
    Traverse[FreeMapA[T, ?]].traverse(apply(s => f(s).isDefined, g))(qg => f(qg.root))

  def maximal[T[_[_]]](g: QSUGraph[T]): FreeMapA[T, QSUGraph[T]] =
    apply(κ(false), g)

  def unaryOf[T[_[_]]](src: Symbol, g: QSUGraph[T]): Option[FreeMap[T]] =
    funcOf(replaceWith(src, SrcHole), g)

  def mappableRegionƒ[T[_[_]]](halt: Symbol => Boolean, g: QSUGraph[T])
      : CoEnv[QSUGraph[T], FreeMapA[T, ?], QSUGraph[T]] =
    g.project match {
      case QSUPattern(s, _) if halt(s) =>
        CoEnv(g.left[FreeMapA[T, QSUGraph[T]]])

      case QSUPattern(_, Map(srcG, fm)) =>
        CoEnv(fm.linearize.map(_ => srcG).right[QSUGraph[T]])

      case QSUPattern(_, AutoJoin2(left, right, combine)) =>
        CoEnv(combine.map {
          case LeftSide => left
          case RightSide => right
        }.right[QSUGraph[T]])

      case QSUPattern(_, AutoJoin3(left, center, right, combine)) =>
        CoEnv(combine.map {
          case LeftSide3 => left
          case Center => center
          case RightSide3 => right
        }.right[QSUGraph[T]])

      case _ =>
        CoEnv(g.left[FreeMapA[T, QSUGraph[T]]])
    }

  /**
   * An extractor for pulling out the a unique unary root of a
   * mappable region (if it exists) and the associated FreeMap.
   * If the unique unary root is Unreferenced(), then this extractor
   * will fail (produce None).  Note that this is less robust than
   * the analogous behavior in MinimizeAutoJoins#coalesceRoots, and
   * will not attempt things such as rewriting Filter into Cond.
   */
  object MaximalUnary {
    import QSUGraph.Extractors._

    def unapply[T[_[_]]](g: QSUGraph[T]): Option[(QSUGraph[T], FreeMap[T])] = {
      val fm = maximal[T](g)

      val roots = fm.toList filter {
        case Unreferenced() => false
        case _ => true
      }

      val deduped = roots.map(_.root).distinct

      if (deduped.lengthCompare(1) === 0)
        deduped.headOption.map(sym => (g.refocus(sym), fm.map(κ(SrcHole: Hole))))
      else
        None
    }
  }

  ////

  private def replaceWith[A](target: Symbol, a: A): Symbol => Option[A] =
    s => (s === target) option a
}
