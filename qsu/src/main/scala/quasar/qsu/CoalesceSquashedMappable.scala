/*
 * Copyright 2014–2020 SlamData Inc.
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

import slamdata.Predef.{Map => SMap, _}

import quasar.ejson.{EJson, Fixed}
import quasar.fp._
import quasar.fp.ski._
import quasar.contrib.iota._
import quasar.contrib.scalaz.free._
import quasar.qscript.{
  ExtractFunc,
  FreeMapA,
  Hole,
  MFC,
  MapFunc,
  MapFuncCore,
  MapFuncsCore,
  RecFreeMapA
}
import quasar.qscript.RecFreeS._
import quasar.qsu.{QScriptUniform => QSU}

import cats.data.State

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._

import scalaz.{State => _, _}, Scalaz._

import shims.{monadToCats, monadToScalaz}

/** Coalesces mappable regions separated by a `Squash`. */
final class CoalesceSquashedMappable[T[_[_]]: BirecursiveT: EqualT] private () extends QSUTTypes[T] {
  import CoalesceSquashedMappable.{Linearized, StaticMapReshape, StaticMapSuffixS}
  import QSUGraph.Extractors._
  import MapFuncCore.rollMF
  import MapFuncsCore.{ConcatMaps, ProjectKey, StrLit}

  type CoMapFuncR = CoEnv[Hole, MapFunc, FreeMap]
  type StaticAssoc = (String, FreeMap)

  def apply(graph: QSUGraph): QSUGraph =
    graph rewrite {
      case g @ Map(DimEdit(Map(src, inner), QSU.DTrans.Squash()), outer) =>
        val inner1 = inner.linearize

        inner1.project match {
          case StaticMapSuffixS(ds, ss) =>
            val sm = ss.foldLeft(SMap[String, FreeMap]())(_ + _)

            g.overwriteAtRoot(QSU.Map(
              src.root,
              MapFuncCore.normalized(outer.linearize.elgotApo[FreeMap](indexedSubst(inner1, ds, sm))).asRec))

          case _ =>
            g.overwriteAtRoot(QSU.Map(
              src.root,
              MapFuncCore.normalized(outer.linearize >> inner1).asRec))
        }

      case g @ Map(
          DimEdit(inner @ AutoJoin2(_, _, ExtractFunc(ConcatMaps(_, _))), QSU.DTrans.Squash()),
          Linearized(Embed(StaticMapReshape(reshape)))) =>

        type Halt = Boolean

        val reshaped =
          inner.corewriteM[State[Halt, ?]] {
            case rg @ AutoJoin2(l, r, Embed(StaticMapSuffixS(Nil, static))) =>
              State.get flatMap {
                case false =>
                  val s = static flatMap {
                    case (k, v) => reshape.get(k).toList.map(_.map(_ >> v))
                  }

                  State.set(true)
                    .as(rg.overwriteAtRoot(QSU.AutoJoin2(l.root, r.root, StaticMapSuffixS(Nil, s))))

                case true =>
                  State.pure(rg)
              }

            case rg @ AutoJoin2(l, r, Embed(StaticMapSuffixS(dynamic, static))) =>
              State.get flatMap {
                case false =>
                  val d = dynamic.flatMap(_.bitraverse(
                    { case (k, v) => reshape.get(k).toList.map(_.map(_ >> v)) },
                    List(_)))

                  val s = static flatMap {
                    case (k, v) => reshape.get(k).toList.map(_.map(_ >> v))
                  }

                  State.pure(rg.overwriteAtRoot(QSU.AutoJoin2(l.root, r.root, StaticMapSuffixS(d, s))))

                case true =>
                  State.pure(rg)
              }
          }

        QSUGraph.refold(g.root, reshaped.runA(false).value.unfold)
    }

  // Like a normalizing flatMap specialized to static maps where we have access
  // to the keys so we can simply select values rather than have to retraverse
  // `orig` multiple times.
  def indexedSubst(
      orig: FreeMap,
      dynamic: List[StaticAssoc \/ FreeMap],
      static: SMap[String, FreeMap])
      : ElgotCoalgebra[FreeMap \/ ?, CoEnv[Hole, MapFunc, ?], FreeMap] = {

    case ExtractFunc(ProjectKey(FreeA(_), kfm @ StrLit(k))) =>
      (static.get(k) getOrElse {
        rollMF[T, Hole](MFC(ProjectKey(
          StaticMapSuffixS(dynamic.filter(_.fold(_._1 ≟ k, κ(true))), Nil),
          kfm))).embed
      }).left

    case FreeA(_) =>
      orig.left

    case Embed(other) =>
      other.right[FreeMap]
  }
}

object CoalesceSquashedMappable {
  def apply[T[_[_]]: BirecursiveT: EqualT](graph: QSUGraph[T]): QSUGraph[T] =
    (new CoalesceSquashedMappable[T]).apply(graph)

  object Linearized {
    def unapply[T[_[_]], A](rf: RecFreeMapA[T, A]): Some[FreeMapA[T, A]] =
      Some(rf.linearize)
  }

  object StaticMapSuffixS {
    def apply[T[_[_]]: BirecursiveT, A](
        dynamic: List[(String, FreeMapA[T, A]) \/ FreeMapA[T, A]],
        static: List[(String, FreeMapA[T, A])])
        : FreeMapA[T, A] =
      MapFuncCore.StaticMapSuffix(
        dynamic.map(_.leftMap(_.leftMap(EJson.str[T[EJson]](_)))),
        static.map(_.leftMap(EJson.str[T[EJson]](_))))

    def unapply[T[_[_]]: BirecursiveT, A](mf: CoEnv[A, MapFunc[T, ?], FreeMapA[T, A]])
        : Option[(List[(String, FreeMapA[T, A]) \/ FreeMapA[T, A]], List[(String, FreeMapA[T, A])])] = {

      val J = Fixed[T[EJson]]

      MapFuncCore.StaticMapSuffix.unapply(mf) flatMap {
        case (d0, s0) =>
          val d1 = d0.traverse(_.fold(
            t => J.str.getOption(t._1).strengthR(t._2).map(_.left),
            f => Some(f.right)))

          val s1 = s0.traverse(t => J.str.getOption(t._1).strengthR(t._2))

          d1 tuple s1
      }
    }
  }

  /** Extractor that matches static maps that reshape an input map. */
  object StaticMapReshape {
    def unapply[T[_[_]]: BirecursiveT, A](mf: CoEnv[A, MapFunc[T, ?], FreeMapA[T, A]])
        : Option[SMap[String, (String, FreeMapA[T, A])]] = {

      val J = Fixed[T[EJson]]

      for {
        assocs <- MapFuncCore.StaticMap.unapply(mf)

        projected <- assocs traverse {
          case (k, v) => J.str.getOption(k) tuple projectedA(v)
        }
      } yield {
        projected.foldLeft(SMap[String, (String, FreeMapA[T, A])]()) {
          case (m, (cur, (prev, f))) => m.updated(prev, (cur, f))
        }
      }
    }

    // Extracts the name of the key projected from `A`, or none if no projection
    def projectedA[T[_[_]]: BirecursiveT, A](fm: FreeMapA[T, A]): Option[(String, FreeMapA[T, A])] = {
      import MapFuncsCore.{ProjectKey, StrLit}

      val algA: A => FreeMapA[T, (Option[String], A)] =
        a => FreeA((None, a))

      val algF: Algebra[MapFunc[T, ?], FreeMapA[T, (Option[String], A)]] = {
        case MFC(ProjectKey(FreeA((None, a)), StrLit(k))) =>
          FreeA((Some(k), a))

        case other => MapFuncCore.rollMF(other).embed
      }

      val fm1 = fm.cata(interpret(algA, algF))
      val keys = Foldable[FreeMapA[T, ?]].foldMap(fm1)(_._1.toSet)

      keys.headOption collect {
        case k if keys.size === 1 => (k, fm1.map(_._2))
      }
    }
  }
}
