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
package minimizers

import slamdata.Predef.{Map => SMap, _}

import quasar.{RenderTreeT, Type}
import quasar.common.effect.NameGenerator
import quasar.contrib.iota._
import quasar.contrib.matryoshka._
import quasar.ejson.{CommonEJson, Str}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.qscript.{construction, HoleF, MFC, MonadPlannerErr, RecFreeS}, RecFreeS._
import quasar.qscript.MapFuncsCore.{Constant, Eq, TypeOf}
import quasar.qsu.{QScriptUniform => QSU}

import matryoshka.data.free._
import matryoshka.patterns.CoEnv
import matryoshka.{BirecursiveT, Embed, EqualT, ShowT, delayEqual}

import scalaz.{-\/, \/-, Equal, Monad}
import scalaz.syntax.equal._
import scalaz.syntax.monad._

import scala.collection
import scala.sys

sealed abstract class FilterToCond[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]
    extends Minimizer[T]
    with MraPhase[T] {

  import MinimizeAutoJoins._
  import QSUGraph.Extractors._

  implicit def PEqual: Equal[P]

  private val func = construction.Func[T]
  private val recFunc = construction.RecFunc[T]

  def couldApplyTo(candidates: List[QSUGraph]): Boolean = {
    val (filters, _) = candidates partition {
      case QSFilter(_, _) => true
      case _ => false
    }

    filters.nonEmpty
  }

  def extract[
      G[_]: Monad: NameGenerator: MonadPlannerErr: RevIdxM: MinStateM[T, P, ?[_]]](
      qgraph: QSUGraph): Option[(QSUGraph, (QSUGraph, FreeMap) => G[QSUGraph])] = qgraph match {

    case QSFilter(src, predicate) =>
      // this is where we do the actual rewriting
      def rebuild(src: QSUGraph, fm: FreeMap): G[QSUGraph] = {
        updateGraph[T, G](
          qprov,
          QSU.Map(src.root, rewriteFilter(predicate.linearize, fm).asRec)).map(_ :++ src)
      }

      Some((src, rebuild _))

    case qgraph =>
      def rebuild(src: QSUGraph, fm: FreeMap): G[QSUGraph] = {
        if (fm === HoleF[T]) {
          src.point[G]
        } else {
          // this case should never happen
          updateGraph[T, G](qprov, QSU.Map(src.root, fm.asRec)) map { rewritten =>
            rewritten :++ src
          }
        }
      }

      Some((qgraph, rebuild _))
  }

  def apply[
      G[_]: Monad: NameGenerator: MonadPlannerErr: RevIdxM: MinStateM[T, P, ?[_]]](
      qgraph: QSUGraph,
      singleSource: QSUGraph,
      candidates: List[QSUGraph],
      fm: FreeMapA[Int]): G[Option[(QSUGraph, QSUGraph)]] = {

    val fms: SMap[Int, RecFreeMap] = candidates.zipWithIndex.map({
      case (Map(parent, fm), i) if parent.root === singleSource.root =>
        i -> fm

      case (parent, i) if parent.root === singleSource.root =>
        i -> recFunc.Hole

      case _ =>
        sys.error("assertion error")
    })(collection.breakOut)

    val collapsed = fm.asRec.flatMap(fms)

    updateGraph[T, G](qprov, QSU.Map(singleSource.root, collapsed)) map { g =>
      val back = g :++ singleSource
      Some((back, back))
    }
  }

  ///

  private def rewriteFilter(predicate: FreeMap, fm: FreeMap): FreeMap = {
    val nameToType: SMap[String, Type] =
      SMap(
        "number" -> Type.Numeric,
        "string" -> Type.Str,
        "boolean" -> Type.Bool,
        "offsetdatetime" -> Type.OffsetDateTime,
        "null" -> Type.Null)

    predicate.resume match {
      case -\/(MFC(Eq(
          Embed(CoEnv(\/-(MFC(TypeOf(Embed(CoEnv(-\/(_)))))))),
          Embed(CoEnv(\/-(MFC(Constant(Embed(CommonEJson(Str(expectedType))))))))))) =>
        nameToType.get(expectedType)
          .fold(func.Cond(predicate.as(fm).join, fm, func.Undefined))(func.Typecheck(fm, _))
      case _ =>
        func.Cond(predicate.as(fm).join, fm, func.Undefined)
    }
  }
}

object FilterToCond {
  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      qp: QProv[T])(implicit P: Equal[qp.P])
      : Minimizer.Aux[T, qp.P] =
    new FilterToCond[T] {
      val qprov: qp.type = qp
      val PEqual = P
    }
}

