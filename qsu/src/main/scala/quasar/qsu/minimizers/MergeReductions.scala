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
package minimizers

import quasar.fs.Planner, Planner.PlannerErrorME
import quasar.contrib.matryoshka._
import quasar.effect.NameGenerator
import quasar.ejson.implicits._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.ski.κ
import quasar.qscript.{
  construction,
  Hole,
  ReduceIndex
}
import quasar.qscript.RecFreeS._
import quasar.qsu.{QScriptUniform => QSU}
import slamdata.Predef._

import matryoshka.{delayEqual, BirecursiveT, EqualT, ShowT}
import matryoshka.data.free._
import scalaz.{Free, Monad, Scalaz}, Scalaz._

final class MergeReductions[T[_[_]]: BirecursiveT: EqualT: ShowT] private () extends Minimizer[T] {
  import MinimizeAutoJoins._
  import QSUGraph.Extractors._

  private val func = construction.Func[T]
  private val recFunc = construction.RecFunc[T]

  def couldApplyTo(candidates: List[QSUGraph]): Boolean = {
    candidates forall {
      case QSReduce(_, _, _, _) => true
      case _ => false
    }
  }

  def extract[
      G[_]: Monad: NameGenerator: PlannerErrorME: RevIdxM: MinStateM[T, ?[_]]](
      qgraph: QSUGraph): Option[(QSUGraph, (QSUGraph, FreeMap) => G[QSUGraph])] = qgraph match {

    case qgraph @ QSReduce(src, buckets, reducers, repair) =>
      def rebuild(src: QSUGraph, fm: FreeMap): G[QSUGraph] = {
        def rewriteBucket(bucket: QAccess[Hole]): FreeMapA[QAccess[Hole]] = bucket match {
          case v @ Access.Value(_) => fm.map(κ(v))
          case other => Free.pure[MapFunc, QAccess[Hole]](other)
        }

        val buckets2 = buckets.map(_.flatMap(rewriteBucket))
        val reducers2 = reducers.map(_.map(_.flatMap(κ(fm))))

        updateGraph[T, G](QSU.QSReduce(src.root, buckets2, reducers2, repair)) map { rewritten =>
          qgraph.overwriteAtRoot(rewritten.vertices(rewritten.root)) :++ rewritten :++ src
        }
      }

      Some((src, rebuild _))
  }

  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  def apply[
      G[_]: Monad: NameGenerator: PlannerErrorME: RevIdxM: MinStateM[T, ?[_]]](
      qgraph: QSUGraph,
      source: QSUGraph,
      candidates: List[QSUGraph],
      fm: FreeMapA[Int]): G[Option[(QSUGraph, QSUGraph)]] = {

    val reducerAttempt = candidates collect {
      case g @ QSReduce(source, buckets, reducers, repair) =>
        (source, buckets, reducers, repair)
    }

    val (_, buckets, _, _) = reducerAttempt.head

    val sourceCheck = reducerAttempt filter {
      case (cSrc, cBuckets, _, _) =>
        cSrc.root === source.root && cBuckets === buckets
    }

    if (sourceCheck.lengthCompare(candidates.length) === 0) {
      val lifted = reducerAttempt.zipWithIndex map {
        case ((_, buckets, reducers, repair), i) =>
          (
            buckets,
            reducers,
            // we use maps here to avoid definedness issues in reduction results
            func.MakeMapS(i.toString, repair))
      }

      // this is fine, because we can't be here if candidates is empty
      // doing it this way avoids an extra (and useless) Option state in the fold
      val (_, lhreducers, lhrepair) = lifted.head

      // squish all the reducers down into lhead
      val (_, (reducers, repair)) =
        lifted.tail.foldLeft((lhreducers.length, (lhreducers, lhrepair))) {
          case ((roffset, (lreducers, lrepair)), (_, rreducers, rrepair)) =>
            val reducers = lreducers ::: rreducers

            val roffset2 = roffset + rreducers.length

            val repair = func.ConcatMaps(
              lrepair,
              rrepair map {
                case ReduceIndex(e) =>
                  ReduceIndex(e.rightMap(_ + roffset))
              })

            (roffset2, (reducers, repair))
        }

      // 107.7, All chiropractors, all the time
      val adjustedFM = fm.asRec flatMap { i =>
        // get the value back OUT of the map
        recFunc.ProjectKeyS(recFunc.Hole, i.toString)
      }

      val redPat = QSU.QSReduce[T, Symbol](source.root, buckets, reducers, repair)

      updateGraph[T, G](redPat) map { rewritten =>
        val back = qgraph.overwriteAtRoot(QSU.Map[T, Symbol](rewritten.root, adjustedFM)) :++ rewritten

        Some((rewritten, back))
      }
    } else {
      (None: Option[(QSUGraph, QSUGraph)]).point[G]
    }
  }
}

object MergeReductions {
  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT]: MergeReductions[T] =
    new MergeReductions[T]
}
