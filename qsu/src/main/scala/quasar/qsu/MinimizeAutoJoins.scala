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

import slamdata.Predef.{Map => SMap, _}
import quasar.common.effect.NameGenerator
import quasar.RenderTreeT
import quasar.contrib.std.errorImpossible
import quasar.contrib.matryoshka._
import quasar.contrib.scalaz.MonadState_
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.contrib.iota._
import quasar.qscript.{
  construction,
  Center,
  Hole,
  JoinSide3,
  LeftSide,
  LeftSide3,
  MonadPlannerErr,
  RightSide,
  RightSide3,
  SrcHole
}
import quasar.qscript.RecFreeS._
import quasar.qscript.provenance.Dimensions
import quasar.qscript.rewrites.NormalizableT
import quasar.qsu.{QScriptUniform => QSU}
import quasar.qsu.ApplyProvenance.AuthenticatedQSU

import matryoshka.{delayEqual, BirecursiveT, EqualT, ShowT}
import monocle.Traversal
import monocle.std.option.{some => someP}
import monocle.syntax.fields._1
import scalaz.{Bind, Monad, OptionT, Scalaz, StateT}, Scalaz._   // sigh, monad/traverse conflict

final class MinimizeAutoJoins[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] private () extends QSUTTypes[T] {
  import MinimizeAutoJoins._
  import QSUGraph.Extractors._

  private val Minimizers = List(
    minimizers.MergeReductions[T],
    minimizers.FilterToCond[T],
    minimizers.CollapseShifts[T])

  private val func = construction.Func[T]
  private val recFunc = construction.RecFunc[T]
  private val srcHole: Hole = SrcHole   // wtb smart constructor

  private val J = Fixed[T[EJson]]
  private val QP = QProv[T]

  private val N = new NormalizableT[T]

  def apply[F[_]: Monad: NameGenerator: MonadPlannerErr](agraph: AuthenticatedQSU[T]): F[AuthenticatedQSU[T]] = {
    type G[A] = StateT[StateT[F, RevIdx, ?], MinimizationState[T], A]

    val back = agraph.graph corewriteM {
      case qgraph @ AutoJoin2(left, right, combiner) =>
        val combiner2: FreeMapA[Int] = combiner map {
          case LeftSide => 0
          case RightSide => 1
        }

        OptionT(coalesceToMap[G](qgraph, List(left, right), combiner2)).getOrElseF(qgraph.point[G])

      case qgraph @ AutoJoin3(left, center, right, combiner) =>
        val combiner2: FreeMapA[Int] = combiner map {
          case LeftSide3 => 0
          case Center => 1
          case RightSide3 => 2
        }

        OptionT(coalesceToMap[G](qgraph, List(left, center, right), combiner2)).getOrElseF(qgraph.point[G])
    }

    val lifted = back(MinimizationState[T](agraph.auth)) map {
      case (MinimizationState(auth), graph) => AuthenticatedQSU[T](graph, auth)
    }

    lifted.eval(agraph.graph.generateRevIndex)
  }

  // the Ints are indices into branches
  private def coalesceToMap[
      G[_]: Monad: NameGenerator: RevIdxM: MinStateM[T, ?[_]]: MonadPlannerErr](
      qgraph: QSUGraph,
      branches: List[QSUGraph],
      combiner: FreeMapA[Int]): G[Option[QSUGraph]] = {

    val groupKeyOf: Traversal[Option[QDims], Symbol] =
      someP[QDims] composeTraversal
      Dimensions.dimension[QP.P] composePrism
      QP.prov.value composePrism
      IdAccess.groupKey composeLens
      _1

    for {
      state <- MinStateM[T, G].get

      fms = branches.map( g =>
          /* Halt coalescing if this graph was grouped, to ensure joining on group keys works */
          MappableRegion[T](s => groupKeyOf.exist(_ === s)(state.auth.lookupDims(s)), g))

      sources = fms.flatMap(_.toList).zipWithIndex

      (remap, candidates) = minimizeSources(sources)

      /* `sources` needs to be reversed because scala.Map construction
       * is right-biased. However, minimizeSources collapses duplicate
       * graphs in `sources` from the right into the leftmost
       * graphs */
      index = SMap(sources.reverse.map(_.leftMap(_.root)):_ *)

      fm = combiner.flatMap(fms(_)).map(g => remap(index(g.root)))

      back <- coalesceRoots[G](qgraph, fm, candidates)
    } yield back
  }

  // attempts to reduce the set of candidates to a single Map node, given a FreeMap[Int]
  // the Int indexes into the final number of distinct roots
  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  private def coalesceRoots[
      G[_]: Monad: NameGenerator: RevIdxM: MinStateM[T, ?[_]]: MonadPlannerErr](
      qgraph: QSUGraph,
      fm: => FreeMapA[Int],
      candidates: List[QSUGraph]): G[Option[QSUGraph]] = candidates match {

    case Nil =>
      updateGraph[T, G](QSU.Unreferenced[T, Symbol]()) map { unref =>
        some(qgraph.overwriteAtRoot(QSU.Map[T, Symbol](unref.root, fm.as(srcHole).asRec)) :++ unref)
      }

    case single :: Nil =>
      // if this is false, it's an assertion error
      // lazy val sanityCheck = fm.toList.forall(0 ==)

      val singleG = qgraph.overwriteAtRoot(QSU.Map[T, Symbol](single.root, fm.as(srcHole).asRec))

      updateProvenance[T, G](singleG) as some(singleG)

    case multiple if Minimizers.all(m => !m.couldApplyTo(multiple)) =>
      multiple match {
        case left :: right :: Nil => {
          val fm2: JoinFunc = fm map {
            case 0 => LeftSide
            case 1 => RightSide
          }

          val aj2 = qgraph.overwriteAtRoot(QSU.AutoJoin2[T, Symbol](left.root, right.root, fm2))

          updateProvenance[T, G](aj2) as some(aj2)
        }

        case left :: center :: right :: Nil => {
          val fm2: FreeMapA[JoinSide3] = fm map {
            case 0 => LeftSide3
            case 1 => Center
            case 2 => RightSide3
          }

          val aj3 = qgraph.overwriteAtRoot(QSU.AutoJoin3[T, Symbol](left.root, center.root, right.root, fm2))

          updateProvenance[T, G](aj3) as some(aj3)
        }

        case _ => none[QSUGraph].point[G]
      }

    case multiple =>
      for {
        resultM <- Minimizers.foldLeftM[G, Option[(QSUGraph, QSUGraph)]](None) {
          case (Some(pair), _) => Option(pair).point[G]

          case (None, minimizer) =>
            if (minimizer.couldApplyTo(multiple)) {
              val backM = for {
                extractions <- multiple traverse { c =>
                  OptionT(minimizer.extract[G](c).point[G])
                }

                (exCandidates, exRebuilds) = extractions unzip {
                  case (c, r) => (c, r)
                }

                upperFM = func.StaticMapFS((0 until exCandidates.length): _*)(_.point[FreeMapA], _.toString)

                /* The contents of the AutoJoin2 are irrelevant, as they
                will be overwritten by updateGraph. We just need any
                graph to be there. */

                fakeAutoJoinM = exCandidates match {
                  case left :: right :: _ =>
                    updateGraph[T, G](QSU.AutoJoin2(left.root, right.root, func.Undefined)) map { back =>
                      back :++ left
                    }

                  case _ => errorImpossible
                }

                fakeAutoJoin <- fakeAutoJoinM.liftM[OptionT]

                singleSource <-
                  OptionT(
                    coalesceToMap[G](
                      fakeAutoJoin,
                      exCandidates,
                      upperFM)).filter {
                    case AutoJoin2(_, _, _) => false
                    case AutoJoin3(_, _, _, _) => false
                    case _ => true
                  }

                (simplifiedSource, simplifiedFM) = singleSource match {
                  case Map(src, fm) => (src, fm)
                  case _  => (singleSource, recFunc.Hole)
                }

                candidates2 <- exRebuilds.zipWithIndex traverse {
                  case (rebuild, i) =>
                    val rebuiltFM = recFunc.ProjectKeyS(simplifiedFM, i.toString)
                    val normalized = N.freeMF(rebuiltFM.linearize)

                    rebuild(
                      simplifiedSource,
                      normalized).liftM[OptionT].map(simplifiedSource ++: _)
                }

                back <- OptionT(minimizer[G](qgraph, simplifiedSource, candidates2, fm))
                _ <- updateProvenance[T, G](back._2).liftM[OptionT]
              } yield back.bimap(simplifiedSource ++: _, simplifiedSource ++: _)

              backM.run
            } else {
              (None: Option[(QSUGraph, QSUGraph)]).point[G]
            }
        }

        back <- resultM traverse {
          case (retarget, result) =>
            updateForCoalesce[G](multiple, retarget.root) as result
        }
      } yield back
  }

  private def updateForCoalesce[G[_]: Bind: MinStateM[T, ?[_]]](
      candidates: List[QSUGraph],
      newRoot: Symbol): G[Unit] = {

    MinStateM[T, G] modify { state =>
      val auth2 = candidates.foldLeft(state.auth) { (auth, c) =>
        if (c.root =/= newRoot)
          auth.renameRefs(c.root, newRoot)
        else
          auth
      }

      state.copy(auth = auth2)
    }
  }

  // we return a remap function along with a minimized list
  // the remapper can be used to determine which input indices
  // collapsed into what output indices
  // note that if everything is Unreferenced, remap will be κ(-1),
  // which is weird but harmless
  private def minimizeSources(sources: List[(QSUGraph, Int)]): (Int => Int, List[QSUGraph]) = {
    val (_, remap, minimized) =
      sources.foldLeft((Set[Symbol](), SMap[Int, Int](), Vector[QSUGraph]())) {
        case ((seen, remap, acc), (Unreferenced(), i)) =>
          (seen, remap + (i -> (acc.length - 1)), acc)

        case ((seen, remap, acc), (g, i)) if seen(g.root) =>
          (seen, remap + (i -> (acc.length - 1)), acc)

        case ((seen, remap, acc), (g, i)) =>
          (seen + g.root, remap + (i -> acc.length), acc :+ g)
      }

    (i => remap.getOrElse(i, -1), minimized.toList)
  }
}

object MinimizeAutoJoins {
  final case class MinimizationState[T[_[_]]](auth: QAuth[T])

  type MinStateM[T[_[_]], F[_]] = MonadState_[F, MinimizationState[T]]
  def MinStateM[T[_[_]], F[_]](implicit ev: MinStateM[T, F]): MinStateM[T, F] = ev

  def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: Monad: NameGenerator: MonadPlannerErr]
      (agraph: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] =
    taggedInternalError("MinimizeAutoJoins", new MinimizeAutoJoins[T].apply[F](agraph))

  def updateGraph[
      T[_[_]]: BirecursiveT: EqualT: ShowT,
      G[_]: Monad: NameGenerator: RevIdxM[T, ?[_]]: MinStateM[T, ?[_]]: MonadPlannerErr](
      pat: QScriptUniform[T, Symbol]): G[QSUGraph[T]] = {

    for {
      qgraph <- QSUGraph.withName[T, G]("maj")(pat)
      _ <- updateProvenance[T, G](qgraph)
    } yield qgraph
  }

  def updateProvenance[
      T[_[_]]: BirecursiveT: EqualT: ShowT,
      G[_]: Monad: NameGenerator: RevIdxM[T, ?[_]]: MinStateM[T, ?[_]]: MonadPlannerErr](
      qgraph: QSUGraph[T]): G[Unit] = {

    for {
      state <- MinStateM[T, G].get

      computed <-
        ApplyProvenance.computeDims[T, StateT[G, QAuth[T], ?]](qgraph).exec(state.auth)

      _ <- MinStateM[T, G].put(state.copy(auth = computed))
    } yield ()
  }
}
