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
import quasar.effect.NameGenerator
import quasar.contrib.scalaz._
import quasar.contrib.scalaz.MonadState_
import quasar.fp._
import quasar.fp.ski.κ
import quasar.qscript.{FreeMapA, IdStatus, OnUndefined, RecFreeMap}

import monocle.macros.Lenses
import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns.EnvT
import scalaz.{\/-, -\/, \/, Cofree, DList, Id, Monad, Monoid, MonadState, Scalaz, Show, State, StateT}, Scalaz._

@Lenses
final case class QSUGraph[T[_[_]]](
    root: Symbol,
    vertices: QSUVerts[T]) {

  /**
   * Uniquely merge the graphs, retaining the root from the right.
   */
  def ++:(left: QSUGraph[T]): QSUGraph[T] = QSUGraph(root, left.vertices ++ vertices)

  /**
   * Uniquely merge the graphs, retaining the root from the left.
   */
  def :++(right: QSUGraph[T]): QSUGraph[T] = right ++: this

  /** A bottom up (leaves first) monoidal fold of the graph. */
  def foldMapUpM[F[_]: Monad, A: Monoid](f: QSUGraph[T] => F[A]): F[A] = {
    type VisitedT[X[_], A] = StateT[X, Set[Symbol], A]
    type G[A] = VisitedT[F, A]
    val MS = MonadState[G, Set[Symbol]]

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def inner(g: QSUGraph[T]): G[A] =
      for {
        visited <- MS.get

        a <- if (visited(g.root))
          mzero[A].point[G]
        else
          for {
            _ <- MS.put(visited + g.root)
            aSub <- g.unfold.foldMapM(inner)
            aG <- f(g).liftM[VisitedT]
          } yield aSub |+| aG
      } yield a

    inner(this).eval(Set())
  }

  def foldMapDownM[F[_]: Monad, A: Monoid](f: QSUGraph[T] => F[A]): F[A] = {
    type VisitedT[X[_], A] = StateT[X, Set[Symbol], A]
    type G[A] = VisitedT[F, A]
    val MS = MonadState[G, Set[Symbol]]

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def inner(g: QSUGraph[T]): G[A] =
      for {
        visited <- MS.get

        a <- if (visited(g.root))
          mzero[A].point[G]
        else
          for {
            _ <- MS.put(visited + g.root)
            aG <- f(g).liftM[VisitedT]
            aSub <- g.unfold.foldMapM(inner)
          } yield aG |+| aSub
      } yield a

    inner(this).eval(Set())
  }

  def foldMapUp[A: Monoid](f: QSUGraph[T] => A): A =
    foldMapUpM[Id, A](f)

  def foldMapDown[A: Monoid](f: QSUGraph[T] => A): A =
    foldMapDownM[Id, A](f)

  def refocus(node: Symbol): QSUGraph[T] =
    copy(root = node)

  /** Removes the `src` vertex, replacing any references to it with `target`. */
  def replace(src: Symbol, target: Symbol): QSUGraph[T] = {
    import QScriptUniform._

    def replaceIfSrc(sym: Symbol): Symbol =
      (sym === src) ? target | sym

    if (vertices.isDefinedAt(src) && vertices.isDefinedAt(target))
      QSUGraph(
        replaceIfSrc(root),
        (vertices - src) mapValues {
          case JoinSideRef(s) => JoinSideRef(replaceIfSrc(s))
          case other          => other map replaceIfSrc
        })
    else
      this
  }

  // the same as replace, but whenever a node is modified, it changes the name
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def replaceWithRename[
      F[_]: Monad: NameGenerator: MonadState_[?[_], QSUGraph.RevIdx[T]]](
      src: Symbol, target: Symbol): F[QSUGraph[T]] = {

    import QScriptUniform._

    val withName = QSUGraph.withName[T, F]("qsu")_

    if (src =/= target) {

      if (root === src) {
        refocus(target).point[F]
      } else {
        unfold match {
          case JoinSideRef(`src`) =>
            withName(JoinSideRef[T, Symbol](target)).map(_ :++ this)

          case _ =>
            for {
              pattern <- unfold.traverse(_.replaceWithRename[F](src, target))
              bare = pattern.map(_.root)
              renamed <- withName(bare)
              verts2 = pattern.foldLeft(SMap[Symbol, QScriptUniform[T, Symbol]]())(_ ++ _.vertices)
            } yield renamed.copy(vertices = verts2 ++ renamed.vertices)
        }
      }
    } else {
      this.point[F]
    }
  }

  def overwriteAtRoot(qsu: QScriptUniform[T, Symbol]): QSUGraph[T] =
    QSUGraph(root, vertices.updated(root, qsu))

  /**
   * Allows rewriting of arbitrary subgraphs.  Rewrites are
   * applied in a bottom-up (leaves-first) order, which avoids
   * ambiguities when changing nodes that are visible to subsequent
   * rewrites.  The one caveat here is that all "changes" to the graph
   * must be additive in nature.  You can never remove a node.
   * Rewrites to the definition of a node are valid, but you cannot
   * remove it from the graph, as other nodes will still point to it.
   * This becomes relevant if you want to transform some subgraph
   * g into f(g).  When this happens, you must internally generate a
   * new symbol for g and rewrite your subgraph to point to that new
   * g, then the composite graph f(g) must be given the original symbol.
   * As a safety check, because this case is just entirely unsound, the
   * symbol you return at the root is entirely ignored in favor of the
   * original root at that locus.
   */
  def rewriteM[F[_]: Monad](pf: PartialFunction[QSUGraph[T], F[QSUGraph[T]]]): F[QSUGraph[T]] = {
    type RewriteS = SMap[Symbol, QSUGraph[T]]
    type VisitedT[X[_], A] = StateT[X, RewriteS, A]
    type G[A] = VisitedT[F, A]
    val MS = MonadState[G, RewriteS]

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def inner(g: QSUGraph[T]): G[QSUGraph[T]] = {
      for {
        visited <- MS.get

        back <- visited.get(g.root) match {
          case Some(result) =>
            result.point[G]

          case None =>
            for {
              recursive <- g.unfold traverse { sg =>
                for {
                  previsM <- MS.get
                  previs = previsM.keySet

                  sg2 <- inner(sg)

                  postvisM <- MS.get
                  postvis = postvisM.keySet
                } yield (sg2, postvis &~ previs)
              }

              index = recursive.foldLeft[SMap[Symbol, Set[Symbol]]](SMap()) {
                case (acc, (sg, snapshot)) =>
                  val sym = sg.root

                  if (acc.contains(sym))
                    acc.updated(sym, acc(sym).union(snapshot))
                  else
                    acc + (sym -> snapshot)
              }

              rewritten = QSUGraph.refold[T](g.root, recursive.map(_._1))

              sum = index.values.reduceOption(_ union _).getOrElse(Set())

              // remove the keys which were touched from the original
              preimage = rewritten.vertices -- sum

              collapsed = recursive.foldLeft[QSUVerts[T]](preimage) {
                case (acc, (sg, _)) =>
                  // we need to explicitly remove the rewritten.root
                  // to avoid it being overwritten by an old version of itself
                  acc ++ (sg.vertices -- (sum &~ index(sg.root)) - rewritten.root)
              }

              self2 = rewritten.copy(vertices = collapsed)

              applied <- if (pf.isDefinedAt(self2))
                pf(self2).liftM[VisitedT]
              else
                self2.point[G]

              // prevent users from building invalid graphs
              back = applied.copy(root = self2.root)

              _ <- MS.modify(_ + (g.root -> back))
            } yield back
        }
      } yield back
    }

    inner(this).eval(SMap())
  }

  def rewrite(pf: PartialFunction[QSUGraph[T], QSUGraph[T]]): QSUGraph[T] =
    rewriteM[Id](pf)

  // projects the root of the graph (which we assume exists)
  def unfold: QScriptUniform[T, QSUGraph[T]] =
    vertices(root).map(refocus)

  /**
   * Note that because this is using SMap, we cannot guarantee
   * uniqueness is handled correctly if nodes include Free.
   * Make sure you do not rely on this.  Also please note that this
   * function is linear in the number of nodes, so try not to call
   * it too often.
   */
  def generateRevIndex: QSUGraph.RevIdx[T] =
    vertices map { case (key, value) => value -> key }
}

object QSUGraph extends QSUGraphInstances {

  import quasar.qsu.{QScriptUniform => QSU}

  // The pattern functor for `QSUGraph[T]`.
  type QSUPattern[T[_[_]], A] = EnvT[Symbol, QSU[T, ?], A]

  object QSUPattern {
    def apply[T[_[_]], A](root: Symbol, qsu: QSU[T, A]): QSUPattern[T, A] =
      EnvT.envT(root -> qsu)

    def unapply[T[_[_]], A](pattern: QSUPattern[T, A]): Option[(Symbol, QSU[T, A])] =
      pattern.run.some
  }

  type RevIdx[T[_[_]]] = SMap[QScriptUniform[T, Symbol], Symbol]

  // we assume that the verticies of the constituents are the same
  // refold is the inverse of unfold
  def refold[T[_[_]]](sym: Symbol, qsu: QScriptUniform[T, QSUGraph[T]]): QSUGraph[T] = {
    import scalaz.{Value, WriterT}

    // left-Monoid, based on the assumption that all
    // vertices changes are additive
    implicit val vm: Monoid[QSUVerts[T]] = new Monoid[QSUVerts[T]] {
      val zero = SMap[Symbol, QScriptUniform[T, Symbol]]()
      def append(left: QSUVerts[T], right: => QSUVerts[T]) =
        left ++ right
    }

    val collapse = qsu traverse { g =>
      WriterT.put(Value(g.root))(g.vertices)
    }

    val (verts, pf) = collapse.run.value
    QSUGraph[T](sym, verts + (sym -> pf))
  }

  def withName[T[_[_]], F[_]: Monad: NameGenerator](
      prefix: String)(
      node: QScriptUniform[T, Symbol])(
      implicit MS: MonadState_[F, RevIdx[T]]): F[QSUGraph[T]] = {

    for {
      reverse <- MS.get

      back <- reverse.get(node) match {
        case Some(sym) =>
          QSUGraph[T](root = sym, SMap(sym -> node)).point[F]

        case None =>
          for {
            sym <- freshSymbol[F](prefix)
            _ <- MS.put(reverse + (node -> sym))
          } yield QSUGraph[T](root = sym, SMap(sym -> node))
      }
    } yield back
  }

  type NodeNames[T[_[_]]] = SMap[QSU[T, Symbol], Symbol]
  type Renames = SMap[Symbol, Symbol]

  type NameState[T[_[_]], F[_]] = MonadState_[F, (NodeNames[T], Renames)]

  def NameState[T[_[_]], F[_]: NameState[T, ?[_]]] =
    MonadState_[F, (NodeNames[T], Renames)]

  /** Construct a QSUGraph from a tree of `QScriptUniform` by compacting
    * common subtrees, providing a mapping from the provided attribute to
    * the final graph vertex names.
    */
  def fromAnnotatedTree[T[_[_]]: RecursiveT](qsu: Cofree[QSU[T, ?], Option[Symbol]])
      : (Renames, QSUGraph[T]) = {

    type F[A] = StateT[State[Long, ?], (NodeNames[T], Renames), A]

    qsu.cataM(fromTreeƒ[T, F]).run((SMap(), SMap())).map {
      case ((_, s), r) => (s, r)
    }.eval(0)
  }

  /** Construct a QSUGraph from a tree of `QScriptUniform` by compacting
    * common subtrees.
    */
  def fromTree[T[_[_]]: RecursiveT](qsu: T[QSU[T, ?]]): QSUGraph[T] =
    fromAnnotatedTree[T](
      qsu.cata(attributeAlgebra[QSU[T, ?], Option[Symbol]](κ(None))))._2

  private def fromTreeƒ[T[_[_]], F[_]: Monad: NameGenerator: NameState[T, ?[_]]]
      : AlgebraM[F, EnvT[Option[Symbol], QSU[T, ?], ?], QSUGraph[T]] = {
    case EnvT((sym, qsu)) =>
      for {
        pair <- NameState[T, F].get
        (nodes, renames) = pair
        node =  qsu.map(_.root)
        name <- nodes.get(node).getOrElseF(
                  NameGenerator[F] prefixedName "__fromTree" map (Symbol(_)))
        _ <- NameState[T, F].put((nodes + (node -> name), sym.fold(renames)(s => renames + (s -> name))))
      } yield qsu.foldRight(QSUGraph(name, SMap(name -> node)))(_ ++: _)
  }

  /**
   * This object contains extraction helpers in terms of QSU nodes.
   */
  object Extractors {
    import quasar.Data
    import quasar.qscript.{
      FreeMap,
      Hole,
      JoinSide,
      JoinSide3,
      MapFunc,
      MapFuncsCore,
      MapFuncCore,
      SrcHole
    }

    import pathy.Path
    import scalaz.:<:

    object AutoJoin2 {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.AutoJoin2[T, QSUGraph[T]] => QSU.AutoJoin2.unapply(g)
        case _ => None
      }
    }

    object AutoJoin3 {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.AutoJoin3[T, QSUGraph[T]] => QSU.AutoJoin3.unapply(g)
        case _ => None
      }
    }

    object QSAutoJoin {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.QSAutoJoin[T, QSUGraph[T]] => QSU.QSAutoJoin.unapply(g)
        case _ => None
      }
    }

    object GroupBy {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.GroupBy[T, QSUGraph[T]] => QSU.GroupBy.unapply(g)
        case _ => None
      }
    }

    object DimEdit {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.DimEdit[T, QSUGraph[T]] => QSU.DimEdit.unapply(g)
        case _ => None
      }
    }

    object LPJoin {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.LPJoin[T, QSUGraph[T]] => QSU.LPJoin.unapply(g)
        case _ => None
      }
    }

    object ThetaJoin {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.ThetaJoin[T, QSUGraph[T]] => QSU.ThetaJoin.unapply(g)
        case _ => None
      }
    }

    object Read {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.Read[T, QSUGraph[T]] => QSU.Read.unapply(g)
        case _ => None
      }
    }

    object Unary {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.Unary[T, QSUGraph[T]] => QSU.Unary.unapply(g)
        case _ => None
      }
    }

    object Map {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.Map[T, QSUGraph[T]] => QSU.Map.unapply(g)
        case _ => None
      }
    }

    object Transpose {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.Transpose[T, QSUGraph[T]] => QSU.Transpose.unapply(g)
        case _ => None
      }
    }

    object LeftShift {
      def unapply[T[_[_]]](g: QSUGraph[T]): Option[(QSUGraph[T], RecFreeMap[T], IdStatus, OnUndefined, FreeMapA[T, QSU.ShiftTarget[T]], QSU.Rotation)] = g.unfold match {
        case g: QSU.LeftShift[T, QSUGraph[T]] => QSU.LeftShift.unapply(g)
        case _ => None
      }
    }

    object MultiLeftShift {
      def unapply[T[_[_]]](g: QSUGraph[T]): Option[(QSUGraph[T], List[(FreeMap[T], IdStatus, QSU.Rotation)], OnUndefined, FreeMapA[T, QAccess[T, Hole] \/ Int])] = g.unfold match {
        case g: QSU.MultiLeftShift[T, QSUGraph[T]] => QSU.MultiLeftShift.unapply(g)
        case _ => None
      }
    }

    object LPReduce {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.LPReduce[T, QSUGraph[T]] => QSU.LPReduce.unapply(g)
        case _ => None
      }
    }

    object QSReduce {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.QSReduce[T, QSUGraph[T]] => QSU.QSReduce.unapply(g)
        case _ => None
      }
    }

    object Distinct {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.Distinct[T, QSUGraph[T]] => QSU.Distinct.unapply(g)
        case _ => None
      }
    }

    object LPSort {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.LPSort[T, QSUGraph[T]] => QSU.LPSort.unapply(g)
        case _ => None
      }
    }

    object QSSort {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.QSSort[T, QSUGraph[T]] => QSU.QSSort.unapply(g)
        case _ => None
      }
    }

    object Union {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.Union[T, QSUGraph[T]] => QSU.Union.unapply(g)
        case _ => None
      }
    }

    object Subset {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.Subset[T, QSUGraph[T]] => QSU.Subset.unapply(g)
        case _ => None
      }
    }

    object LPFilter {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.LPFilter[T, QSUGraph[T]] => QSU.LPFilter.unapply(g)
        case _ => None
      }
    }

    object QSFilter {
      def unapply[T[_[_]]](g: QSUGraph[T]): Option[(QSUGraph[T], RecFreeMap[T])] = g.unfold match {
        case g: QSU.QSFilter[T, QSUGraph[T]] => QSU.QSFilter.unapply(g)
        case _ => None
      }
    }

    object Unreferenced {
      def unapply[T[_[_]]](g: QSUGraph[T]): Boolean = g.unfold match {
        case g: QSU.Unreferenced[T, QSUGraph[T]] => QSU.Unreferenced.unapply(g)
        case _ => false
      }
    }

    object JoinSideRef {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.JoinSideRef[T, QSUGraph[T]] => QSU.JoinSideRef.unapply(g)
        case _ => None
      }
    }

    object AutoJoin2C {
      def unapply[T[_[_]]](qgraph: QSUGraph[T])(
          implicit IC: MapFuncCore[T, ?] :<: MapFunc[T, ?])
          : Option[(QSUGraph[T], QSUGraph[T], MapFuncCore[T, JoinSide])] = qgraph match {

        case AutoJoin2(left, right, fm) =>
          fm.resumeTwice match {
            case \/-(_) => None
            case -\/(IC(func)) => func.traverse {
              case -\/(_) => None
              case \/-(side) => Some(side)
            }.flatMap(f => Some((left, right, f)))
          }
        case _ => None
      }
    }

    object AutoJoin3C {
      def unapply[T[_[_]]](qgraph: QSUGraph[T])(
          implicit IC: MapFuncCore[T, ?] :<: MapFunc[T, ?])
          : Option[(QSUGraph[T], QSUGraph[T], QSUGraph[T], MapFuncCore[T, JoinSide3])] = qgraph match {

        case AutoJoin3(left, center, right, fm) =>
          fm.resumeTwice match {
            case \/-(_) => None
            case -\/(IC(func)) => func.traverse {
              case -\/(_) => None
              case \/-(side) => Some(side)
            }.flatMap(f => Some((left, center, right, f)))
          }
        case _ => None
      }
    }

    object DataConstant {
      def unapply[T[_[_]]: BirecursiveT](qgraph: QSUGraph[T])(
          implicit IC: MapFuncCore[T, ?] :<: MapFunc[T, ?]): Option[Data] = qgraph match {

        case Unary(Unreferenced(), IC(MapFuncsCore.Constant(ejson))) =>
          Some(ejson.cata(Data.fromEJson))
        case _ => None
      }
    }

    object DataConstantMapped {
      def unapply[T[_[_]]: BirecursiveT](qgraph: QSUGraph[T])(
          implicit IC: MapFuncCore[T, ?] :<: MapFunc[T, ?]): Option[Data] = qgraph match {

        case Map(Unreferenced(), FMFC1(MapFuncsCore.Constant(ejson))) =>
          Some(ejson.cata(Data.fromEJson))
        case _ => None
      }
    }

    // TODO doesn't guarantee only one function; could be more!
    object FMFC1 {
      def unapply[T[_[_]]](fm: RecFreeMap[T])(
          implicit IC: MapFuncCore[T, ?] :<: MapFunc[T, ?]): Option[MapFuncCore[T, Hole]] = {

        fm.linearize.resume.swap.toOption collect {
          case IC(mfc) => mfc.map(_ => SrcHole: Hole)
        }
      }
    }

    object TRead {
      @SuppressWarnings(Array("org.wartremover.warts.Equals"))
      def unapply[T[_[_]]: BirecursiveT](qgraph: QSUGraph[T])(
          implicit IC: MapFuncCore[T, ?] :<: MapFunc[T, ?]): Option[String] = qgraph match {

        case Transpose(Read(path), QSU.Retain.Values, QSU.Rotation.ShiftMap) =>
          for {
            (front, end) <- Path.peel(path)
            file <- end.toOption
            if Path.peel(front).isEmpty
          } yield file.value

        case _ => None
      }
    }
  }
}

sealed abstract class QSUGraphInstances extends QSUGraphInstances0 {
  import QSUGraph._

  implicit def corecursive[T[_[_]]]: Corecursive.Aux[QSUGraph[T], QSUPattern[T, ?]] =
    birecursive[T]

  implicit def recursive[T[_[_]]]: Recursive.Aux[QSUGraph[T], QSUPattern[T, ?]] =
    birecursive[T]

  implicit def show[T[_[_]]: ShowT]: Show[QSUGraph[T]] =
    Show.shows { g =>
      val assocs = g.foldMapDown(sg => DList((sg.root, sg.vertices(sg.root))))

      s"QSUGraph(${g.root.shows})[\n" +
      printMultiline(assocs.toList) +
      "\n]"
    }
}

sealed abstract class QSUGraphInstances0 {
  import QSUGraph._

  implicit def birecursive[T[_[_]]]: Birecursive.Aux[QSUGraph[T], QSUPattern[T, ?]] =
    Birecursive.algebraIso(φ[T], ψ[T])

  ////

  // only correct given compacted subgraphs which agree on names
  private def φ[T[_[_]]]: Algebra[QSUPattern[T, ?], QSUGraph[T]] = {
    case QSUPattern(root, qsu) =>
      val initial: QSUGraph[T] = QSUGraph[T](root, SMap(root -> qsu.map(_.root)))

      qsu.foldRight(initial) {
        case (graph, acc) => graph ++: acc // retain the root from the right
      }
  }

  private def ψ[T[_[_]]]: Coalgebra[QSUPattern[T, ?], QSUGraph[T]] = graph => {
    QSUPattern[T, QSUGraph[T]](graph.root, graph.unfold)
  }
}
