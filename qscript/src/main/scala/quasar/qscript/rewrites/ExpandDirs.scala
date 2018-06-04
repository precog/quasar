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

import slamdata.Predef.{Map => _, _}
import quasar.contrib.pathy._
import quasar.contrib.scalaz._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.ski._
import quasar.fs.Planner.{NoFilesFound, PlannerErrorME}
import quasar.qscript._

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import pathy.Path.{dir1, file1}
import scalaz._, Scalaz._
import iotaz.{TListK, CopK, TNilK}
import iotaz.TListK.:::

/** Converts any {Shifted}Read containing a directory to a union of all the
  * files in that directory.
  */
trait ExpandDirs[IN[_]] {
  type IT[F[_]]
  type OUT[A]

  def expandDirs[M[_]: Monad: PlannerErrorME, F[_]: Functor]
    (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M])
      : IN[IT[F]] => M[F[IT[F]]]
}

object ExpandDirs extends ExpandDirsInstances {
  type Aux[T[_[_]], IN[_], F[_]] = ExpandDirs[IN] {
    type IT[F[_]] = T[F]
    type OUT[A] = F[A]
  }

  def apply[T[_[_]], IN[_], OUT[_]](implicit ev: ExpandDirs.Aux[T, IN, OUT]) =
    ev
}

abstract class ExpandDirsInstances {
  def expandDirsPath[T[_[_]]: BirecursiveT, O[a] <: ACopK[a]: Functor](
    implicit
    FI: Injectable[O, QScriptTotal[T, ?]],
    QC: QScriptCore[T, ?] :<<: O
  ): ExpandDirsPath[T, O] =
    new ExpandDirsPath[T, O]

  def expandDirsBranch[T[_[_]]: BirecursiveT]: ExpandDirsBranch[T] =
    new ExpandDirsBranch[T]

  // real instances

  implicit def readDir[T[_[_]]: BirecursiveT, F[a] <: ACopK[a]: Functor]
    (implicit
      R: Const[Read[AFile], ?] :<<: F,
      QC:    QScriptCore[T, ?] :<<: F,
      FI: Injectable[F, QScriptTotal[T, ?]])
      : ExpandDirs.Aux[T, Const[Read[ADir], ?], F] =
    expandDirsPath[T, F].readDir

  implicit def shiftedReadDir[T[_[_]]: BirecursiveT, F[a] <: ACopK[a]: Functor]
    (implicit
      SR: Const[ShiftedRead[AFile], ?] :<<: F,
      QC:            QScriptCore[T, ?] :<<: F,
      FI: Injectable[F, QScriptTotal[T, ?]])
      : ExpandDirs.Aux[T, Const[ShiftedRead[ADir], ?], F] =
    expandDirsPath[T, F].shiftedReadDir

  // branch handling

  implicit def qscriptCore[T[_[_]]: BirecursiveT, F[a] <: ACopK[a]](implicit QC: QScriptCore[T, ?] :<<: F)
      : ExpandDirs.Aux[T, QScriptCore[T, ?], F] =
    expandDirsBranch[T].qscriptCore[F]

  implicit def thetaJoin[T[_[_]]: BirecursiveT, F[a] <: ACopK[a]](implicit TJ: ThetaJoin[T, ?] :<<: F)
      : ExpandDirs.Aux[T, ThetaJoin[T, ?], F] =
    expandDirsBranch[T].thetaJoin[F]

  implicit def equiJoin[T[_[_]]: BirecursiveT, F[a] <: ACopK[a]](implicit EJ: EquiJoin[T, ?] :<<: F)
      : ExpandDirs.Aux[T, EquiJoin[T, ?], F] =
    expandDirsBranch[T].equiJoin[F]

  implicit def copk[T[_[_]], LL <: TListK, H[_]](implicit M: Materializer[T, LL, H]): ExpandDirs.Aux[T, CopK[LL, ?], H] =
    M.materialize(offset = 0)

  sealed trait Materializer[T[_[_]], LL <: TListK, H[_]] {
    def materialize(offset: Int): ExpandDirs.Aux[T, CopK[LL, ?], H]
  }

  object Materializer {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def base[T[_[_]], G[_], H[_]](
      implicit
      G: ExpandDirs.Aux[T, G, H]
    ): Materializer[T, G ::: TNilK, H] = new Materializer[T, G ::: TNilK, H] {
      override def materialize(offset: Int): ExpandDirs.Aux[T, CopK[G ::: TNilK, ?], H] = {
        val I = mkInject[G, G ::: TNilK](offset)
        new ExpandDirs[CopK[G ::: TNilK, ?]] {
          type IT[F[_]] = T[F]
          type OUT[A] = H[A]

          def expandDirs[M[_]: Monad: PlannerErrorME, F[_]: Functor](OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) = {
            case I(fa) => G.expandDirs(OutToF, g).apply(fa)
          }
        }
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[T[_[_]], G[_], LL <: TListK, H[_]](
      implicit
      G: ExpandDirs.Aux[T, G, H],
      LL: Materializer[T, LL, H]
    ): Materializer[T, G ::: LL, H] = new Materializer[T, G ::: LL, H] {
      override def materialize(offset: Int): ExpandDirs.Aux[T, CopK[G ::: LL, ?], H] = {
        val I = mkInject[G, G ::: LL](offset)
        new ExpandDirs[CopK[G ::: LL, ?]] {
          type IT[F[_]] = T[F]
          type OUT[A] = H[A]

          def expandDirs[M[_]: Monad: PlannerErrorME, F[_]: Functor](OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) = {
            case I(fa) => G.expandDirs(OutToF, g).apply(fa)
            case other => LL.materialize(offset + 1).expandDirs(OutToF, g).apply(other.asInstanceOf[CopK[LL, T[F]]])
          }
        }
      }
    }
  }

  def default[T[_[_]], F[_], G[a] <: ACopK[a]](implicit F: F :<<: G)
      : ExpandDirs.Aux[T, F, G] =
    new ExpandDirs[F] {
      type IT[F[_]] = T[F]
      type OUT[A] = G[A]

      def expandDirs[M[_]: Monad: PlannerErrorME, H[_]: Functor]
        (OutToF: OUT ~> H, g: DiscoverPath.ListContents[M]) =
        fa => OutToF(F.inj(fa)).point[M]
    }

  implicit def deadEnd[T[_[_]], F[a] <: ACopK[a]](implicit DE: Const[DeadEnd, ?] :<<: F)
      : ExpandDirs.Aux[T, Const[DeadEnd, ?], F] =
    default

  implicit def readFile[T[_[_]], F[a] <: ACopK[a]](implicit R: Const[Read[AFile], ?] :<<: F)
      : ExpandDirs.Aux[T, Const[Read[AFile], ?], F] =
    default

  implicit def shiftedReadFile[T[_[_]], F[a] <: ACopK[a]]
    (implicit SR: Const[ShiftedRead[AFile], ?] :<<: F)
      : ExpandDirs.Aux[T, Const[ShiftedRead[AFile], ?], F] =
    default

  implicit def projectBucket[T[_[_]], F[a] <: ACopK[a]]
    (implicit PB: ProjectBucket[T, ?] :<<: F)
      : ExpandDirs.Aux[T, ProjectBucket[T, ?], F] =
    default
}

private[qscript] final class ExpandDirsPath[T[_[_]]: BirecursiveT, O[a] <: ACopK[a]: Functor](
  implicit FI: Injectable[O, QScriptTotal[T, ?]], QC: QScriptCore[T, ?] :<<: O
) extends TTypes[T] {
  val recFunc = construction.RecFunc[T]

  def union(elems: NonEmptyList[O[T[O]]]): O[T[O]] =
    elems.foldRight1(
      (elem, acc) => QC.inj(Union(QC.inj(Unreferenced[T, T[O]]()).embed,
        elem.embed.cata[Free[QScriptTotal, Hole]](g => Free.roll(FI.inject(g))),
        acc.embed.cata[Free[QScriptTotal, Hole]](g => Free.roll(FI.inject(g))))))

  def wrapDir(name: String, d: O[T[O]]): O[T[O]] =
    QC.inj(Map(d.embed, recFunc.MakeMapS(name, recFunc.Hole)))

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def allDescendents[M[_]: Monad: PlannerErrorME]
    (listContents: DiscoverPath.ListContents[M], wrapFile: AFile => O[T[O]])
      : ADir => M[List[O[T[O]]]] =
    dir => (listContents(dir) >>=
      (ps => ISet.fromList(ps.toList).toList.traverseM(_.fold(
        d => allDescendents[M](listContents, wrapFile).apply(dir </> dir1(d)) ∘ (_ ∘ (wrapDir(d.value, _))),
        f => List(wrapDir(f.value, wrapFile(dir </> file1(f)))).point[M]))))
      .handleError(κ(List.empty[O[T[O]]].point[M]))

  def unionDirs[M[_]: Monad: PlannerErrorME]
    (g: DiscoverPath.ListContents[M], wrapFile: AFile => O[T[O]])
      : ADir => M[Option[NonEmptyList[O[T[O]]]]] =
    allDescendents[M](g, wrapFile).apply(_) ∘ {
      case Nil    => None
      case h :: t => NonEmptyList.nel(h, t.toIList).some
    }

  def unionAll[M[_]: Monad: PlannerErrorME, F[_]: Functor]
    (OutToF: O ~> F, g: DiscoverPath.ListContents[M], wrapFile: AFile => O[T[O]])
      : ADir => M[F[T[F]]] =
    dir => unionDirs[M](g, wrapFile).apply(dir) >>= (_.fold[M[F[T[F]]]](
      PlannerErrorME[M].raiseError(NoFilesFound(List(dir))))(
      nel => OutToF(union(nel) ∘ (_.transAna[T[F]](OutToF))).point[M]))


  def readDir(implicit R: Const[Read[AFile], ?] :<<: O): ExpandDirs.Aux[T, Const[Read[ADir], ?], O] =
    new ExpandDirs[Const[Read[ADir], ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = O[A]

      def wrapRead[F[_]](file: AFile) =
        R.inj(Const[Read[AFile], T[F]](Read(file)))

      def expandDirs[M[_]: Monad: PlannerErrorME, F[_]: Functor]
        (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) =
        r => unionAll(OutToF, g, wrapRead[OUT]) apply r.getConst.path
    }

  def shiftedReadDir(implicit SR: Const[ShiftedRead[AFile], ?] :<<: O): ExpandDirs.Aux[T, Const[ShiftedRead[ADir], ?], O] =
    new ExpandDirs[Const[ShiftedRead[ADir], ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = O[A]

      def wrapRead[F[_]](file: AFile, idStatus: IdStatus) =
        SR.inj(Const[ShiftedRead[AFile], T[F]](ShiftedRead(file, idStatus)))

      def expandDirs[M[_]: Monad: PlannerErrorME, F[_]: Functor]
        (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) =
        r => unionAll(OutToF, g, wrapRead[OUT](_, r.getConst.idStatus)) apply r.getConst.path
    }
}

private[qscript] final class ExpandDirsBranch[T[_[_]]: BirecursiveT] extends TTypes[T] {
  private def ExpandDirsTotal = ExpandDirs[T, QScriptTotal, QScriptTotal]

  def applyToBranch[M[_]: Monad: PlannerErrorME]
    (listContents: DiscoverPath.ListContents[M], branch: FreeQS)
      : M[FreeQS] =
    branch.transCataM[M, T[CoEnvQS], CoEnvQS](
      liftCoM[T, M, QScriptTotal, Hole, T[CoEnvQS]](
        ExpandDirsTotal.expandDirs(
          coenvPrism[QScriptTotal, Hole].reverseGet,
          listContents))
    ) ∘ (_.convertTo[FreeQS])


  def qscriptCore[O[a] <: ACopK[a]](implicit QC: QScriptCore :<<: O)
      : ExpandDirs.Aux[T, QScriptCore, O] =
    new ExpandDirs[QScriptCore] {
      type IT[F[_]] = T[F]
      type OUT[A] = O[A]

      def expandDirs[M[_]: Monad: PlannerErrorME, F[_]: Functor]
        (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) =
        fa => (fa match {
          case Union(src, lb, rb) =>
            (applyToBranch(g, lb) ⊛ applyToBranch(g, rb))(Union(src, _, _))
          case Subset(src, lb, o, rb) =>
            (applyToBranch(g, lb) ⊛ applyToBranch(g, rb))(Subset(src, _, o, _))
          case x => x.point[M]
        }) ∘ (OutToF.compose(QC.inj))
    }

  def thetaJoin[O[a] <: ACopK[a]](implicit TJ: ThetaJoin :<<: O)
      : ExpandDirs.Aux[T, ThetaJoin, O] =
    new ExpandDirs[ThetaJoin] {
      type IT[F[_]] = T[F]
      type OUT[A] = O[A]

      def expandDirs[M[_]: Monad: PlannerErrorME, F[_]: Functor]
        (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) =
        fa => (fa match {
          case ThetaJoin(src, lb, rb, on, jType, combine) =>
            (applyToBranch(g, lb) ⊛ applyToBranch(g, rb))(
              ThetaJoin(src, _, _, on, jType, combine))
        }) ∘ (OutToF.compose(TJ.inj))
    }

  def equiJoin[O[a] <: ACopK[a]](implicit EJ: EquiJoin :<<: O)
      : ExpandDirs.Aux[T, EquiJoin, O] =
    new ExpandDirs[EquiJoin] {
      type IT[F[_]] = T[F]
      type OUT[A] = O[A]

      def expandDirs[M[_]: Monad: PlannerErrorME, F[_]: Functor]
        (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) =
        fa => (fa match {
          case EquiJoin(src, lb, rb, k, jType, combine) =>
            (applyToBranch(g, lb) ⊛ applyToBranch(g, rb))(
              EquiJoin(src, _, _, k, jType, combine))
        }) ∘ (OutToF.compose(EJ.inj))
    }
}
