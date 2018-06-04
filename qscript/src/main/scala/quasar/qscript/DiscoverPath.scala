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

package quasar.qscript

import slamdata.Predef._
import quasar.fs.Planner.NoFilesFound
import quasar.contrib.pathy._
import quasar.contrib.scalaz._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.ski._
import quasar.fs._
import quasar.qscript.MapFuncsCore._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import pathy.Path.{dir1, file1, rootDir}
import scalaz._, Scalaz._, \&/._

import iotaz.TListK.:::
import iotaz.{ CopK, TListK, TNilK }

/** This extracts statically-known paths from QScript queries to make it easier
  * for connectors to map queries to their own filesystems.
  */
trait DiscoverPath[IN[_]] {
  type IT[F[_]]
  type OUT[A]

  def discoverPath[M[_]: Monad: MonadFsErr](g: DiscoverPath.ListContents[M])
      : AlgebraM[M, IN, List[ADir] \&/ IT[OUT]]
}

object DiscoverPath extends DiscoverPathInstances {
  type Aux[T[_[_]], IN[_], F[_]] = DiscoverPath[IN] {
    type IT[F[_]] = T[F]
    type OUT[A] = F[A]
  }

  type ListContents[M[_]] = ADir => M[Set[PathSegment]]

  object ListContents {
    def static[F[_]: Foldable, M[_]: Applicative](paths: F[APath]): ListContents[M] = {
      def segment(d: ADir): APath => Set[PathSegment] =
        _.relativeTo(d).flatMap(firstSegmentName).toSet

      dir => paths.foldMap(segment(dir)).point[M]
    }
  }

  def apply[T[_[_]], IN[_], OUT[_]](implicit ev: DiscoverPath.Aux[T, IN, OUT]) =
    ev

  def unionAll[T[_[_]]: BirecursiveT, M[_]: Monad: MonadFsErr, OUT[a] <: ACopK[a] : Functor]
    (g: ListContents[M])
    (implicit
      RD:  Const[Read[ADir], ?] :<<: OUT,
      RF: Const[Read[AFile], ?] :<<: OUT,
      QC:     QScriptCore[T, ?] :<<: OUT,
      FI: Injectable[OUT, QScriptTotal[T, ?]])
      : List[ADir] \&/ T[OUT] => M[T[OUT]] =
    discoverPath[T, OUT].unionAll[M](g)
}

abstract class DiscoverPathInstances {
  import DiscoverPath.ListContents

  def discoverPath[T[_[_]]: BirecursiveT, O[a] <: ACopK[a] : Functor]
    (implicit
      RD:  Const[Read[ADir], ?] :<<: O,
      RF: Const[Read[AFile], ?] :<<: O,
      QC:     QScriptCore[T, ?] :<<: O,
      FI: Injectable[O, QScriptTotal[T, ?]]) =
    new DiscoverPathT[T, O]

  // real instances

  implicit def deadEnd[T[_[_]], F[_]]: DiscoverPath.Aux[T, Const[DeadEnd, ?], F] =
    new DiscoverPath[Const[DeadEnd, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def discoverPath[M[_]: Monad: MonadFsErr](g: ListContents[M]) =
        κ(-\&/[List[ADir], T[OUT]](List(rootDir)).point[M])
    }

  implicit def projectBucket[T[_[_]]: BirecursiveT, F[a] <: ACopK[a] : Functor]
    (implicit
      RD:  Const[Read[ADir], ?] :<<: F,
      RF: Const[Read[AFile], ?] :<<: F,
      QC:     QScriptCore[T, ?] :<<: F,
      PB:   ProjectBucket[T, ?] :<<: F,
      FI: Injectable[F, QScriptTotal[T, ?]])
      : DiscoverPath.Aux[T, ProjectBucket[T, ?], F] =
    discoverPath[T, F].projectBucket

  implicit def qscriptCore[T[_[_]]: BirecursiveT, F[a] <: ACopK[a] : Functor]
    (implicit
      RD:  Const[Read[ADir], ?] :<<: F,
      RF: Const[Read[AFile], ?] :<<: F,
      QC:     QScriptCore[T, ?] :<<: F,
      FI: Injectable[F, QScriptTotal[T, ?]])
      : DiscoverPath.Aux[T, QScriptCore[T, ?], F] =
    discoverPath[T, F].qscriptCore

  // branch handling

  implicit def thetaJoin[T[_[_]]: BirecursiveT, F[a] <: ACopK[a] : Functor]
    (implicit
      RD:  Const[Read[ADir], ?] :<<: F,
      RF: Const[Read[AFile], ?] :<<: F,
      QC:     QScriptCore[T, ?] :<<: F,
      TJ:       ThetaJoin[T, ?] :<<: F,
      FI: Injectable[F, QScriptTotal[T, ?]])
      : DiscoverPath.Aux[T, ThetaJoin[T, ?], F] =
    discoverPath[T, F].thetaJoin

  implicit def equiJoin[T[_[_]]: BirecursiveT, F[a] <: ACopK[a] : Functor]
    (implicit
      RD:  Const[Read[ADir], ?] :<<: F,
      RF: Const[Read[AFile], ?] :<<: F,
      QC:     QScriptCore[T, ?] :<<: F,
      EJ:        EquiJoin[T, ?] :<<: F,
      FI: Injectable[F, QScriptTotal[T, ?]])
      : DiscoverPath.Aux[T, EquiJoin[T, ?], F] =
    discoverPath[T, F].equiJoin

  implicit def copk[T[_[_]], LL <: TListK, H[_]](implicit M: Materializer[T, LL, H]): DiscoverPath.Aux[T, CopK[LL, ?], H] =
    M.materialize(offset = 0)

  sealed trait Materializer[T[_[_]], LL <: TListK, H[_]] {
    def materialize(offset: Int): DiscoverPath.Aux[T, CopK[LL, ?], H]
  }

  object Materializer {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def base[T[_[_]], F[_], H[_]](
      implicit
      F: DiscoverPath.Aux[T, F, H]
    ): Materializer[T, F ::: TNilK, H] = new Materializer[T, F ::: TNilK, H] {
      override def materialize(offset: Int): DiscoverPath.Aux[T, CopK[F ::: TNilK, ?], H] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new DiscoverPath[CopK[F ::: TNilK, ?]] {
          type IT[X[_]] = T[X]
          type OUT[A] = H[A]
          override def discoverPath[M[_]: Monad: MonadFsErr](g: ListContents[M]) = {
            case I(fa) => F.discoverPath(g).apply(fa)
          }
        }
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[T[_[_]], F[_], LL <: TListK, H[_]](
      implicit
      F: DiscoverPath.Aux[T, F, H],
      LL: Materializer[T, LL, H]
    ): Materializer[T, F ::: LL, H] = new Materializer[T, F ::: LL, H] {
      override def materialize(offset: Int): DiscoverPath.Aux[T, CopK[F ::: LL, ?], H] = {
        val I = mkInject[F, F ::: LL](offset)
        new DiscoverPath[CopK[F ::: LL, ?]] {
          type IT[X[_]] = T[X]
          type OUT[A] = H[A]
          override def discoverPath[M[_]: Monad: MonadFsErr](g: ListContents[M]) = {
            case I(fa) => F.discoverPath(g).apply(fa)
            case other => LL.materialize(offset + 1).discoverPath(g).apply(other.asInstanceOf[CopK[LL, List[ADir] \&/ IT[OUT]]])
          }
        }
      }
    }
  }

  implicit def read[T[_[_]]: BirecursiveT, F[a] <: ACopK[a] : Functor, A]
    (implicit
      RD:  Const[Read[ADir], ?] :<<: F,
      RF: Const[Read[AFile], ?] :<<: F,
      QC:     QScriptCore[T, ?] :<<: F,
      RA:     Const[Read[A], ?] :<<: F,
      FI: Injectable[F, QScriptTotal[T, ?]])
      : DiscoverPath.Aux[T, Const[Read[A], ?], F] =
    discoverPath[T, F].default[Const[Read[A], ?]]

  implicit def shiftedRead[T[_[_]]: BirecursiveT, F[a] <: ACopK[a] : Functor, A]
    (implicit
      RD:     Const[Read[ADir], ?] :<<: F,
      RF:    Const[Read[AFile], ?] :<<: F,
      QC:        QScriptCore[T, ?] :<<: F,
      IN: Const[ShiftedRead[A], ?] :<<: F,
      FI: Injectable[F, QScriptTotal[T, ?]])
      : DiscoverPath.Aux[T, Const[ShiftedRead[A], ?], F] =
    discoverPath[T, F].default[Const[ShiftedRead[A], ?]]
}

private[qscript] final class DiscoverPathT[T[_[_]]: BirecursiveT, O[a] <: ACopK[a] : Functor](
  implicit
  RD:  Const[Read[ADir], ?] :<<: O,
  RF: Const[Read[AFile], ?] :<<: O,
  QC:     QScriptCore[T, ?] :<<: O,
  FI: Injectable[O, QScriptTotal[T, ?]]
) extends TTypes[T] {
  import DiscoverPath.ListContents

  private val recFunc = construction.RecFunc[T]
  private def DiscoverPathTotal = DiscoverPath[T, QScriptTotal, QScriptTotal]
  private def DiscoverPathTTotal = new DiscoverPathT[T, QScriptTotal]

  private def union(elems: NonEmptyList[T[O]]): T[O] =
    elems.foldRight1(
      (elem, acc) => QC.inj(Union(QC.inj(Unreferenced[T, T[O]]()).embed,
        elem.cata[FreeQS](g => Free.roll(FI.inject(g))),
        acc.cata[FreeQS](g => Free.roll(FI.inject(g))))).embed)

  private def makeRead[F[a] <: ACopK[a]](path: ADir)(implicit R: Const[Read[ADir], ?] :<<: F):
      F[T[F]] =
    R.inj(Const[Read[ADir], T[F]](Read(path)))

  private def wrapDir[F[a] <: ACopK[a] : Functor]
    (name: String, d: F[T[F]])
    (implicit QC: QScriptCore :<<: F)
      : F[T[F]] =
    QC.inj(Map(d.embed, recFunc.MakeMapS(name, recFunc.Hole)))

  private val unionDirs: List[ADir] => Option[NonEmptyList[T[O]]] =
    _ ∘ (makeRead[O](_).embed) match {
      case Nil    => None
      case h :: t => NonEmptyList.nel(h, t.toIList).some
    }

  def unionAll[M[_]: Monad: MonadFsErr](g: ListContents[M]): List[ADir] \&/ T[O] => M[T[O]] =
    _.fold(
      ds => unionDirs(ds).fold[M[T[O]]](
        MonadFsErr[M].raiseError(FileSystemError.qscriptPlanningFailed(NoFilesFound(ds))))(
        union(_).point[M]),
      _.point[M],
      (ds, qs) => unionDirs(ds).fold(qs)(d => union(qs <:: d)).point[M])

  private def convertBranch[M[_]: Monad: MonadFsErr]
    (src: List[ADir] \&/ T[O], branch: FreeQS)
    (f: ListContents[M])
      : M[FreeQS] =
    branch.cataM[M, List[ADir] \&/ T[QScriptTotal]](
      interpretM[M, QScriptTotal, Hole, List[ADir] \&/ T[QScriptTotal]](
        κ((src ∘ (_.transCata[T[QScriptTotal]](FI.inject))).point[M]),
        DiscoverPathTotal.discoverPath(f))) >>=
      (DiscoverPathTTotal.unionAll[M](f).apply(_) ∘ (_.cata(Free.roll[QScriptTotal, Hole])))

  private def convertBranchingOp[M[_]: Monad: MonadFsErr]
    (src: List[ADir] \&/ T[O], lb: FreeQS, rb: FreeQS, f: ListContents[M])
    (op: (T[O], FreeQS, FreeQS) => O[T[O]])
      : M[List[ADir] \&/ T[O]] =
    (convertBranch[M](src, lb)(f) ⊛ convertBranch[M](src, rb)(f))((l, r) =>
      \&/-(op(QC.inj(Unreferenced[T, T[O]]()).embed, l, r).embed))

  def fileType[M[_]: Monad: MonadFsErr](listContents: ListContents[M]):
      (ADir, String) => OptionT[M, ADir \/ AFile] =
    (dir, name) => OptionT(MonadFsErr[M].handleError(listContents(dir).map(_.some))(κ(none.point[M]))) >>=
      (cont => OptionT((cont.find(_.fold(_.value ≟ name, _.value ≟ name)) ∘
        (_.bimap(dir </> dir1(_), dir </> file1(_)))).point[M]))

  // real instances

  def projectBucket(implicit PB: ProjectBucket :<<: O): DiscoverPath.Aux[T, ProjectBucket, O] =
    new DiscoverPath[ProjectBucket] {
      type IT[F[_]] = T[F]
      type OUT[A] = O[A]

      def handleDirs[M[_]: Monad: MonadFsErr](
        g: ListContents[M],
        dirs: List[ADir],
        key: String)
          : M[List[ADir] \&/ T[OUT]] =
        dirs.traverseM(fileType(g).apply(_, key).fold(
          df => List(df ∘ (file => RF.inj(Const[Read[AFile], T[OUT]](Read(file))).embed)),
          Nil)) ∘ {
          case Nil => -\&/(Nil)
          case h :: t => t.foldRight(h.fold(d => -\&/(List(d)), \&/-(_)))((elem, acc) =>
            elem.fold(
              d => acc match {
                case This(ds) => -\&/(d :: ds)
                case That(qs) => Both(List(d), qs)
                case Both(ds, qs) => Both(d :: ds, qs)
              },
              f => acc match {
                case This(ds) => Both(ds, f)
                case That(qs) => That(union(NonEmptyList(f, qs)))
                case Both(ds, qs) => Both(ds, union(NonEmptyList(f, qs)))
              }))
        }

      def rebucket(out: T[OUT], value: FreeMap, key: String) =
        PB.inj(BucketKey(out, value, StrLit(key))).embed

      def discoverPath[M[_]: Monad: MonadFsErr](g: ListContents[M]) = {
        // FIXME: `value` must be `HoleF`.
        case BucketKey(src, value, StrLit(key)) =>
          src.fold(
            handleDirs(g, _, key),
            out => \&/-(rebucket(out, value, key)).point[M],
            (dirs, out) => handleDirs(g, dirs, key) ∘ {
              case This(dirs)        => Both(dirs, rebucket(out, value, key))
              case That(files)       => That(union(NonEmptyList(files, rebucket(out, value, key))))
              case Both(dirs, files) => Both(dirs, union(NonEmptyList(files, rebucket(out, value, key))))
            })
        case x => x.traverse(unionAll[M](g)) ∘ (in => \&/-(PB.inj(in).embed))
      }
    }

  def qscriptCore: DiscoverPath.Aux[T, QScriptCore, O] =
    new DiscoverPath[QScriptCore] {
      type IT[F[_]] = T[F]
      type OUT[A] = O[A]

      def discoverPath[M[_]: Monad: MonadFsErr](g: ListContents[M]) = {
        case Union(src, lb, rb) if !src.isThat =>
          convertBranchingOp(src, lb, rb, g)((s, l, r) =>
            QC.inj(Union(s, l, r)))
        case Subset(src, lb, sel, rb) if !src.isThat =>
          convertBranchingOp(src, lb, rb, g)((s, l, r) =>
            QC.inj(Subset(s, l, sel, r)))

        case x => x.traverse(unionAll[M](g)) ∘ (in => \&/-(QC.inj(in).embed))
      }
    }

  // branch handling

  def thetaJoin(implicit TJ: ThetaJoin :<<: O): DiscoverPath.Aux[T, ThetaJoin, O] =
    new DiscoverPath[ThetaJoin] {
      type IT[F[_]] = T[F]
      type OUT[A] = O[A]

      def discoverPath[M[_]: Monad: MonadFsErr](g: ListContents[M]) = {
        case ThetaJoin(src, lb, rb, on, jType, combine) if !src.isThat =>
          convertBranchingOp(src, lb, rb, g)((s, l, r) =>
            TJ.inj(ThetaJoin(s, l, r, on, jType, combine)))
        case x => x.traverse(unionAll[M](g)) ∘ (in => \&/-(TJ.inj(in).embed))
      }
    }

  def equiJoin(implicit EJ: EquiJoin :<<: O): DiscoverPath.Aux[T, EquiJoin, O] =
    new DiscoverPath[EquiJoin] {
      type IT[F[_]] = T[F]
      type OUT[A] = O[A]

      def discoverPath[M[_]: Monad: MonadFsErr](g: ListContents[M]) = {
        case EquiJoin(src, lb, rb, k, jType, combine) if !src.isThat =>
          convertBranchingOp(src, lb, rb, g)((s, l, r) =>
            EJ.inj(EquiJoin(s, l, r, k, jType, combine)))
        case x => x.traverse(unionAll[M](g)) ∘ (in => \&/-(EJ.inj(in).embed))
      }
    }

  def default[IN[_]: Traverse](implicit IN: IN :<<: O): DiscoverPath.Aux[T, IN, O] =
    new DiscoverPath[IN] {
      type IT[F[_]] = T[F]
      type OUT[A] = O[A]

      def discoverPath[M[_]: Monad: MonadFsErr](g: ListContents[M]) =
        _.traverse(unionAll[M](g)) ∘ (in => \&/-(IN.inj(in).embed))
    }
}
