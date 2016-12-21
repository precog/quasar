/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._
import quasar.Planner.NoFilesFound
import quasar.contrib.pathy._
import quasar.fp._
import quasar.fp.ski._
import quasar.fs._
import quasar.qscript.MapFuncs._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import pathy.Path.{dir1, file1, FileName, rootDir}
import scalaz._, Scalaz._, \&/._

/** This extracts statically-known paths from QScript queries to make it easier
  * for connectors to map queries to their own filesystems.
  */
trait DiscoverPath[IN[_]] {
  type IT[F[_]]
  type OUT[A]

  def discoverPath[M[_]: MonadFsErr](g: DiscoverPath.ListContents[M])
      : AlgebraM[M, IN, List[ADir] \&/ IT[OUT]]
}

object DiscoverPath extends DiscoverPathInstances {
  type Aux[T[_[_]], IN[_], F[_]] = DiscoverPath[IN] {
    type IT[F[_]] = T[F]
    type OUT[A] = F[A]
  }

  def apply[T[_[_]], IN[_], OUT[_]](implicit ev: DiscoverPath.Aux[T, IN, OUT]) =
    ev
}

abstract class DiscoverPathInstances {
  type ListContents[M[_]] = ADir => M[Set[PathSegment]]

  // TODO: Move to scalaz, or at least quasar.fp
  def -\&/[A, B](a: A): These[A, B] = This(a)
  def \&/-[A, B](b: B): These[A, B] = That(b)

  private def union[T[_[_]]: BirecursiveT, OUT[_]: Functor]
    (elems: NonEmptyList[T[OUT]])
    (implicit
      QC: QScriptCore[T, ?] :<: OUT,
      FI: Injectable.Aux[OUT, QScriptTotal[T, ?]])
      : T[OUT] =
    elems.foldRight1(
      (elem, acc) => QC.inj(Union(QC.inj(Unreferenced[T, T[OUT]]()).embed,
        elem.cata[FreeQS[T]](g => Free.roll(FI.inject(g))),
        acc.cata[FreeQS[T]](g => Free.roll(FI.inject(g))))).embed)

  private def makeRead[T[_[_]], F[_]]
    (dir: ADir, file: FileName)
    (implicit R: Const[Read, ?] :<: F):
      F[T[F]] =
    R.inj(Const[Read, T[F]](Read(dir </> file1(file))))

  private def wrapDir[T[_[_]]: CorecursiveT, F[_]: Functor]
    (name: String, d: F[T[F]])
    (implicit QC: QScriptCore[T, ?] :<: F)
      : F[T[F]] =
    QC.inj(Map(d.embed, Free.roll(MakeMap(StrLit(name), HoleF))))

  // TODO: Some connectors (notably MongoDB) could provide more efficient
  //       implementations of this.
  private def allDescendents[T[_[_]]: CorecursiveT, M[_]: MonadFsErr, F[_]: Functor](
    listContents: ListContents[M])(
    implicit R: Const[Read, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F):
      ADir => M[List[F[T[F]]]] =
    dir => (listContents(dir) >>=
      (ps => ISet.fromList(ps.toList).toList.traverseM(_.fold(
        d => allDescendents[T, M, F](listContents).apply(dir </> dir1(d)) ∘ (_ ∘ (wrapDir(d.value, _))),
        f => List(wrapDir[T, F](f.value, makeRead(dir, f))).point[M]))))
      .handleError(κ(List.empty[F[T[F]]].point[M]))

  private def unionDirs[T[_[_]]: CorecursiveT, M[_]: MonadFsErr, OUT[_]: Functor]
    (g: ListContents[M])
    (implicit R: Const[Read, ?] :<: OUT, QC: QScriptCore[T, ?] :<: OUT)
      : List[ADir] => M[Option[NonEmptyList[T[OUT]]]] =
    dirs => dirs.traverseM(allDescendents[T, M, OUT](g)) ∘ (_ ∘ (_.embed) match {
      case Nil    => None
      case h :: t => NonEmptyList.nel(h, t.toIList).some
    })

  def unionAll[T[_[_]]: BirecursiveT, M[_]: MonadFsErr, OUT[_]: Functor]
    (g: ListContents[M])
    (implicit
      R:     Const[Read, ?] :<: OUT,
      QC: QScriptCore[T, ?] :<: OUT,
      FI: Injectable.Aux[OUT, QScriptTotal[T, ?]])
      : List[ADir] \&/ T[OUT] => M[T[OUT]] =
    _.fold(
      ds => unionDirs[T, M, OUT](g).apply(ds) >>= (_.fold[M[T[OUT]]](
        MonadError[M, FileSystemError].raiseError(FileSystemError.qscriptPlanningFailed(NoFilesFound(ds))))(
        union(_).point[M])),
      _.point[M],
      (ds, qs) => unionDirs[T, M, OUT](g).apply(ds) ∘ (_.fold(qs)(d => union(qs <:: d))))

  private def convertBranch
    [T[_[_]]: BirecursiveT, M[_]: MonadFsErr, OUT[_]: Functor]
    (src: List[ADir] \&/ T[OUT], branch: FreeQS[T])
    (f: ListContents[M])
    (implicit
      R:     Const[Read, ?] :<: OUT,
      QC: QScriptCore[T, ?] :<: OUT,
      FI: Injectable.Aux[OUT, QScriptTotal[T, ?]])
      : M[FreeQS[T]] =
    branch.cataM[M, List[ADir] \&/ T[QScriptTotal[T, ?]]](
      interpretM(
        κ((src ∘ (_.transCata[T[QScriptTotal[T, ?]]](FI.inject))).point[M]),
        DiscoverPath[T, QScriptTotal[T, ?], QScriptTotal[T, ?]].discoverPath(f))) >>=
      (unionAll[T, M, QScriptTotal[T, ?]](f).apply(_) ∘ (_.cata(Free.roll[QScriptTotal[T, ?], Hole])))


  private def convertBranchingOp
    [T[_[_]]: BirecursiveT, M[_]: MonadFsErr, OUT[_]: Functor]
    (src: List[ADir] \&/ T[OUT], lb: FreeQS[T], rb: FreeQS[T], f: ListContents[M])
    (op: (T[OUT], FreeQS[T], FreeQS[T]) => OUT[T[OUT]])
    (implicit
      R:     Const[Read, ?] :<: OUT,
      QC: QScriptCore[T, ?] :<: OUT,
      FI: Injectable.Aux[OUT, QScriptTotal[T, ?]])
      : M[List[ADir] \&/ T[OUT]] =
    (convertBranch(src, lb)(f) ⊛ convertBranch(src, rb)(f))((l, r) =>
      \&/-(op(QC.inj(Unreferenced[T, T[OUT]]()).embed, l, r).embed))

  def fileType[M[_]: MonadFsErr](listContents: ListContents[M]):
      (ADir, String) => OptionT[M, ADir \/ AFile] =
    (dir, name) => OptionT(listContents(dir).map(_.some).handleError(κ(none.point[M]))) >>=
      (cont => OptionT((cont.find(_.fold(_.value ≟ name, _.value ≟ name)) ∘
        (_.bimap(dir </> dir1(_), dir </> file1(_)))).point[M]))

  // real instances

  implicit def root[T[_[_]], F[_]]: DiscoverPath.Aux[T, Const[DeadEnd, ?], F] =
    new DiscoverPath[Const[DeadEnd, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def discoverPath[M[_]: MonadFsErr](g: ListContents[M]) =
        κ(-\&/[List[ADir], T[OUT]](List(rootDir)).point[M])
    }

  implicit def projectBucket[T[_[_]]: BirecursiveT, F[_]: Functor]
    (implicit
      R:       Const[Read, ?] :<: F,
      QC:   QScriptCore[T, ?] :<: F,
      PB: ProjectBucket[T, ?] :<: F,
      FI: Injectable.Aux[F, QScriptTotal[T, ?]])
      : DiscoverPath.Aux[T, ProjectBucket[T, ?], F] =
    new DiscoverPath[ProjectBucket[T, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def handleDirs[M[_]: MonadFsErr](g: ListContents[M], dirs: List[ADir], field: String) =
        dirs.traverseM(fileType(g).apply(_, field).fold(
          df => List(df ∘ (file => R.inj(Const[Read, T[OUT]](Read(file))).embed)),
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

      def rebucket(out: T[OUT], value: FreeMap[T], field: String) =
        PB.inj(BucketField(out, value, StrLit(field))).embed

      def discoverPath[M[_]: MonadFsErr](g: ListContents[M]) = {
        // FIXME: `value` must be `HoleF`.
        case BucketField(src, value, StrLit(field)) =>
          src.fold(
            handleDirs(g, _, field),
            out => \&/-(rebucket(out, value, field)).point[M],
            (dirs, out) => handleDirs(g, dirs, field) ∘ {
              case This(dirs)        => Both(dirs, rebucket(out, value, field))
              case That(files)       => That(union(NonEmptyList(files, rebucket(out, value, field))))
              case Both(dirs, files) => Both(dirs, union(NonEmptyList(files, rebucket(out, value, field))))
            })
        case x => x.traverse(unionAll(g)) ∘ (in => \&/-(PB.inj(in).embed))
      }
    }

  implicit def qscriptCore[T[_[_]]: BirecursiveT, F[_]: Functor]
    (implicit
      R:     Const[Read, ?] :<: F,
      QC: QScriptCore[T, ?] :<: F,
      FI: Injectable.Aux[F, QScriptTotal[T, ?]])
      : DiscoverPath.Aux[T, QScriptCore[T, ?], F] =
    new DiscoverPath[QScriptCore[T, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def discoverPath[M[_]: MonadFsErr](g: ListContents[M]) = {
        case Union(src, lb, rb) if !src.isThat =>
          convertBranchingOp(src, lb, rb, g)((s, l, r) =>
            QC.inj(Union(s, l, r)))
        case Subset(src, lb, sel, rb) if !src.isThat =>
          convertBranchingOp(src, lb, rb, g)((s, l, r) =>
            QC.inj(Subset(s, l, sel, r)))

        case x => x.traverse(unionAll(g)) ∘ (in => \&/-(QC.inj(in).embed))
      }
    }

  // branch handling

  implicit def thetaJoin[T[_[_]]: BirecursiveT, F[_]: Functor]
    (implicit
      R:     Const[Read, ?] :<: F,
      QC: QScriptCore[T, ?] :<: F,
      TJ: ThetaJoin[T, ?] :<: F,
      FI: Injectable.Aux[F, QScriptTotal[T, ?]])
      : DiscoverPath.Aux[T, ThetaJoin[T, ?], F] =
    new DiscoverPath[ThetaJoin[T, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def discoverPath[M[_]: MonadFsErr](g: ListContents[M]) = {
        case ThetaJoin(src, lb, rb, on, jType, combine) if !src.isThat =>
          convertBranchingOp(src, lb, rb, g)((s, l, r) =>
            TJ.inj(ThetaJoin(s, l, r, on, jType, combine)))
        case x => x.traverse(unionAll(g)) ∘ (in => \&/-(TJ.inj(in).embed))
      }
    }

  implicit def equiJoin[T[_[_]]: BirecursiveT, F[_]: Functor]
    (implicit
      R:     Const[Read, ?] :<: F,
      QC: QScriptCore[T, ?] :<: F,
      EJ: EquiJoin[T, ?] :<: F,
      FI: Injectable.Aux[F, QScriptTotal[T, ?]])
      : DiscoverPath.Aux[T, EquiJoin[T, ?], F] =
    new DiscoverPath[EquiJoin[T, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def discoverPath[M[_]: MonadFsErr](g: ListContents[M]) = {
        case EquiJoin(src, lb, rb, lk, rk, jType, combine) if !src.isThat =>
          convertBranchingOp(src, lb, rb, g)((s, l, r) =>
            EJ.inj(EquiJoin(s, l, r, lk, rk, jType, combine)))
        case x => x.traverse(unionAll(g)) ∘ (in => \&/-(EJ.inj(in).embed))
      }
    }

  implicit def coproduct[T[_[_]], F[_], G[_], H[_]]
    (implicit F: DiscoverPath.Aux[T, F, H], G: DiscoverPath.Aux[T, G, H])
      : DiscoverPath.Aux[T, Coproduct[F, G, ?], H] =
    new DiscoverPath[Coproduct[F, G, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = H[A]

      def discoverPath[M[_]: MonadFsErr](g: ListContents[M]) =
        _.run.fold(F.discoverPath(g), G.discoverPath(g))
    }

  def default[T[_[_]]: BirecursiveT, IN[_]: Traverse, F[_]: Functor]
    (implicit
      R:     Const[Read, ?] :<: F,
      QC: QScriptCore[T, ?] :<: F,
      IN:                IN :<: F,
      FI: Injectable.Aux[F, QScriptTotal[T, ?]])
      : DiscoverPath.Aux[T, IN, F] =
    new DiscoverPath[IN] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def discoverPath[M[_]: MonadFsErr](g: ListContents[M]) =
        _.traverse(unionAll(g)) ∘ (in => \&/-(IN.inj(in).embed))
    }

  implicit def read[T[_[_]]: BirecursiveT, F[_]: Functor]
    (implicit
      R:     Const[Read, ?] :<: F,
      QC: QScriptCore[T, ?] :<: F,
      FI: Injectable.Aux[F, QScriptTotal[T, ?]])
      : DiscoverPath.Aux[T, Const[Read, ?], F] =
    default

  implicit def shiftedRead[T[_[_]]: BirecursiveT, F[_]: Functor]
    (implicit
      R:         Const[Read, ?] :<: F,
      QC:     QScriptCore[T, ?] :<: F,
      IN: Const[ShiftedRead, ?] :<: F,
      FI: Injectable.Aux[F, QScriptTotal[T, ?]])
      : DiscoverPath.Aux[T, Const[ShiftedRead, ?], F] =
    default
}
