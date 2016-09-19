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
import quasar.Planner.{NoFilesFound, PlannerError}
import quasar.contrib.pathy._
import quasar.fp._
import quasar.fs.{FileSystemError, MonadFsErr}
import quasar.qscript.MapFuncs._

import matryoshka._, Recursive.ops._, FunctorT.ops._
import matryoshka.patterns._
import pathy.Path._
import scalaz._, Scalaz._

// NB: Should be a type class, except for it being multi-parameter
trait ConvertPath[T[_[_]], F[_]] {
  import ConvertPath._

  type H[A]

  def convertPath[M[_]: MonadFsErr](f: ListContents[M]):
      ConvertPath.StaticPathTransformation[T, M, F, H]

  def fileType[M[_]: Monad](listContents: ListContents[M]):
      (ADir, String) => OptionT[M, ADir \/ AFile] =
    (dir, name) => listContents(dir).liftM[OptionT] >>=
      (cont => OptionT((cont.find(_.fold(_.value ≟ name, _.value ≟ name)) ∘
        (_.bimap(dir </> dir1(_), dir </> file1(_)))).point[M]))
}

object ConvertPath extends ConvertPathInstances {
  type Aux[T[_[_]], F[_], G[_]] = ConvertPath[T, F] { type H[A] = G[A] }

  type ListContents[M[_]] = ADir => M[Set[PathSegment]]

  /** A function that converts a portion of QScript to statically-known paths.
    *
    *     ProjectField(LeftShift(ProjectField(Root, StrLit("foo")),
    *                            HoleF,
    *                            RightSide),
    *                  StrLit("bar"))
    *
    * Potentially represents the path “/foo/\*\/bar”, but it could also
    * represent “/foo/\*” followed by a projection into the data. Or it could
    * represent some mixture of files at “/foo/_” referencing a record with id
    * “bar” and files at “/foo/_/bar”. It’s up to a particular mount to tell the
    * compiler which of these is the case, and it does so by providing a
    * function with this type.
    *
    * A particular mount might return a structure like this:
    *
    *     [-\/(“/foo/a/bar”),
    *      -\/("/foo/b/bar"),
    *      \/-(ProjectField([-\/(“/foo/c”), -\/(“/foo/d”)], StrLit("bar"))),
    *      -\/("/foo/e/bar"),
    *      \/-(Map([-\/("/foo/f/bar/baz")], MakeMap(StrLit("baz"), SrcHole))),
    *      \/-(Map([-\/("/foo/f/bar/quux/ducks")],
    *              MakeMap(StrLit("quux"), MakeMap(StrLit("ducks"), SrcHole))))]
    *
    * Starting from Root becoming “/”, the first projection resulted in the
    * static path “/foo”. The LeftShift then collected everything from the next
    * level of the file system – “a”, “b”, “c”, “d”, “e”, and “f”, where “c” and
    * “d” are files while the others are directories. This means that the final
    * projection is handled differently in the two contexts – on the files it
    * remains a projection, an operation to be performed on the records in a
    * file, while on the directories, it becomes another path component.
    * Finally, the path “/foo/f/bar/” is still a directory, so we recursively
    * collect all the files under that directory, and rebuild the structure of
    * them in data.
    */
  type StaticPathTransformation[T[_[_]], M[_], F[_], G[_]] =
    AlgebraicTransformM[T, M, F, Pathed[G, ?]]

  def apply[T[_[_]], F[_]](implicit ev: ConvertPath[T, F]): ConvertPath[T, F] = ev

  implicit def deadEnd[T[_[_]], G[_]]: ConvertPath.Aux[T, Const[DeadEnd, ?], G] =
    new ConvertPath[T, Const[DeadEnd, ?]] {
      type H[A] = G[A]

      def convertPath[M[_]: MonadFsErr](f: ListContents[M]): StaticPathTransformation[T, M, Const[DeadEnd, ?], G] =
        κ(List(CoEnv(rootDir[Sandboxed].left[G[T[Pathed[G, ?]]]])).point[M])
    }

  def wrapDir[T[_[_]]: Corecursive, F[_]: Functor](
    name: String, d: F[T[F]])(
    implicit QC: QScriptCore[T, ?] :<: F):
      F[T[F]] =
    QC.inj(Map(d.embed, Free.roll(MapFuncs.MakeMap(MapFuncs.StrLit(name), HoleF))))

  def makeRead[T[_[_]], F[_]](
    dir: ADir, file: FileName)(
    implicit R: Const[Read, ?] :<: F):
      F[T[F]] =
    R.inj(Const[Read, T[F]](Read(dir </> file1(file))))

  def allDescendents[T[_[_]]: Corecursive, M[_]: Monad, F[_]: Functor](
    listContents: ListContents[M])(
    implicit R: Const[Read, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F):
      ADir => M[List[F[T[F]]]] =
    dir => listContents(dir) flatMap { ps =>
      ISet.fromList(ps.toList).toList.traverseM(_.fold(
        d => allDescendents[T, M, F](listContents).apply(dir </> dir1(d)) ∘ (_ ∘ (wrapDir(d.value, _))),
        f => List(wrapDir[T, F](f.value, makeRead(dir, f))).point[M]))
    }

  def readFile[T[_[_]]: Corecursive, M[_]: Monad, F[_], G[_]: Functor](
    ls: ListContents[M])(
    implicit R: Const[Read, ?] :<: G, QC: QScriptCore[T, ?] :<: G, F: F ~> G):
      CoEnv[ADir, F, T[G]] => M[List[G[T[G]]]] =
    _.run.fold(allDescendents[T, M, G](ls).apply(_), fa => List(F(fa)).point[M])

  def union[T[_[_]]: Recursive: Corecursive, F[_]: Functor]
    (elems: NonEmptyList[F[T[F]]])
    (implicit
      QC: QScriptCore[T, ?] :<: F,
      FI: F :<: QScriptTotal[T, ?])
      : F[T[F]] =
    elems.foldRight1(
      (elem, acc) => QC.inj(Union(QC.inj(Unreferenced[T, T[F]]()).embed,
        elem.embed.cata[Free[QScriptTotal[T, ?], Hole]](g => Free.roll(FI.inj(g))),
        acc.embed.cata[Free[QScriptTotal[T, ?], Hole]](g => Free.roll(FI.inj(g))))))

  /** Applied after the backend-supplied function, to convert the files into
    * reads and combine them as necessary.
    */
  def postPathify[T[_[_]]: Recursive: Corecursive, M[_]: Monad, F[_], G[_]: Functor](
    ls: ListContents[M])(
    implicit R: Const[Read, ?] :<: G,
             FG: F ~> G,
             QC: QScriptCore[T, ?] :<: G,
             FI: G :<: QScriptTotal[T, ?]):
      AlgebraicTransformM[T, EitherT[M, PlannerError, ?], Pathed[F, ?], G] =
    p => EitherT(p.traverseM(readFile[T, M, F, G](ls).apply) ∘ {
      case Nil => NoFilesFound(p.foldMap(_.run.fold(List(_), κ(Nil)))).left
      case h :: t => union[T, G](NonEmptyList.nel(h, t.toIList)).right
    })

  def convertBranch[T[_[_]]: Recursive: Corecursive, M[_]: MonadFsErr, F[_]: Functor]
    (src: T[Pathed[F, ?]], branch: FreeQS[T])
    (ls: ListContents[M])
    (implicit
      CP: ConvertPath.Aux[T, QScriptTotal[T, ?], QScriptTotal[T, ?]],
      FI: F :<: QScriptTotal[T, ?])
      : M[FreeQS[T]] = {
    implicit val FG = NaturalTransformation.refl[QScriptTotal[T, ?]]

    val plugHole =
      FunctorT[T].transCata[Pathed[F, ?], Pathed[QScriptTotal[T, ?], ?]](src)(_.map(coEnvHmap(_)(FI))).point[M]

    val convertPathAlg =
      transformToAlgebra[T, Id, M, QScriptTotal[T, ?], Pathed[QScriptTotal[T, ?], ?]](CP.convertPath(ls))

    val pathedBranch =
      freeGcataM[Id, M, QScriptTotal[T, ?], Hole, T[Pathed[QScriptTotal [T, ?], ?]]](
        branch)(distCata, ginterpretM(κ(plugHole), convertPathAlg))

    def postProcess(pathed: T[Pathed[QScriptTotal[T, ?], ?]]): M[T[QScriptTotal[T, ?]]] =
      TraverseT[T].transCataM[EitherT[M, PlannerError, ?], Pathed[QScriptTotal[T, ?], ?], QScriptTotal[T, ?]](
        pathed)(postPathify[T, M, QScriptTotal[T, ?], QScriptTotal[T, ?]](ls))
        .run.flatMap(_.fold(
          FileSystemError.qscriptPlanningFailed(_).raiseError[M, T[QScriptTotal[T, ?]]],
          _.point[M]))

    (pathedBranch >>= postProcess) ∘ (_.convertTo[Free[?[_], Hole]])
  }

  def convertBranchingOp[T[_[_]]: Recursive: Corecursive, M[_]: MonadFsErr, F[_]: Functor]
    (src: T[Pathed[F, ?]], lb: FreeQS[T], rb: FreeQS[T], f: ListContents[M])
    (op: (T[Pathed[F, ?]], FreeQS[T], FreeQS[T]) => F[T[Pathed[F, ?]]])
    (implicit
      QC: QScriptCore[T, ?] :<: F,
      FI: F :<: QScriptTotal[T, ?])
      : M[Pathed[F, T[Pathed[F, ?]]]] =
    (convertBranch(src, lb)(f) ⊛ convertBranch(src, rb)(f))((l, r) =>
      List(CoEnv(
        op(
          Corecursive[T].embed[Pathed[F, ?]](List(CoEnv(QC.inj(Unreferenced[T, T[Pathed[F, ?]]]()).right[ADir]))),
          l,
          r).right[ADir])))

  implicit def qscriptCore[T[_[_]]: Recursive: Corecursive, G[_]: Functor]
    (implicit
      QC: QScriptCore[T, ?] :<: G,
      FI: G :<: QScriptTotal[T, ?])
      : ConvertPath.Aux[T, QScriptCore[T, ?], G] =
    new ConvertPath[T, QScriptCore[T, ?]] {
      type H[A] = G[A]

      def convertPath[M[_]: MonadFsErr](f: ListContents[M]): StaticPathTransformation[T, M, QScriptCore[T, ?], G] = {
        case Union(src, lb, rb)
            if Recursive[T].project[Pathed[G, ?]](src.copoint).exists(_.run.isLeft) =>
          convertBranchingOp(src, lb, rb, f)((s, l, r) =>
            QC.inj(Union(s, l, r)))
        case Take(src, lb, rb)
            if Recursive[T].project[Pathed[G, ?]](src.copoint).exists(_.run.isLeft) =>
          convertBranchingOp(src, lb, rb, f)((s, l, r) =>
            QC.inj(Take(s, l, r)))
        case Drop(src, lb, rb)
            if Recursive[T].project[Pathed[G, ?]](src.copoint).exists(_.run.isLeft) =>
          convertBranchingOp(src, lb, rb, f)((s, l, r) =>
            QC.inj(Drop(s, l, r)))
        case qc => List(CoEnv(QC.inj(qc).right[ADir])).point[M]
      }
    }

  implicit def read[T[_[_]], G[_]](implicit R: Const[Read, ?] :<: G):
      ConvertPath.Aux[T, Const[Read, ?], G] =
    new ConvertPath[T, Const[Read, ?]] {
      type H[A] = G[A]

      def convertPath[M[_]: MonadFsErr](f: ListContents[M]): StaticPathTransformation[T, M, Const[Read, ?], G] =
        r => List(CoEnv(R.inj(r).right[ADir])).point[M]
    }

  implicit def shiftedRead[T[_[_]], G[_]](implicit R: Const[ShiftedRead, ?] :<: G):
      ConvertPath.Aux[T, Const[ShiftedRead, ?], G] =
    new ConvertPath[T, Const[ShiftedRead, ?]] {
      type H[A] = G[A]

      def convertPath[M[_]: MonadFsErr](f: ListContents[M]): StaticPathTransformation[T, M, Const[ShiftedRead, ?], G] =
        r => List(CoEnv(R.inj(r).right[ADir])).point[M]
    }

  implicit def thetaJoin[T[_[_]]: Recursive: Corecursive, G[_]: Functor]
    (implicit
      QC: QScriptCore[T, ?] :<: G,
      TJ: ThetaJoin[T, ?] :<: G,
      FI: G :<: QScriptTotal[T, ?]):
      ConvertPath.Aux[T, ThetaJoin[T, ?], G] =
    new ConvertPath[T, ThetaJoin[T, ?]] {
      type H[A] = G[A]

      def convertPath[M[_]: MonadFsErr](f: ListContents[M]): StaticPathTransformation[T, M, ThetaJoin[T, ?], G] = {
        case ThetaJoin(src, lb, rb, on, jType, combine)
            if Recursive[T].project[Pathed[G, ?]](src.copoint).exists(_.run.isLeft) =>
          convertBranchingOp(src, lb, rb, f)((s, l, r) =>
            TJ.inj(ThetaJoin(s, l, r, on, jType, combine)))
          }
    }

  implicit def equiJoin[T[_[_]]: Recursive: Corecursive, G[_]: Functor]
    (implicit
      QC: QScriptCore[T, ?] :<: G,
      EJ: EquiJoin[T, ?] :<: G,
      FI: G :<: QScriptTotal[T, ?]):
      ConvertPath.Aux[T, EquiJoin[T, ?], G] =
    new ConvertPath[T, EquiJoin[T, ?]] {
      type H[A] = G[A]

      def convertPath[M[_]: MonadFsErr](f: ListContents[M]): StaticPathTransformation[T, M, EquiJoin[T, ?], G] = {
        case EquiJoin(src, lb, rb, lk, rk, jType, combine)
            if Recursive[T].project[Pathed[G, ?]](src.copoint).exists(_.run.isLeft) =>
          convertBranchingOp(src, lb, rb, f)((s, l, r) =>
            EJ.inj(EquiJoin(s, l, r, lk, rk, jType, combine)))
          }
    }

  implicit def projectBucket[T[_[_]]: Recursive, G[_]: Functor](
    implicit R: Const[Read, ?] :<: G, PB: ProjectBucket[T, ?] :<: G):
      ConvertPath.Aux[T, ProjectBucket[T, ?], G] =
    new ConvertPath[T, ProjectBucket[T, ?]] {
      type H[A] = G[A]

      def convertPath[M[_]: MonadFsErr](f: ListContents[M]):
          StaticPathTransformation[T, M, ProjectBucket[T, ?], G] = {
        case x @ BucketField(src, _, StrLit(str)) =>
          Recursive[T].project[Pathed[G, ?]](src.copoint).traverseM(_.run.fold(
            dir => fileType(f).apply(dir, str).fold(
              df => List(CoEnv(df.map(file => R.inj(Const[Read, T[Pathed[G, ?]]](Read(file)))))),
              Nil),
            κ(List(CoEnv(PB.inj(x).right[ADir])).point[M])))
        case x => List(CoEnv(PB.inj(x).right[ADir])).point[M]
      }
    }

  implicit def coproduct[T[_[_]], F[_], G[_], I[_]](
    implicit F: ConvertPath.Aux[T, F, I], G: ConvertPath.Aux[T, G, I]):
      ConvertPath.Aux[T, Coproduct[F, G, ?], I] =
    new ConvertPath[T, Coproduct[F, G, ?]] {
      type H[A] = I[A]

      def convertPath[M[_]: MonadFsErr](f: ListContents[M]): StaticPathTransformation[T, M, Coproduct[F, G, ?], I] =
        _.run.fold(F.convertPath(f), G.convertPath(f))
    }
}

abstract class ConvertPathInstances extends ConvertPathInstances0 {
  implicit def pathedTraverse[F[_]: Traverse]: Traverse[Pathed[F, ?]] =
    Traverse[List].compose(Traverse[CoEnv[ADir, F, ?]])
}

abstract class ConvertPathInstances0 {
  /** Represents QScript with portions turned into statically-known paths.
    */
  type Pathed[F[_], A] = (List ∘ CoEnv[ADir, F, ?])#λ[A]

  implicit def pathedFunctor[F[_]: Functor]: Functor[Pathed[F, ?]] =
    Functor[List].compose(Functor[CoEnv[ADir, F, ?]])
}
