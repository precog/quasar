/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.fs.mount

import slamdata.Predef._
import quasar.common.PhaseResults
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.effect._
import quasar.fp.free._
import quasar.fs._
import quasar.frontend.{logicalplan => lp}, lp.LogicalPlan

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz._

object hierarchical {
  import QueryFile.ResultHandle
  import FileSystemError._, PathError._

  type MountedResultH[A] = KeyValueStore[ResultHandle, (ADir, ResultHandle), A]

  /** Returns a `ReadFileF` interpreter that selects one of the configured
    * child interpreters based on the path of the incoming request.
    *
    * @param rfs `ReadFileF` interpreters indexed by mount
    */
  def readFile[F[_], S[_]](
    rfs: Mounts[ReadFile ~> F]
  )(implicit
    S: F :<: S
  ): ReadFile ~> Free[S, ?] = {
    import ReadFile._

    type M[A] = Free[S, A]

    lazy val mountedRfs = rfs mapWithDir { case (d, f) =>
      flatMapSNT(injectFT[F, S] compose f) compose mounted.readFile[ReadFile](d)
    }

    λ[ReadFile ~> M] {
      case Open(loc, off, lim) =>
        lookupMounted(mountedRfs, loc) map { case (_, g) =>
          g(Open(loc, off, lim))
        } getOrElse pathErr(pathNotFound(loc)).left.point[M]

      case Read(h) =>
        lookupMounted(mountedRfs, h.file) map { case (_, g) =>
          g(Read(h))
        } getOrElse unknownReadHandle(h).left.point[M]

      case Close(h) =>
        lookupMounted(mountedRfs, h.file) map { case (_, g) =>
          g(Close(h))
        } getOrElse ().point[M]
    }
  }

  /** Returns a `WriteFileF` interpreter that selects one of the configured
    * child interpreters based on the path of the incoming request.
    *
    * @param mountSep used to separate the mount from the original file in handles
    * @param wfs `WriteFileF` interpreters indexed by mount
    */
  def writeFile[F[_], S[_]](
    wfs: Mounts[WriteFile ~> F]
  )(implicit
    S: F :<: S
  ): WriteFile ~> Free[S, ?] = {
    import WriteFile._

    type M[A] = Free[S, A]

    lazy val mountedWfs = wfs mapWithDir { case (d, f) =>
      flatMapSNT(injectFT[F, S] compose f) compose mounted.writeFile[WriteFile](d)
    }

    λ[WriteFile ~> M] {
      case Open(loc) =>
        lookupMounted(mountedWfs, loc) map { case (_, g) =>
          g(Open(loc))
        } getOrElse pathErr(pathNotFound(loc)).left.point[M]

      case Write(h, chunk) =>
        lookupMounted(mountedWfs, h.file) map { case (_, g) =>
          g(Write(h, chunk))
        } getOrElse Vector(unknownWriteHandle(h)).point[M]

      case Close(h) =>
        lookupMounted(mountedWfs, h.file) map { case (_, g) =>
          g(Close(h))
        } getOrElse ().point[M]
    }
  }

  /** Returns a `ManageFileF` interpreter that selects one of the configured
    * child interpreters based on the path of the incoming request.
    */
  def manageFile[F[_], S[_]](
    mfs: Mounts[ManageFile ~> F]
  )(implicit
    S: F :<: S
  ): ManageFile ~> Free[S, ?] = {
    import ManageFile._

    type M[A] = Free[S, A]
    type MES[A] = EitherT[M, FileSystemError, A]

    val mountedMfs = mfs mapWithDir { case (d, f) =>
      flatMapSNT(injectFT[F, S] compose f) compose mounted.manageFile[ManageFile](d)
    }

    val lookup = lookupMounted(mountedMfs, _: APath)

    def noMountError(path: APath) =
      pathErr(invalidPath(path, "does not refer to a mounted filesystem"))

    def deleteDir(d: ADir) =
      lookup(d) cata (
        { case (_, g) => g(Delete(d)) },
        mountedMfs.toMap.filterKeys(_.relativeTo(d).isDefined)
          .toList
          .traverse { case (mnt, g) => g(Delete(mnt)) }
          .map(_.sequence_))

    def deleteFile(f: AFile) =
      EitherT.fromDisjunction[M](
        lookup(f) toRightDisjunction pathErr(pathNotFound(f))
      ).flatMapF { case (_, g) =>
        g(Delete(f))
      }.run

    λ[ManageFile ~> M] {
      case Move(scn, sem) =>
        val src = lookup(scn.src).toRightDisjunction(
          pathErr(pathNotFound(scn.src)))

        val dst = lookup(scn.dst).toRightDisjunction(
          noMountError(scn.dst))

        EitherT.fromDisjunction[M](src tuple dst).flatMap {
          case ((srcMnt, g), (dstMnt, _)) if srcMnt == dstMnt =>
            EitherT(g(Move(scn, sem)))

          case _ =>
            pathErr(invalidPath(
              scn.dst,
              s"must refer to the same filesystem as '${posixCodec.printPath(scn.src)}'"
            )).raiseError[MES, Unit]
        }.run

      case Delete(path) =>
        refineType(path).fold(deleteDir, deleteFile)

      case TempFile(near) =>
        EitherT.fromDisjunction[M](
          lookup(near) toRightDisjunction noMountError(near)
        ).flatMapF { case (_, g) =>
          g(TempFile(near))
        }.run
    }
  }

  /** Returns a `QueryFileF` interpreter that selects one of the configured
    * child interpreters based on the path of the incoming request.
    */
  def queryFile[F[_], S[_]](
    qfs: Mounts[QueryFile ~> F]
  )(
    implicit
    S1: F :<: S,
    S2: MonotonicSeq :<: S,
    S3: MountedResultH :<: S
  ): QueryFile ~> Free[S, ?] = {
    import QueryFile._

    type M[A] = Free[S, A]

    val seq = MonotonicSeq.Ops[S]
    val handles = KeyValueStore.Ops[ResultHandle, (ADir, ResultHandle), S]
    val transforms = Transforms[M]
    import transforms._

    lazy val mountedQfs = qfs mapWithDir { case (d, f) =>
      flatMapSNT(injectFT[F, S] compose f) compose mounted.queryFile[QueryFile](d)
    }

    def resultForPlan[A](
      lp: Fix[LogicalPlan],
      out: Option[AFile],
      qf: QueryFile[(PhaseResults, FileSystemError \/ A)]
    ): ExecM[(ADir, A)] =
      mountForPlan(mountedQfs, lp, out) match {
        case -\/(err) =>
          EitherT.leftU[(ADir, A)](err.point[G])

        case \/-((mnt, g)) =>
          EitherT(WriterT(g(qf)): G[FileSystemError \/ A])
            .strengthL(mnt)
      }

    λ[QueryFile ~> M] {
      case ExecutePlan(lp, out) =>
        resultForPlan(lp, some(out), ExecutePlan(lp, out))
          .map(_._2).run.run

      case EvaluatePlan(lp) =>
        (for {
          r    <- resultForPlan(lp, none, EvaluatePlan(lp))
          newH <- toExec(seq.next.map(ResultHandle(_)))
          _    <- toExec(handles.put(newH, r))
        } yield newH).run.run

      case More(h) =>
        getMounted[S](h, mountedQfs)
          .toRight(unknownResultHandle(h))
          .flatMapF { case (qh, g) => g(More(qh)) }
          .run

      case Close(h) =>
        getMounted[S](h, mountedQfs)
          .flatMapF(handles.delete(h).as(_))
          .flatMapF { case (qh, g) => g(Close(qh)) }
          .getOrElse(())

      case Explain(lp) =>
        resultForPlan(lp, none, Explain(lp))
          .map(_._2).run.run

      case ListContents(d) =>
        lookupMounted(mountedQfs, d)
          .map { case (_, g) => g(ListContents(d)) }
          .orElse(
            lsMounts(mountedQfs.toMap.keySet, d)
              .map(_.right[FileSystemError].point[M]))
          .getOrElse(pathErr(pathNotFound(d)).left.point[M])

      case FileExists(f) =>
        lookupMounted(mountedQfs, f)
          .map { case (_, g) => g(FileExists(f)) }
          .getOrElse(false.point[M])
    }
  }

  def analyze[F[_], S[_]](
    afs: Mounts[Analyze ~> F]
  )(
    implicit
    S1: F :<: S,
    S2: MonotonicSeq :<: S,
    S3: MountedResultH :<: S
  ): Analyze ~> Free[S, ?] = {
    import Analyze._

    type M[A] = Free[S, A]

    lazy val mountedAfs = afs mapWithDir { case (d, f) =>
      flatMapSNT(injectFT[F, S] compose f) compose mounted.analyze[Analyze](d)
    }

    λ[Analyze ~> M] {
      case a @ QueryCost(lp) =>
        val forPlan: FileSystemError \/ (ADir, Analyze ~> M) = mountForPlan(mountedAfs, lp, none)
        forPlan.fold(_.left[Int].point[M], {
          case (mnt, g) => g(a)
        })
    }
  }

  def fileSystem[F[_], S[_]](
    mounts: Mounts[FileSystem ~> F]
  )(implicit
    S1: F :<: S,
    S2: MountedResultH :<: S,
    S3: MonotonicSeq :<: S
  ): FileSystem ~> Free[S, ?] = {
    type M[A] = Free[S, A]
    type FS[A] = FileSystem[A]

    def injFS[G[_]](implicit I: G :<: FS): G ~> FS = injectNT[G, FS]

    val qf: QueryFile ~> M  = queryFile[F, S](mounts map (_ compose injFS[QueryFile]))
    val rf: ReadFile ~> M   = readFile[F, S](mounts map (_ compose injFS[ReadFile]))
    val wf: WriteFile ~> M  = writeFile[F, S](mounts map (_ compose injFS[WriteFile]))
    val mf: ManageFile ~> M = manageFile[F, S](mounts map (_ compose injFS[ManageFile]))

    qf :+: rf :+: wf :+: mf
  }

  def backendEffect[F[_], S[_]](
    mounts: Mounts[BackendEffect ~> F]
  )(implicit
    S1: F :<: S,
    S2: MountedResultH :<: S,
    S3: MonotonicSeq :<: S
  ): BackendEffect ~> Free[S, ?] = {
    analyze[F, S](mounts map (_ compose Inject[Analyze, BackendEffect])) :+:
    fileSystem[F,S](mounts.map(_  compose Inject[FileSystem, BackendEffect]))
  }

  ////

  private def noMountsDefined(lp: Fix[LogicalPlan]): FileSystemError =
    executionFailed_(lp, "No mounts defined.")

  // TODO{performance}: Move the prefix find op to `Mounts` so it can be optimized
  private def lookupMounted[A](mounts: Mounts[A], path: APath): Option[(ADir, A)] =
    mounts.toMap find { case (d, a) => path.relativeTo(d).isDefined }

  private def mountForPlan[A](
    mounts: Mounts[A],
    plan: Fix[LogicalPlan],
    out: Option[AFile]
  ): FileSystemError \/ (ADir, A) = {

    type MntA = (ADir, A)
    type F[A] = State[Option[MntA], A]
    type M[A] = FileSystemErrT[F, A]

    val F = MonadState[F, Option[MntA]]

    def lookupMnt(p: APath): FileSystemError \/ MntA =
      lookupMounted(mounts, p) toRightDisjunction pathErr(pathNotFound(p))

    def compareToExisting(mnt: ADir): M[Unit] = {
      def errMsg(exMnt: ADir): FileSystemError =
        pathErr(invalidPath(mnt,
          s"refers to a different filesystem than '${posixCodec.printPath(exMnt)}'"))

      EitherT[F, FileSystemError, Unit](F.gets(exMnt =>
        (exMnt map (_._1) filter (_ != mnt) map errMsg) <\/ (())
      ))
    }

    def mountFor(p: APath): M[Unit] = for {
      mntA     <- EitherT.fromDisjunction[F](lookupMnt(p))
      (mnt, a) =  mntA
      _        <- compareToExisting(mnt)
      _        <- F.put(some(mntA)).liftM[FileSystemErrT]
    } yield ()

    out.cata(d => lookupMnt(d) map some, none.right) flatMap (initMnt =>
      plan.cataM[M, Unit] {
        // Documentation on `QueryFile` guarantees absolute paths, so calling `mkAbsolute`
        case lp.Read(p) => mountFor(mkAbsolute(rootDir, p))
        case _          => ().point[M]
      }.run.run(initMnt) match {
        // NB: If mnt is empty, then there were no `Read`, so we should
        // be able to get a result without needing an actual filesystem,
        // and we just pass it to an arbitrary mount, if there is at
        // least one present.
        case (mntA, r) =>
          r *> (mntA.orElse(mounts.toMap.toList.headOption) \/> noMountsDefined(plan))
      })
  }

  private def lsMounts(mounts: Set[ADir], ls: ADir): Option[Set[PathSegment]] = {
    def firstDir(rdir: RDir): Option[DirName] =
      firstSegmentName(rdir).flatMap(_.swap.toOption)

    if (mounts.isEmpty && ls === rootDir)
      Some(Set())
    else
      mounts.foldMap(_ relativeTo ls flatMap firstDir map (d => Set(d.left)))
  }

  private object getMounted {
    final class Aux[S[_]] {
      type F[A] = Free[S, A]

      def apply[A, B](a: A, mnts: Mounts[B])
                     (implicit I: KeyValueStore[A, (ADir, A), ?] :<: S)
                     : OptionT[F, (A, B)] = {
        KeyValueStore.Ops[A, (ADir, A), S].get(a) flatMap { case (d, a1) =>
          OptionT(mnts.lookup(d).strengthL(a1).point[F])
        }
      }
    }

    def apply[S[_]]: Aux[S] = new Aux[S]
  }
}
