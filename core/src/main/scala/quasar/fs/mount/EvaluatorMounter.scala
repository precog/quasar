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

package quasar.fs.mount

import quasar.Predef.Unit
import quasar.effect._
import quasar.fp.{liftFT, injectNT, free}
import quasar.fs.FileSystem
import hierarchical.{HFSFailureF, MountedResultHF}

import scalaz._
import scalaz.syntax.monad._

/** Handles mount requests, managing a composite `FileSystem` interpreter
  * into a base effect type `F`, adding support for views and hierarchical
  * mounts.
  *
  * @tparam F the base effect that `FileSystem` operations are translated into
  * @tparam S the composite effect, supporting the base, view and hierarchical effects
  */
final class EvaluatorMounter[F[_], S[_]: Functor](
  fsDef: FileSystemDef[F]
)(implicit S0: F :<: S,
           S1: MonotonicSeqF :<: S,
           S2: ViewStateF :<: S,
           S3: MountedResultHF :<: S,
           S4: HFSFailureF :<: S) {

  import MountRequest._, FileSystemDef.DefinitionResult

  type EvalFS[A]     = Free[S, A]
  type EvalFSRef[A]  = AtomicRef[FileSystem ~> EvalFS, A]
  type EvalFSRefF[A] = Coyoneda[EvalFSRef, A]

  def mount[T[_]: Functor]
      (req: MountRequest)
      (implicit T0: F :<: T,
                T1: fsMounter.MountedFsF :<: T,
                T2: MountedViewsF :<: T,
                T3: EvalFSRefF :<: T)
      : Free[T, MountingError \/ Unit] = {

    type EvalM[A]    = Free[T, A]
    type EvalErrM[A] = MntErrT[EvalM, A]

    val handleMount: EvalErrM[Unit] =
      EitherT(req match {
        case MountView(f, qry, vars) =>
          ViewMounter.mount[T](f, qry, vars)

        case MountFileSystem(d, typ, uri) =>
          fsMounter.mount[T](d, typ, uri)
      })

    (handleMount *> updateComposite[T].liftM[MntErrT]).run
  }

  def unmount[T[_]: Functor]
      (req: MountRequest)
      (implicit T0: F :<: T,
                T1: fsMounter.MountedFsF :<: T,
                T2: MountedViewsF :<: T,
                T3: EvalFSRefF :<: T)
      : Free[T, Unit] = {

    val handleUnmount: Free[T, Unit] =
      req match {
        case MountView(f, _, _) =>
          ViewMounter.unmount[T](f)

        case MountFileSystem(d, _, _) =>
          fsMounter.unmount[T](d)
      }

    handleUnmount *> updateComposite[T]
  }

  ////

  private type ViewEff0[A] = Coproduct[MonotonicSeqF, FileSystem, A]
  private type ViewEff[A]  = Coproduct[ViewStateF, ViewEff0, A]

  private val fsMounter = FileSystemMounter[F](fsDef)

  private def evalFS[T[_]: Functor](implicit T: EvalFSRefF :<: T) =
    AtomicRef.Ops[FileSystem ~> EvalFS, T]

  private def views[T[_]: Functor](implicit T: MountedViewsF :<: T) =
    AtomicRef.Ops[Views, T]

  private def mounts[T[_]: Functor](implicit T: fsMounter.MountedFsF :<: T) =
    AtomicRef.Ops[Mounts[DefinitionResult[F]], T]

  /** Builds the composite interpreter from the currently mounted views and
    * filesystems, storing the result in an `AtomicRef`.
    *
    * This involves, roughly
    *   1. Get the current mounted views and filesystem intepreters from their
    *      respective refs.
    *
    *   2. Build a hierarchical filesystem interpreter using the filesystem
    *      mounts from (1).
    *
    *   3. Using the hierarchical interpreter from (2), create a view filesystem
    *      interpreter using the mounted views from (1), lifting the result into
    *      the output effect type, `S[_]`.
    *
    *   4. Store the result of (3) in a ref.
    */
  private def updateComposite[T[_]: Functor]
              (implicit T0: F :<: T,
                        T1: fsMounter.MountedFsF :<: T,
                        T2: MountedViewsF :<: T,
                        T3: EvalFSRefF :<: T)
              : Free[T, Unit] =
    for {
      vws    <- views[T].get
      mnts   <- mounts[T].get
      evals  =  mnts.map(_._1)
      mnted  =  hierarchical.fileSystem[F, S](evals)
      injSeq =  liftFT[S] compose injectNT[MonotonicSeqF, S]
      injVST =  liftFT[S] compose injectNT[ViewStateF, S]
      iView  =  free.interpret3[ViewStateF, MonotonicSeqF, FileSystem, EvalFS](
                  injVST, injSeq, mnted)
      viewd  =  view.fileSystem[ViewEff](vws)
      f      =  free.foldMapNT[ViewEff, EvalFS](iView) compose viewd
      _      <- evalFS[T].set(f)
    } yield ()
}

object EvaluatorMounter {
  def apply[F[_], S[_]: Functor](
    fsDef: FileSystemDef[F]
  )(implicit S0: F :<: S,
             S1: MonotonicSeqF :<: S,
             S2: ViewStateF :<: S,
             S3: MountedResultHF :<: S,
             S4: HFSFailureF :<: S
  ): EvaluatorMounter[F, S] =
    new EvaluatorMounter[F, S](fsDef)
}
