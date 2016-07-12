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

package quasar

import quasar.Predef._

import quasar.effect._
import quasar.fp._
import quasar.fp.free._
import quasar.fs._
import quasar.fs.mount._
import quasar.fs.mount.hierarchical._
import quasar.physical.mongodb._

import com.mongodb.MongoException
import monocle.Lens
import pathy.Path.posixCodec
import scalaz.{Failure => _, Lens => _, _}, Scalaz._
import scalaz.concurrent.Task

/** Concrete effect types and their interpreters that implement the quasar
  * functionality.
  */
package object main {
  import FileSystemDef.DefinitionResult
  import QueryFile.ResultHandle

  type MainErrT[F[_], A] = EitherT[F, String, A]
  type MainTask[A]       = MainErrT[Task, A]

  val MainTask           = MonadError[EitherT[Task,String,?], String]

  /** Effects that physical filesystems require.
    */
  type PhysFsEff[A]  = Coproduct[MongoErr, Task, A]
  type PhysFsEffM[A] = Free[PhysFsEff, A]

  object PhysFsEff {
    // Lift into FsEvalIOM
    val toFsEvalIOM: PhysFsEff ~> FsEvalIOM =
      injectFT[MongoErr, FsEvalIO] :+:
      injectFT[Task, FsEvalIO]
  }

  /** The physical filesystems currently supported.
    *
    * NB: Will eventually be the lcd of all physical filesystems, or we limit
    * to a fixed set of effects that filesystems must interpret into.
    */
  val physicalFileSystems: FileSystemDef[PhysFsEffM] =
    quasar.physical.mongodb.fs.mongoDbFileSystemDef[PhysFsEff]

  /** The intermediate effect FileSystem operations are interpreted into.
    */
  type FsEff0[A] = Coproduct[ViewState, MountedResultH, A]
  type FsEff1[A] = Coproduct[MonotonicSeq, FsEff0, A]
  type FsEff2[A] = Coproduct[PhysFsEffM, FsEff1, A]
  type FsEff[A]  = Coproduct[MountConfigs, FsEff2, A]
  type FsEffM[A] = Free[FsEff, A]

  object FsEff {
    /** Interpret all effects except failures. */
    def toFsEvalIOM(
      seqRef: TaskRef[Long],
      viewHandlesRef: TaskRef[ViewHandles],
      mntResRef: TaskRef[Map[ResultHandle, (ADir, ResultHandle)]]
    ): FsEff ~> FsEvalIOM = {
      def injTask[E[_]](f: E ~> Task): E ~> FsEvalIOM =
        injectFT[Task, FsEvalIO] compose f

      injectFT[MountConfigs, FsEvalIO]                              :+:
      foldMapNT(PhysFsEff.toFsEvalIOM)                              :+:
      injTask[MonotonicSeq](MonotonicSeq.fromTaskRef(seqRef))       :+:
      injTask[ViewState](KeyValueStore.fromTaskRef(viewHandlesRef)) :+:
      injTask[MountedResultH](KeyValueStore.fromTaskRef(mntResRef))
    }

    /** A dynamic `FileSystem` evaluator formed by internally fetching an
      * interpreter from a `TaskRef`, allowing for the behavior to change over
      * time as the ref is updated.
      */
    def evalFSFromRef[S[_]](
      ref: TaskRef[FileSystem ~> FsEffM],
      f: FsEff ~> Free[S, ?]
    )(implicit S: Task :<: S): FileSystem ~> Free[S, ?] = {
      type F[A] = Free[S, A]
      new (FileSystem ~> F) {
        def apply[A](fs: FileSystem[A]) =
          injectFT[Task, S].apply(ref.read.map(free.foldMapNT[FsEff, F](f) compose _))
            .flatMap(_.apply(fs))
      }
    }
  }

  /** The effects involved in FileSystem evaluation.
    *
    * We interpret into this effect to defer error handling and mount
    * configuration based on the final context of interpretation
    * (i.e. web service vs cmd line).
    */
  type FsEval[A]    = Coproduct[MongoErr, MountConfigs, A]
  type FsEvalIO[A]  = Coproduct[Task, FsEval, A]
  type FsEvalIOM[A] = Free[FsEvalIO, A]


  //--- Composite FileSystem ---

  /** Provides the mount handlers to update the composite
    * (view + hierarchical + physical) filesystem whenever a mount is added
    * or removed.
    */
  val mountHandler = EvaluatorMounter[PhysFsEffM, FsEff](physicalFileSystems)
  import mountHandler.EvalFSRef

  /** An atomic reference holding the mapping between mount points and
    * physical filesystem interpreters.
    */
  type MountedFs[A]  = AtomicRef[Mounts[DefinitionResult[PhysFsEffM]], A]

  /** Effect required by the composite (view + hierarchical + physical)
    * filesystem.
    */
  type CompFsEff0[A] = Coproduct[MountedFs, MountConfigs, A]
  type CompFsEff1[A] = Coproduct[PhysFsEffM, CompFsEff0, A]
  type CompFsEff[A]  = Coproduct[EvalFSRef, CompFsEff1, A]
  type CompFsEffM[A] = Free[CompFsEff, A]

  object CompFsEff {
    def toFsEvalIOM(
      evalRef: TaskRef[FileSystem ~> FsEffM],
      mntsRef: TaskRef[Mounts[DefinitionResult[PhysFsEffM]]]
    ): CompFsEff ~> FsEvalIOM = {
      def injTask[E[_]](f: E ~> Task): E ~> FsEvalIOM =
        injectFT[Task, FsEvalIO] compose f

      injTask[EvalFSRef](AtomicRef.fromTaskRef(evalRef)) :+:
      foldMapNT(PhysFsEff.toFsEvalIOM)                   :+:
      injTask[MountedFs](AtomicRef.fromTaskRef(mntsRef)) :+:
      injectFT[MountConfigs, FsEvalIO]
    }
  }

  /** Effect required by the "complete" filesystem supporting modifying mounts,
    * views, hierarchical mounts and physical implementations.
    */
  type CompleteFsEff[A] = Coproduct[MountConfigs, CompFsEffM, A]
  type CompleteFsEffM[A] = Free[CompleteFsEff, A]

  val mounter: Mounting ~> CompleteFsEffM =
    quasar.fs.mount.Mounter[CompFsEffM, CompleteFsEff](
      mountHandler.mount[CompFsEff](_),
      mountHandler.unmount[CompFsEff](_))

  def ephemeralMountConfigs[F[_]: Monad]: MountConfigs ~> F = {
    type ST[A] = StateT[F, Map[APath, MountConfig], A]

    val toState: MountConfigs ~> ST =
      KeyValueStore.toState[ST](Lens.id[Map[APath, MountConfig]])

    evalNT[F, Map[APath, MountConfig]](Map()) compose toState
  }

  /** Encompasses all the failure effects and mount config effect, all of
    * which we need to evaluate using more than one implementation.
    */
  type CfgsErrs0[A]   = Coproduct[MongoErr, MountConfigs, A]
  type CfgsErrs[A]    = Coproduct[FileSystemFailure, CfgsErrs0, A]

  object CfgsErrs {
    def toCatchable[F[_]: Catchable](
      eval: MountConfigs ~> F
    ): CfgsErrs ~> F =
      Failure.toRuntimeError[F, FileSystemError] :+:
      Failure.toCatchable[F, MongoException]     :+:
      eval
  }

  type CfgsErrsIO[A]  = Coproduct[Task, CfgsErrs, A]
  type CfgsErrsIOM[A] = Free[CfgsErrsIO, A]

  object CfgsErrsIO {
    /** Interprets errors into strings. */
    def toMainTask(eval: MountConfigs ~> Task): CfgsErrsIO ~> MainTask = {
      val f =
        NaturalTransformation.refl[Task] :+:
        CfgsErrs.toCatchable(eval)

      new (CfgsErrsIO ~> MainTask) {
        def apply[A](a: CfgsErrsIO[A]) =
          EitherT(f(a).attempt).leftMap(_.getMessage)
      }
    }
  }

  /** Effect required by the core Quasar services */
  type CoreEff0[A] = Coproduct[Mounting, FileSystem, A]
  type CoreEff1[A] = Coproduct[FileSystemFailure, CoreEff0, A]
  type CoreEff[A]  = Coproduct[Task, CoreEff1, A]
  type CoreEffM[A] = Free[CoreEff, A]

  // TODO: Accept an initial set of mounts?
  object CoreEff {
    val interpreter: Task[CoreEff ~> CfgsErrsIOM] =
      for {
        startSeq   <- Task.delay(scala.util.Random.nextInt.toLong)
        seqRef     <- TaskRef(startSeq)
        viewHRef   <- TaskRef[ViewHandles](Map())
        mntedRHRef <- TaskRef(Map[ResultHandle, (ADir, ResultHandle)]())
        evalFsRef  <- TaskRef(Empty.fileSystem[FsEffM])
        mntsRef    <- TaskRef(Mounts.empty[DefinitionResult[PhysFsEffM]])
      } yield {
        val f: FsEff ~> FsEvalIOM = FsEff.toFsEvalIOM(seqRef, viewHRef, mntedRHRef)
        val g: CompFsEff ~> FsEvalIOM = CompFsEff.toFsEvalIOM(evalFsRef, mntsRef)

        val liftTask: Task ~> CfgsErrsIOM =
          injectFT[Task, CfgsErrsIO]

        val translateFsErrs: FsEvalIOM ~> CfgsErrsIOM =
          free.foldMapNT[FsEvalIO, CfgsErrsIOM](
            liftTask                           :+:
            injectFT[MongoErr, CfgsErrsIO]     :+:
            injectFT[MountConfigs, CfgsErrsIO])

        val mnt: CompleteFsEff ~> CfgsErrsIOM =
          injectFT[MountConfigs, CfgsErrsIO] :+:
          translateFsErrs.compose[CompFsEffM](free.foldMapNT(g))

        val mounting: Mounting ~> CfgsErrsIOM =
          free.foldMapNT(mnt) compose mounter

        liftTask                                :+:
        injectFT[FileSystemFailure, CfgsErrsIO] :+:
        mounting                                :+:
        (translateFsErrs compose FsEff.evalFSFromRef(evalFsRef, f))
      }
  }

  /** Mount all the mounts defined in the given configuration, returning
    * the paths that failed to mount along with the reasons why.
    */
  def attemptMountAll[S[_]](
    config: MountingsConfig
  )(implicit
    mounting: Mounting.Ops[S]
  ): Free[S, Map[APath, String]] = {
    val attemptMount: ((APath, MountConfig)) => Free[S, Map[APath, String]] = {
      case (path, cfg) =>
        mounting.mount(path, cfg).run map {
          case \/-(\/-(_)) => Map.empty
          case \/-(-\/(e)) => Map(path -> e.shows)
          case -\/(e)      => Map(path -> e.shows)
        }
    }

    config.toMap.toList foldMapM attemptMount
  }

  /** Prints a warning about the mount failure to the console. */
  val logFailedMount: ((APath, String)) => Task[Unit] = {
    case (path, err) => console.stderr(
      s"Warning: Failed to mount '${posixCodec.printPath(path)}' because '$err'."
    )
  }
}
