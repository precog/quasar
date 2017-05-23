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
import quasar.queryPlan
import quasar.effect._
import quasar.fs.AnalyticalFileSystem
import quasar.sql.Blob
import hierarchical.MountedResultH

import eu.timepit.refined.auto._
import monocle.function.Field1
import pathy.Path.fileParent
import scalaz._, Scalaz._

/** Handles mount requests, validating them and updating a hierarchical
  * `FileSystem` interpreter as mounts are added and removed.
  *
  * @tparam F the base effect that `FileSystem` operations are translated into
  * @tparam S the composite effect, supporting the base and hierarchical effects
  */
final class MountRequestHandler[F[_], S[_]](
  fsDef: FileSystemDef[F]
)(implicit
  S0: F :<: S,
  S1: MountedResultH :<: S,
  S2: MonotonicSeq :<: S
) {
  import MountRequest._, MountingError._, MountConfig._

  type HierarchicalFsRef[A] = AtomicRef[AnalyticalFileSystem ~> Free[S, ?], A]

  object HierarchicalFsRef {
    def Ops[G[_]](implicit G: HierarchicalFsRef :<: G) =
      AtomicRef.Ops[AnalyticalFileSystem ~> Free[S, ?], G]
  }

  def mount[T[_]](
    req: MountRequest
  )(implicit
    T0: F :<: T,
    T1: fsm.MountedFsRef :<: T,
    T2: HierarchicalFsRef :<: T,
    F: Monad[F]
  ): Free[T, MountingError \/ Unit] = {
    val handleMount: MntErrT[Free[T, ?], Unit] =
      EitherT(req match {
        case MountView(f, qry, vars) =>
          queryPlan(Blob(qry, Nil), vars, fileParent(f), 0L, None).run.value
            .leftMap(e => invalidConfig(viewConfig(qry, vars), e.map(_.shows)))
            .void.point[Free[T, ?]]

        case MountFileSystem(d, typ, uri) =>
          fsm.mount[T](d, typ, uri)

        case MountModule(d, s) => ().right.point[Free[T, ?]]
      })

    (handleMount *> updateHierarchy[T].liftM[MntErrT]).run
  }

  def unmount[T[_]](
    req: MountRequest
  )(implicit
    T0: F :<: T,
    T1: fsm.MountedFsRef :<: T,
    T2: HierarchicalFsRef :<: T
  ): Free[T, Unit] =
    fsDir.getOption(req).traverse_(fsm.unmount[T]) *> updateHierarchy[T]

  ////

  private val fsm = FileSystemMountHandler[F](fsDef)
  private val fsDir = mountFileSystem composeLens Field1.first

  /** Builds the hierarchical interpreter from the currently mounted filesystems,
    * storing the result in `HierarchicalFsRef`.
    *
    * TODO: Effects should be `Read[MountedFs, ?]` and `Write[HierarchicalFs, ?]`
    *       to be more precise.
    *
    * This involves, roughly
    *   1. Get the current mounted filesystems from `MountedFsRef`.
    *
    *   2. Build a hierarchical filesystem interpreter using the mounts from (1).
    *
    *   3. Lift the result of (2) into the output effect, `S[_]`.
    *
    *   4. Store the result of (3) in `HierarchicalFsRef`.
    */
  private def updateHierarchy[T[_]](
    implicit
    T0: F :<: T,
    T1: fsm.MountedFsRef :<: T,
    T2: HierarchicalFsRef :<: T
  ): Free[T, Unit] =
    for {
      mnted <- fsm.MountedFsRef.Ops[T].get ∘
                 (mnts => hierarchical.analyticalFileSystem[F, S](mnts.map(_.run)))
      _     <- HierarchicalFsRef.Ops[T].set(mnted)
    } yield ()
}

object MountRequestHandler {
  def apply[F[_], S[_]](
    fsDef: FileSystemDef[F]
  )(implicit
    S0: F :<: S,
    S1: MountedResultH :<: S,
    S2: MonotonicSeq :<: S
  ): MountRequestHandler[F, S] =
    new MountRequestHandler[F, S](fsDef)
}
