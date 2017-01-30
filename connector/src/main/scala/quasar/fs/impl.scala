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

package quasar.fs

import quasar.Predef._
import quasar.Data
import quasar.common.PhaseResults
import quasar.contrib.pathy._
import quasar.effect.{Kvs, KeyValueStore, MonoSeq, MonotonicSeq}
import quasar.fs.PathError._
import quasar.fs.ManageFile._
import quasar.fs.ManageFile.MoveSemantics._
import quasar.fp.free._
import quasar.fp.numeric._
import quasar.frontend.logicalplan.LogicalPlan

import matryoshka.data.Fix
import scalaz._, Scalaz._
import scalaz.stream.Process

object impl {
  import FileSystemError._

  def ensureMoveSemantics[F[_]: Monad] (
    src: APath,
    dst: APath,
    fExists: APath => F[Boolean],
    semantics: MoveSemantics
  ): OptionT[F, FileSystemError] =
    OptionT[F, FileSystemError](
      fExists(src).flatMap { srcExists =>
        if(srcExists) {
          semantics match {
            case Overwrite => none.point[F]
            case FailIfExists =>
              fExists(dst).map { dstExists =>
                if(dstExists) Some(PathErr(pathExists(dst))) else None
              }
            case FailIfMissing =>
              fExists(dst).map { dstExists =>
                if(!dstExists) Some(PathErr(pathNotFound(dst))) else None
              }
          }
        }
        else some(pathErr(pathNotFound(src))).point[F]
      })

  final case class ReadOpts(offset: Natural, limit: Option[Positive])

  def read[C, F[_]: Monad](
    open: (AFile, ReadOpts) => F[FileSystemError \/ C],
    read: C => F[FileSystemError \/ (C, Vector[Data])],
    close: C => F[Unit]
  )(implicit
    cursors: Kvs[F, ReadFile.ReadHandle, C],
    idSeq: MonoSeq[F]
  ) = λ[ReadFile ~> F] {
    case ReadFile.Open(file, offset, limit) =>
      (for {
        cursor <- EitherT(open(file, ReadOpts(offset, limit)))
        id     <- idSeq.next.liftM[FileSystemErrT]
        handle =  ReadFile.ReadHandle(file, id)
        _      <- cursors.put(handle, cursor).liftM[FileSystemErrT]
      } yield handle).run

    case ReadFile.Read(handle) =>
      (for {
        cursor <- OptionT(cursors.get(handle)).toRight(unknownReadHandle(handle))
        result <- EitherT(read(cursor))
        (newCursor, data) = result
        _      <- cursors.put(handle, newCursor).liftM[FileSystemErrT]
      } yield data).run

    case ReadFile.Close(handle) =>
      (for {
        cursor <- OptionT(cursors.get(handle))
        _      <- close(cursor).liftM[OptionT]
        _      <- cursors.delete(handle).liftM[OptionT]
      } yield ()).run.void
  }

  def readFromDataCursor[C, F[_]: Monad: Kvs[?[_], ReadFile.ReadHandle, C]: MonoSeq](
    open: (AFile, ReadOpts) => F[FileSystemError \/ C]
  )(implicit
    C: DataCursor[F, C]
  ): ReadFile ~> F =
    read[C, F](open, c => C.nextChunk(c) map (_.right[FileSystemError] strengthL c), C.close)

  type ReadStream[F[_]] = Process[F, FileSystemError \/ Vector[Data]]

  def readFromProcess[S[_], F[_]: Monad: Catchable](
    f: (AFile, ReadOpts) => Free[S, FileSystemError \/ ReadStream[F]]
  )(implicit
    state: KeyValueStore.Ops[ReadFile.ReadHandle, ReadStream[F], S],
    idGen: MonotonicSeq.Ops[S],
    S0: F :<: S
  ) = λ[ReadFile ~> Free[S, ?]] {
    case ReadFile.Open(file, offset, limit) =>
      (for {
        readStream <- EitherT(f(file, ReadOpts(offset, limit)))
        id         <- idGen.next.liftM[FileSystemErrT]
        handle     =  ReadFile.ReadHandle(file, id)
        _          <- state.put(handle, readStream).liftM[FileSystemErrT]
      } yield handle).run

    case ReadFile.Read(handle) =>
      (for {
        stream <- state.get(handle).toRight(unknownReadHandle(handle))
        data   <- EitherT(lift(stream.unconsOption).into[S].flatMap {
                    case Some((value, streamTail)) =>
                      state.put(handle, streamTail).as(value)
                    case None                      =>
                      state.delete(handle).as(Vector.empty[Data].right[FileSystemError])
                  })
      } yield data).run

    case ReadFile.Close(handle) =>
      (for {
        stream <- state.get(handle)
        _      <- lift(stream.kill.run).into[S].liftM[OptionT]
        _      <- state.delete(handle).liftM[OptionT]
      } yield ()).run.void
  }

  def queryFile[C, F[_]: Monad](
    execute: (Fix[LogicalPlan], AFile) => F[(PhaseResults, FileSystemError \/ AFile)],
    evaluate: Fix[LogicalPlan] => F[(PhaseResults, FileSystemError \/ C)],
    more: C => F[FileSystemError \/ (C, Vector[Data])],
    close: C => F[Unit],
    explain: Fix[LogicalPlan] => F[(PhaseResults, FileSystemError \/ ExecutionPlan)],
    listContents: ADir => F[FileSystemError \/ Set[PathSegment]],
    fileExists: AFile => F[Boolean]
  )(implicit
    cursors: Kvs[F, QueryFile.ResultHandle, C],
    idSeq: MonoSeq[F]
  ) = λ[QueryFile ~> F] {
    case QueryFile.ExecutePlan(lp, out) => execute(lp, out)
    case QueryFile.Explain(lp)          => explain(lp)
    case QueryFile.ListContents(dir)    => listContents(dir)
    case QueryFile.FileExists(file)     => fileExists(file)

    case QueryFile.EvaluatePlan(lp) =>
      evaluate(lp) flatMap { case (phaseResults, orCursor) =>
        val handle = for {
          cursor <- EitherT.fromDisjunction[F](orCursor)
          id     <- idSeq.next.liftM[FileSystemErrT]
          h      =  QueryFile.ResultHandle(id)
          _      <- cursors.put(h, cursor).liftM[FileSystemErrT]
        } yield h

        handle.run strengthL phaseResults
      }

    case QueryFile.More(h) =>
      (for {
        cursor <- OptionT(cursors.get(h)).toRight(unknownResultHandle(h))
        result <- EitherT(more(cursor))
        (nextCursor, data) = result
        _      <- cursors.put(h, nextCursor).liftM[FileSystemErrT]
      } yield data).run

    case QueryFile.Close(h) =>
      OptionT(cursors.get(h))
        .flatMapF(c => close(c) *> cursors.delete(h))
        .orZero
  }

  def queryFileFromDataCursor[C, F[_]: Monad: Kvs[?[_], QueryFile.ResultHandle, C]: MonoSeq](
    execute: (Fix[LogicalPlan], AFile) => F[(PhaseResults, FileSystemError \/ AFile)],
    evaluate: Fix[LogicalPlan] => F[(PhaseResults, FileSystemError \/ C)],
    explain: Fix[LogicalPlan] => F[(PhaseResults, FileSystemError \/ ExecutionPlan)],
    listContents: ADir => F[FileSystemError \/ Set[PathSegment]],
    fileExists: AFile => F[Boolean]
  )(implicit
    C: DataCursor[F, C]
  ): QueryFile ~> F =
    queryFile[C, F](
      execute,
      evaluate,
      c => C.nextChunk(c) map (_.right[FileSystemError] strengthL c),
      C.close,
      explain,
      listContents,
      fileExists)
}
