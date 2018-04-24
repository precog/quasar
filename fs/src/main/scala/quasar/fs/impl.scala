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

package quasar.fs

import slamdata.Predef._
import quasar.Data
import quasar.common.PhaseResults
import quasar.contrib.pathy._
import quasar.effect.{Kvs, MonoSeq}
import quasar.fs.PathError._
import quasar.fs.MoveSemantics._
import quasar.fp.numeric._
import quasar.frontend.logicalplan.LogicalPlan

import matryoshka.data.Fix
import scalaz._, Scalaz._
import scalaz.stream.{Writer => PWriter, Process}

object impl {
  import FileSystemError._

  type DataStream[F[_]] = PWriter[F, FileSystemError, Vector[Data]]

  def dataStreamRead[F[_]: Monad: Catchable](s: DataStream[F]): F[FileSystemError \/ (DataStream[F], Vector[Data])] =
    s.unconsOption map {
      case Some((chunk, next)) => chunk strengthL next
      case None                => (Process.empty: DataStream[F], Vector.empty[Data]).right[FileSystemError]
    }

  def dataStreamClose[F[_]: Monad: Catchable](s: DataStream[F]): F[Unit] =
    s.kill.run

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

  def readFromProcess[
    F[_]: Monad: Catchable,
    G[_]: Monad: Kvs[?[_], ReadFile.ReadHandle, DataStream[F]]: MonoSeq
  ](
    fToG: F ~> G)(
    f: (AFile, ReadOpts) => G[FileSystemError \/ DataStream[F]]
  ): ReadFile ~> G =
    read[DataStream[F], G](f, s => fToG(dataStreamRead(s)), s => fToG(dataStreamClose(s)))

  def queryFile[C, F[_]: Monad](
    execute: (Fix[LogicalPlan], AFile) => F[(PhaseResults, FileSystemError \/ Unit)],
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
    case QueryFile.ListContents(dir)    => listContents(dir).map(_.map(set => set.map(Node.fromSegment)))
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
    execute: (Fix[LogicalPlan], AFile) => F[(PhaseResults, FileSystemError \/ Unit)],
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

  def queryFileFromProcess[
    F[_]: Monad: Catchable,
    G[_]: Monad: Kvs[?[_], QueryFile.ResultHandle, DataStream[F]]: MonoSeq
  ](fToG: F ~> G,
    execute: (Fix[LogicalPlan], AFile) => G[(PhaseResults, FileSystemError \/ Unit)],
    evaluate: Fix[LogicalPlan] => G[(PhaseResults, FileSystemError \/ DataStream[F])],
    explain: Fix[LogicalPlan] => G[(PhaseResults, FileSystemError \/ ExecutionPlan)],
    listContents: ADir => G[FileSystemError \/ Set[PathSegment]],
    fileExists: AFile => G[Boolean]
  ): QueryFile ~> G =
    queryFile[DataStream[F], G](
      execute,
      evaluate,
      s => fToG(dataStreamRead(s)),
      s => fToG(dataStreamClose(s)),
      explain,
      listContents,
      fileExists)
}
