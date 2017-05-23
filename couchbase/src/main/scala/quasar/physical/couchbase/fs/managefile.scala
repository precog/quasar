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

package quasar.physical.couchbase.fs

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.effect.{MonotonicSeq, Read}
import quasar.fp.free._
import quasar.fs._
import quasar.physical.couchbase.common._

import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object managefile {
  import ManageFile._

  def interpret[S[_]](implicit
    S0: MonotonicSeq :<: S,
    S1: Read[ClientContext, ?] :<:  S,
    S2: Task :<: S
  ): ManageFile ~> Free[S, ?] = λ[ManageFile ~> Free[S, ?]] {
    case Move(scenario, semantics) => move(scenario, semantics)
    case Delete(path)              => delete(path)
    case TempFile(path)            => tempFile(path)
  }

  def move[S[_]](
    scenario: MoveScenario, semantics: MoveSemantics
  )(implicit
    S0: Task :<: S,
    context: Read.Ops[ClientContext, S]
  ): Free[S, FileSystemError \/ Unit] =
    (for {
      ctx       <- context.ask.liftM[FileSystemErrT]
      src       <- docTypeValueFromPath(scenario.src).η[FileSystemErrT[Free[S, ?], ?]]
      dst       <- docTypeValueFromPath(scenario.dst).η[FileSystemErrT[Free[S, ?], ?]]
      srcExists <- EitherT(lift(existsWithPrefix(ctx, src.v)).into)
      _         <- EitherT((
                     if (!srcExists)
                       FileSystemError.pathErr(PathError.pathNotFound(scenario.src)).left
                     else
                       ().right
                   ).η[Free[S, ?]])
      dstExists <- EitherT(lift(existsWithPrefix(ctx, dst.v)).into)
      _         <- EitherT((semantics match {
                    case MoveSemantics.FailIfExists if dstExists =>
                      FileSystemError.pathErr(PathError.pathExists(scenario.dst)).left
                    case MoveSemantics.FailIfMissing if !dstExists =>
                      FileSystemError.pathErr(PathError.pathNotFound(scenario.dst)).left
                    case _ =>
                      ().right[FileSystemError]
                  }).η[Free[S, ?]])
      _         <- dstExists.whenM(EitherT(delete(scenario.dst)))
      qStr      =  s"""update `${ctx.bucket.name}`
                       set `${ctx.docTypeKey.v}`=("${dst.v}" || REGEXP_REPLACE(`${ctx.docTypeKey.v}`, "^${src.v}", ""))
                       where `${ctx.docTypeKey.v}` like "${src.v}%""""
      _         <- EitherT(lift(query(ctx.bucket, qStr)).into)
    } yield ()).run

  def delete[S[_]](
    path: APath
  )(implicit
    S0: Task :<: S,
    context: Read.Ops[ClientContext, S]
  ): Free[S, FileSystemError \/ Unit] =
    (for {
      ctx       <- context.ask.liftM[FileSystemErrT]
      col       <- docTypeValueFromPath(path).η[FileSystemErrT[Free[S, ?], ?]]
      docsExist <- EitherT(lift(existsWithPrefix(ctx, col.v)).into)
      _         <- EitherT((
                     if (!docsExist) FileSystemError.pathErr(PathError.pathNotFound(path)).left
                     else ().right
                   ).η[Free[S, ?]])
      _         <- EitherT(lift(deleteHavingPrefix(ctx, col.v)).into)
    } yield ()).run

  def tempFile[S[_]](
    path: APath
  )(implicit
    S0: MonotonicSeq :<: S
  ): Free[S, FileSystemError \/ AFile] =
    MonotonicSeq.Ops[S].next.map { i =>
      val tmpFilename = file(s"__quasar_tmp_$i")
      refineType(path).fold(
        d => d </> tmpFilename,
        f => fileParent(f) </> tmpFilename
      ).right
    }

}
