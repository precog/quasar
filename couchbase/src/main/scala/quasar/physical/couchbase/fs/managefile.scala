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
import quasar.contrib.scalaz._, eitherT._
import quasar.effect.MonotonicSeq
import quasar.fp.free._
import quasar.fs._, ManageFile._
import quasar.physical.couchbase._, common._, Couchbase._

import pathy.Path._
import scalaz._, Scalaz._

abstract class managefile {
  def move(scenario: MoveScenario, semantics: MoveSemantics): Backend[Unit] =
    for {
      ctx       <- MR.asks(_.ctx)
      src       =  docTypeValueFromPath(scenario.src)
      dst       =  docTypeValueFromPath(scenario.dst)
      srcExists <- ME.unattempt(lift(existsWithPrefix(ctx, src.v)).into[Eff].liftB)
      _         <- srcExists.unlessM(
                     ME.raiseError(FileSystemError.pathErr(PathError.pathNotFound(scenario.src))))
      dstExists <- ME.unattempt(lift(existsWithPrefix(ctx, dst.v)).into[Eff].liftB)
      _         <- semantics match {
                    case MoveSemantics.FailIfExists if dstExists =>
                      ME.raiseError(FileSystemError.pathErr(PathError.pathExists(scenario.dst)))
                    case MoveSemantics.FailIfMissing if !dstExists =>
                      ME.raiseError(FileSystemError.pathErr(PathError.pathNotFound(scenario.dst)))
                    case _ =>
                      ().η[Backend]
                   }
      _         <- dstExists.whenM(delete(scenario.dst))
      qStr      =  s"""update `${ctx.bucket.name}`
                       set `${ctx.docTypeKey.v}`=("${dst.v}" || REGEXP_REPLACE(`${ctx.docTypeKey.v}`, "^${src.v}", ""))
                       where `${ctx.docTypeKey.v}` like "${src.v}%""""
      _         <- ME.unattempt(lift(query(ctx.bucket, qStr)).into[Eff].liftB)
    } yield ()

  def delete(path: APath): Backend[Unit] =
    for {
      ctx       <- MR.asks(_.ctx)
      col       =  docTypeValueFromPath(path)
      docsExist <- ME.unattempt(lift(existsWithPrefix(ctx, col.v)).into[Eff].liftB)
      _         <- docsExist.unlessM(
                     ME.raiseError(FileSystemError.pathErr(PathError.pathNotFound(path))))
      _         <- ME.unattempt(lift(deleteHavingPrefix(ctx, col.v)).into[Eff].liftB)
    } yield ()

  def tempFile(near: APath): Backend[AFile] =
    MonotonicSeq.Ops[Eff].next.map { i =>
      val tmpFilename = file(s"__quasar_tmp_$i")
      refineType(near).fold(
        d => d </> tmpFilename,
        f => fileParent(f) </> tmpFilename)
    }.liftB
}
