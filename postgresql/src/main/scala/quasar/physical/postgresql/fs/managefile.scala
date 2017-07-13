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

package quasar.physical.postgresql.fs

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.effect.MonotonicSeq
import quasar.fp.free, free._
import quasar.fp.ski.κ2
import quasar.fs._
import quasar.physical.postgresql.common._

import doobie.imports._
import pathy.Path._
import scalaz._, Scalaz._
import shapeless.HNil

object managefile {
  import ManageFile._

  def interpret[S[_]](implicit
    S0: MonotonicSeq :<: S,
    S1: ConnectionIO :<: S
  ): ManageFile ~> Free[S, ?] = new (ManageFile ~> Free[S, ?]) {
    def apply[A](fs: ManageFile[A]) = fs match {
      case Move(scenario, semantics) => move(scenario, semantics)
      case Delete(path) => delete(path)
      case TempFile(path) => tempFile(path)
    }
  }

  def move[S[_]](
    scenario: MoveScenario, semantics: MoveSemantics
  )(implicit
    S0: ConnectionIO :<: S
  ): Free[S, FileSystemError \/ Unit] =
    (for {
      src       <- EitherT(dbTableFromPath(scenario.src).point[Free[S, ?]])
      dst       <- EitherT(dbTableFromPath(scenario.dst).point[Free[S, ?]])
      _         <- EitherT((
                     if (src.db =/= dst.db) FileSystemError.pathErr(
                       PathError.invalidPath(scenario.dst, "different db from src path")).left
                     else
                       ().right
                   ).point[Free[S, ?]])
      srcTables <- lift(tablesWithPrefix(src.table)).into.liftM[FileSystemErrT]
      _         <- EitherT((
                     if (srcTables.isEmpty)
                       FileSystemError.pathErr(PathError.pathNotFound(scenario.src)).left
                     else
                       ().right
                   ).point[Free[S, ?]])
      dstExists <- lift(tableExists(dst.table)).into.liftM[FileSystemErrT]
      _         <- EitherT((semantics match {
                     case MoveSemantics.FailIfExists if dstExists =>
                       FileSystemError.pathErr(PathError.pathExists(scenario.dst)).left
                     case MoveSemantics.FailIfMissing if !dstExists =>
                       FileSystemError.pathErr(PathError.pathNotFound(scenario.dst)).left
                     case _ =>
                       ().right[FileSystemError]
                   }).point[Free[S, ?]])
      tblsToMv  =  scenario.fold(κ2(srcTables), κ2(List(src.table)))
      _         <- lift(tblsToMv.traverse { srcTable =>
                     val dstTable = dst.table + srcTable.stripPrefix(src.table)

                     val dropQStr  = s"""DROP TABLE IF EXISTS "$dstTable""""
                     val alterQStr = s"""ALTER TABLE "$srcTable" RENAME TO "$dstTable" """

                     Update[HNil](dropQStr, none).toUpdate0(HNil).run *>
                     Update[HNil](alterQStr, none).toUpdate0(HNil).run
                   }).into.liftM[FileSystemErrT]
    } yield ()).run

  def delete[S[_]](
    path: APath
  )(implicit
    S0: ConnectionIO :<: S
  ): Free[S, FileSystemError \/ Unit] =
    (for {
      dt   <- EitherT(dbTableFromPath(path).point[Free[S, ?]])
      tbls <- lift(tablesWithPrefix(dt.table)).into.liftM[FileSystemErrT]
      _    <- EitherT((
                if (tbls.isEmpty) FileSystemError.pathErr(PathError.pathNotFound(path)).left
                else ().right
              ).point[Free[S, ?]])
      _    <- lift(tbls.traverse { tableName =>
                val qStr = s"""DROP TABLE IF EXISTS "$tableName""""
                Update[HNil](qStr, none).toUpdate0(HNil).run
              }).into.liftM[FileSystemErrT]
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
