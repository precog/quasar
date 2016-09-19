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

package quasar.physical.postgresql.fs

import quasar.Predef._
import quasar.contrib.pathy._
import quasar.fp.κ
import quasar.fp.free._
import quasar.fs._
import quasar.physical.postgresql.common._

import doobie.imports.ConnectionIO
import scalaz._, Scalaz._

object queryfile {
  import QueryFile._

  def interpret[S[_]](
    implicit
    S0: ConnectionIO :<: S
  ): QueryFile ~> Free[S, ?] = new (QueryFile ~> Free[S, ?]) {
    def apply[A](qf: QueryFile[A]) = qf match {
      case ExecutePlan(lp, out) => ???
      case EvaluatePlan(lp) => ???
      case More(h) => ???
      case Close(h) => ???
      case Explain(lp) => ???
      case ListContents(dir) => listContents(dir)
      case FileExists(file) => fileExists(file)
    }
  }

  def listContents[S[_]](
    dir: APath
  )(implicit
    S0: ConnectionIO :<: S
  ): Free[S, FileSystemError \/ Set[PathSegment]] =
    (for {
      dt  <- EitherT(dbTableFromPath(dir).point[Free[S, ?]])
      r   <- lift(tablesWithPrefix(dt.table)).into.liftM[FileSystemErrT]
      _   <- EitherT((
               if (r.isEmpty) FileSystemError.pathErr(PathError.pathNotFound(dir)).left
               else ().right
             ).point[Free[S, ?]])
    } yield pathSegmentsFromPrefix(dt.table, r)).run

  def fileExists[S[_]](
    file: AFile
  )(implicit
    S0: ConnectionIO :<: S
  ): Free[S, Boolean] =
    (for {
      dt  <- EitherT(dbTableFromPath(file).point[Free[S, ?]])
      r   <- lift(tableExists(dt.table)).into.liftM[FileSystemErrT]
    } yield r).leftMap(κ(false)).merge[Boolean]


}
