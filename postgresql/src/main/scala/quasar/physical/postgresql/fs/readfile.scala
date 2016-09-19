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
import quasar.DataCodec
import quasar.contrib.pathy._
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.fp.free._
import quasar.fs._
import quasar.physical.postgresql.common._

import doobie.imports._
import eu.timepit.refined.api.RefType.ops._
import shapeless.HNil
import scalaz._, Scalaz._

object readfile {

  implicit val codec = DataCodec.Precise

  def interpret[S[_]](
    implicit
    S0: KeyValueStore[ReadFile.ReadHandle, impl.ReadStream[ConnectionIO], ?] :<: S,
    S1: MonotonicSeq :<: S,
    S3: ConnectionIO :<: S
  ): ReadFile ~> Free[S, ?] =
    impl.readFromProcess[S, ConnectionIO] { (file: AFile, readOpts: impl.ReadOpts) =>
      (for {
        dt <- EitherT(dbTableFromPath(file).point[Free[S, ?]])
        _  <- EitherT(lift(tableExists(dt.table)).into.map(_
                .either(())
                .or(FileSystemError.pathErr(PathError.pathNotFound(file)))))
      } yield {
        val lim = readOpts.limit.map(lim => s"limit ${lim.unwrap}").orZero

        // TODO: https://github.com/quasar-analytics/quasar/issues/1363
        val qStr = s"""select v from "${dt.table}" $lim offset ${readOpts.offset.unwrap}"""
        Query[HNil, String](qStr, none)
          .toQuery0(HNil)
          .process
          .chunk(1024) // arbitrary size for the moment
          .map(_.traverse(s => DataCodec.parse(s).leftMap(
            err => FileSystemError.readFailed(s, err.shows))))
      }).run
    }
}
