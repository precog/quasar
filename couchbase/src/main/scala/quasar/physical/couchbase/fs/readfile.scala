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
import quasar.Data
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.fp._, free._, numeric._
import quasar.fs._, ReadFile.ReadHandle
import quasar.physical.couchbase._, common._, Couchbase._

import eu.timepit.refined.api.RefType.ops._
import scalaz._, Scalaz._

abstract class readfile {
  val rh = KeyValueStore.Ops[ReadHandle, Cursor, Eff]

  // TODO: Streaming
  def open(file: AFile, offset: Natural, limit: Option[Positive]): Backend[ReadHandle] =
    for {
      ctx     <- MR.asks(_.ctx)
      col     =  docTypeValueFromPath(file)
      lmt     =  limit.map(lim => s"LIMIT ${lim.unwrap}").orZero
      qStr    =  s"""SELECT ifmissing(d.`value`, d).* FROM `${ctx.bucket.name}` d
                     WHERE `${ctx.docTypeKey.v}`="${col.v}"
                     $lmt OFFSET ${offset.unwrap.shows}"""
      qResult <- ME.unattempt(lift(queryData(ctx.bucket, qStr)).into[Eff].liftB)
      i       <- MonotonicSeq.Ops[Eff].next.liftB
      handle  =  ReadHandle(file, i)
      _       <- rh.put(handle, Cursor(qResult)).liftB
    } yield handle

  def read(h: ReadHandle): Backend[Vector[Data]] =
    for {
      c       <- ME.unattempt(rh.get(h).toRight(FileSystemError.unknownReadHandle(h)).run.liftB)
      (cʹ, r) =  resultsFromCursor(c)
      _       <- rh.put(h, cʹ).liftB
    } yield r

  def close(h: ReadHandle): Configured[Unit] =
    rh.delete(h).liftM[ConfiguredT]
}
