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
import quasar.{Data, DataCodec}
import quasar.contrib.pathy._
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.fp.free._
import quasar.fs._
import quasar.physical.postgresql.common._

import doobie.imports._
import scalaz._, Scalaz._
import shapeless.HNil

object writefile {
  import WriteFile._

  implicit val codec: DataCodec = DataCodec.Precise

  final case class TableName(v: String)

  def interpret[S[_]](
    implicit
    S0: KeyValueStore[WriteHandle, TableName, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: ConnectionIO :<: S
  ): WriteFile ~> Free[S, ?] =
    new (WriteFile ~> Free[S, ?]) {
      def apply[A](wf: WriteFile[A]) = wf match {
        case Open(file) => open(file)
        case Write(h, data) => write(h, data)
        case Close(h) => close(h)
      }
    }

  def writeHandles[S[_]](
    implicit
    S0: KeyValueStore[WriteHandle, TableName, ?] :<: S
  ) = KeyValueStore.Ops[WriteHandle, TableName, S]

  def open[S[_]](
    file: AFile
  )(implicit
    S0: KeyValueStore[WriteHandle, TableName, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: ConnectionIO :<: S
  ): Free[S, FileSystemError \/ WriteHandle] =
    (for {
      dt   <- EitherT(dbTableFromPath(file).point[Free[S, ?]])
      tbEx <- lift(tableExists(dt.table)).into.liftM[FileSystemErrT]
      _    <- (
                if (tbEx)
                  ().point[Free[S, ?]]
                else {
                  // TODO: Issue error if table name length is too long
                  val qStr = s"""CREATE TABLE "${dt.table}" (v json)"""
                  lift(Update[HNil](qStr, none).toUpdate0(HNil).run.void).into
                }
              ).liftM[FileSystemErrT]
      i    <- MonotonicSeq.Ops[S].next.liftM[FileSystemErrT]
      h    =  WriteHandle(file, i)
      _    <- writeHandles.put(h, TableName(dt.table)).liftM[FileSystemErrT]
    } yield h).run

  def escape(json: String) = json.replace("'", "''")

  // TODO: https://github.com/quasar-analytics/quasar/issues/1363
  def insertQueryStr(table: String)(json: String) =
    s"""insert into "$table"
       |  select * from
       |  json_populate_record(NULL::"$table", '{"v": ${escape(json)}}')
       |""".stripMargin

  def write[S[_]](
    h: WriteHandle,
    chunk: Vector[Data]
  )(implicit
    S0: KeyValueStore[WriteHandle, TableName, ?] :<: S,
    S1: ConnectionIO :<: S
  ): Free[S, Vector[FileSystemError]] =
    (for {
      tbl  <- writeHandles.get(h).toRight(Vector(FileSystemError.unknownWriteHandle(h)))
      data =  chunk.map(DataCodec.render).unite
      qry  =  insertQueryStr(tbl.v) _
      _    <- lift(data.traverse(d =>
                Update[HNil](qry(d), none).toUpdate0(HNil).run.void
              )).into.liftM[EitherT[?[_], Vector[FileSystemError], ?]]
    } yield Vector.empty).merge

  def close[S[_]](
    h: WriteHandle
  )(implicit
    S0: KeyValueStore[WriteHandle, TableName, ?] :<: S
  ): Free[S, Unit] =
    writeHandles.delete(h).liftM[OptionT].run.void

}
