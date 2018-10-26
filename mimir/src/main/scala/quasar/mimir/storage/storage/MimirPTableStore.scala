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

package quasar.mimir.storage

import slamdata.Predef._

import quasar.contrib.cats.effect._
import quasar.contrib.fs2.convert._
import quasar.contrib.pathy.{ADir, AFile}
import quasar.contrib.scalaz.MonadError_
import quasar.mimir.MimirCake.Cake
import quasar.niflheim.NIHDB
import quasar.precog.common.ColumnRef
import quasar.yggdrasil.ExactSize
import quasar.yggdrasil.nihdb.SegmentsWrapper
import quasar.yggdrasil.vfs.ResourceError
import quasar.yggdrasil.nihdb.NIHDBProjection

import cats.arrow.FunctionK
import cats.effect.{ContextShift, IO, LiftIO}

import fs2.Stream
import fs2.concurrent.SignallingRef

import pathy.Path

import scalaz.{-\/, \/-, Monad, OptionT}
import scalaz.syntax.monad._

import shims._

import scala.concurrent.ExecutionContext

/*
 * We don't abstract this because read (and write) use the path-dependent type based on the
 * value of `cake`. It would be nearly impossible to get the types to work out correctly in
 * an abstraction, and probably just yield all sorts of problems for users of this functionality.
 */
final class MimirPTableStore[F[_]: Monad: LiftIO] private (
    val cake: Cake,
    tablesPrefix: ADir)(
    implicit ME: MonadError_[F, ResourceError],
    cs: ContextShift[IO],
    ec: ExecutionContext) {

  import cake.{Table => PTable}

  // has overwrite semantics
  def write(key: StoreKey, table: PTable): Stream[F, Unit] = {

    def doWrite(completed: SignallingRef[IO, Boolean]) =
      Stream.bracket(cake.createDB(keyToFile(key)).map(_.toOption))({
        case Some((blob, version, db)) =>
          completed.get.flatMap { c =>
            if (c)
              cake.commitDB(blob, version, db).void
            else
              IO.fromFutureShift(IO(db.close))  // TODO cleanup the orphaned version (ch1962)
          }

        case None =>
          IO.pure(())
      }).flatMap {
        case Some((_, _, db)) =>
          // we assume the table will be compacted/canonicalized before storage, if desired
          (fromStreamT(table.slices).zipWithIndex evalMap {
            case (slice, offset) =>
              val segments = SegmentsWrapper.sliceToSegments(offset.toLong, slice)
              IO.fromFutureShift(
                IO(db.insertSegmentsVerified(offset.toLong, slice.size, segments)))
          }) ++ Stream.eval_(completed.set(true))

        case None =>
          Stream.empty
      }

    val ios = for {
      completed <- Stream.eval(SignallingRef[IO, Boolean](false))
      res <- doWrite(completed)
    } yield res

    ios.translate(λ[FunctionK[IO, F]](LiftIO[F].liftIO(_))).drain
  }

  def read(key: StoreKey): F[Option[PTable]] = {
    val ioaOpt: OptionT[F, PTable] = for {
      dbOr <- cake.openDB(keyToFile(key)).mapT(LiftIO[F].liftIO(_))

      db <- dbOr match {
        case \/-(db) => db.point[OptionT[F, ?]]
        case -\/(ResourceError.NotFound(_)) => OptionT.none[F, NIHDB]
        case -\/(err) => ME.raiseError(err).liftM[OptionT]
      }

      proj <- LiftIO[F].liftIO(NIHDBProjection.wrap(db)).liftM[OptionT]
      table = PTable(proj.getBlockStream(None), ExactSize(proj.length))

      back = table.transform(cake.trans.constants.SourceValue.Single)
    } yield back

    ioaOpt.run
  }

  def exists(key: StoreKey): F[Boolean] =
    read(key).map(_.isDefined)

  def delete(key: StoreKey): F[Unit] = {
    val path = keyToFile(key)
    LiftIO[F].liftIO(cake.closeDB(path) *> cake.fs.delete(path).as(()))
  }

  def schema(key: StoreKey): F[Option[PTableSchema]] = {
    val optColRef = for {
      dbOr <- cake.openDB(keyToFile(key)).mapT(LiftIO[F].liftIO(_))

      db <- dbOr match {
        case \/-(db) => db.point[OptionT[F, ?]]
        case -\/(ResourceError.NotFound(_)) => OptionT.none[F, NIHDB]
        case -\/(err) => ME.raiseError(err).liftM[OptionT]
      }

      cols <- LiftIO[F].liftIO(IO.fromFutureShift(IO(db.structure))).liftM[OptionT]
    } yield {
      cols match {
        case (s: Set[ColumnRef]) if !s.isEmpty => Some(PTableSchema(s))
        case (s: Set[ColumnRef]) if s.isEmpty => None
      }
    }

    optColRef.run.flatMap(x => x.getOrElse(None).pure[F])
  }


  private[this] def keyToFile(key: StoreKey): AFile =
    tablesPrefix </> Path.file(key.value)
}

object MimirPTableStore {

  def apply[F[_]: Monad: LiftIO: MonadError_[?[_], ResourceError]](
      cake: Cake,
      tablesPrefix: ADir)(
      implicit
      cs: ContextShift[IO],
      ec: ExecutionContext)
      : MimirPTableStore[F] =
    new MimirPTableStore[F](cake, tablesPrefix)
}
