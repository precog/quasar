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

package quasar.yggdrasil.table

import quasar.Disposable
import quasar.blueeyes.json.JValue
import quasar.contrib.cats.effect._
import quasar.contrib.pathy.{ADir, AFile, APath, PathSegment, firstSegmentName}
import quasar.niflheim.NIHDB
import quasar.precog.common.{Path => PrecogPath}
import quasar.precog.util.IOUtils
import quasar.yggdrasil.{Config, ExactSize, Schema, TransSpecModule}
import quasar.yggdrasil.bytecode.JType
import quasar.yggdrasil.nihdb.{NIHDBProjection, SegmentsWrapper}
import quasar.yggdrasil.vfs._

import java.util.concurrent.{ConcurrentHashMap, ScheduledThreadPoolExecutor, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger
import java.io.File
import java.nio.file.Files
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import akka.actor.{ActorRef, ActorSystem}
import cats.effect._
import fs2.Stream
import org.slf4s.Logging
import pathy.Path
import scalaz.{-\/, EitherT, Monad, OptionT, Scalaz, StreamT, \/}
import Scalaz._
import cats.effect.concurrent.{Deferred, Ref}
import shims._

trait VFSColumnarTableModule extends BlockStoreColumnarTableModule with Logging {
  private type ET[F[_], A] = EitherT[F, ResourceError, A]

  private val dbs = new ConcurrentHashMap[(Blob, Version), NIHDB]

  // TODO 20 is the old default size; need to actually tune this
  private val TxLogScheduler = new ScheduledThreadPoolExecutor(20,
    new ThreadFactory {
      private val counter = new AtomicInteger(0)

      def newThread(r: Runnable): Thread = {
        val t = new Thread(r)
        t.setName("HOWL-sched-%03d".format(counter.getAndIncrement()))

        t
      }
    })

  implicit def ExecutionContext: ExecutionContext
  implicit def cs = IO.contextShift(ExecutionContext)

  def CookThreshold: Int
  def StorageTimeout: FiniteDuration

  def actorSystem: ActorSystem
  private[this] implicit def _actorSystem = actorSystem

  def vfs: SerialVFS[IO]

  def masterChef: ActorRef

  def openDB(path: AFile): OptionT[IO, ResourceError \/ NIHDB] = {
    val failure = ResourceError.fromExtractorError(s"failed to open NIHDB in path $path")

    for {
      blob <- OptionT(vfs.readPath(path))
      version <- OptionT(vfs.headOfBlob(blob))

      cached <- IO(Option(dbs.get((blob, version)))).liftM[OptionT]

      nihdb <- cached match {
        case Some(nihdb) =>
          nihdb.right[ResourceError].point[OptionT[IO, ?]]

        case None =>
          for {
            dir <- vfs.underlyingDir(blob, version).liftM[OptionT]

            tndb = IO {
              NIHDB.open(
                masterChef,
                dir,
                CookThreshold,
                StorageTimeout,
                TxLogScheduler).unsafePerformIO()
            }

            validation <- OptionT(tndb)

            back <- validation.disjunction.leftMap(failure) traverse { nihdb: NIHDB =>
              for {
                check <- IO(dbs.putIfAbsent((blob, version), nihdb)).liftM[OptionT]

                back <- if (check == null)
                  nihdb.right[ResourceError].point[OptionT[IO, ?]]
                else
                  IO.fromFutureShift(IO(nihdb.close)).liftM[OptionT] >> openDB(path)
              } yield back
            }
          } yield back.flatMap(x => x)
      }
    } yield nihdb
  }

  def createDB(path: AFile): IO[ResourceError \/ (Blob, Version, NIHDB)] = {
    val failure = ResourceError.fromExtractorError(s"Failed to create NIHDB in $path")

    val createBlob: IO[Blob] = for {
      blob <- vfs.scratch
      _ <- vfs.link(blob, path)   // should be `true` because we already looked for path
    } yield blob

    for {
      maybeBlob <- vfs.readPath(path)
      blob <- maybeBlob.map(IO.pure(_)).getOrElse(createBlob)

      version <- vfs.fresh(blob)    // overwrite any previously-existing HEAD
      dir <- vfs.underlyingDir(blob, version)

      validation <- IO {
        NIHDB.create(
          masterChef,
          dir,
          CookThreshold,
          StorageTimeout,
          TxLogScheduler).unsafePerformIO()
      }

      disj = validation.disjunction.leftMap(failure)
    } yield disj.map(n => (blob, version, n))
  }

  /**
   * Closes the database and removes it from the internal state. This doesn't
   * remove the dataset from disk.
   */
  def closeDB(path: AFile): IO[Unit] = {
    val back = for {
      blob <- OptionT(vfs.readPath(path))
      head <- OptionT(vfs.headOfBlob(blob))
      db <- OptionT(IO(Option(dbs.get((blob, head)))))
      _ <- IO.fromFutureShift(IO(db.close)).liftM[OptionT]
      _ <- IO(dbs.remove((blob, head), db)).liftM[OptionT]
    } yield ()

    back.run.map(_ => ())
  }

  /**
    * Commits the new version and closes (if exists) the old version of the database.
    */
  def commitDB(blob: Blob, version: Version, db: NIHDB): IO[Boolean] = {
    val closeOldM = for {
      oldHead <- OptionT(vfs.headOfBlob(blob))
      oldDB <- OptionT(IO(Option(dbs.get((blob, oldHead)))))
      _ <- IO(log.trace(s"closing old database for blob $blob and version $oldHead")).liftM[OptionT]
      _ <- IO.fromFutureShift(IO(oldDB.close)).liftM[OptionT]
      check <- IO(dbs.remove((blob, oldHead), oldDB)).liftM[OptionT]
    } yield check

    for {
      check <- IO(dbs.putIfAbsent((blob, version), db))
      _ <- if (check == null)
        for {
          _ <- IO(log.trace(s"committing blob $blob and version $version"))
          _ <- vfs.commit(blob, version)
          _ <- closeOldM.run      // just run this for the effect; we don't really care if it passes or fails
        } yield ()
      else
        IO(log.trace(s"commit on blob $blob and version $version failed due to race condition (should be impossible?)"))
    } yield check == null;
  }

  def ingest(path: PrecogPath, ingestion: Stream[IO, JValue]): EitherT[IO, ResourceError, Unit] = {
    for {
      afile <- pathToAFileET[IO](path)
      triple <- EitherT.eitherT(createDB(afile))
      (blob, version, nihdb) = triple

      actions = ingestion.chunks.zipWithIndex evalMap {
        case (jvs, offset) =>
          // insertVerified forces sequential behavior and backpressure
          IO.fromFutureShift(IO(nihdb.insertVerified(NIHDB.Batch(offset, jvs.toList) :: Nil)))
      }

      driver = actions.drain ++ Stream.eval(commitDB(blob, version, nihdb))

      _ <- EitherT.rightT[IO, ResourceError, Unit](driver.compile.drain)
    } yield ()
  }

  // note: this function should almost always be unnecessary, since nihdb includes the append log in snapshots
  def flush(path: AFile): IO[Unit] = {
    val ot = for {
      blob <- OptionT(vfs.readPath(path))
      head <- OptionT(vfs.headOfBlob(blob))
      db <- OptionT(IO(Option(dbs.get((blob, head)))))
      _ <- IO.fromFutureShift(IO(db.cook)).liftM[OptionT]
    } yield ()

    ot.getOrElseF(IO.unit)
  }

  /**
   * Produces a Table which will, when run the first time, write itself out to
   * NIHDB storage in a temporary directory, which in turn the subsequent reads
   * will consume. The very first running of the table must be completed entirely
   * before subsequent reads may proceed. Once the first read is completed, all
   * subsequent reads may happen in parallel. The disposal action produced will
   * shut down the temporary database and remove the directory.
   *
   * If `earlyForce` is `true`, then the effect of running the *entire* input
   * table stream and writing it out to disk will be sequenced into the outer
   * `IO`. This flag is important in the event that the output `Table` will be
   * sequenced multiple times *interleaved*, which is of course a deadlock.
   */
  def cacheTable(table: Table, earlyForce: Boolean): IO[Disposable[IO, Table]] = {
    def zipWithIndex[F[_]: Monad, A](st: StreamT[F, A]): StreamT[F, (A, Int)] = {
      StreamT.unfoldM((st, 0)) {
        case (st, index) =>
          st.uncons map {
            case Some((head, tail)) =>
              Some(((head, index), (tail, index + 1)))

            case None => None
          }
      }
    }

    val target = table.compact(trans.TransSpec1.Id).canonicalize(Config.maxSliceRows)

    // hey! we fit in memory for sure
    if (target.size.maxSize < Config.maxSliceRows) {
      target.force.map(Disposable(_, IO.pure(())))
    } else {
      for {
        started <- Ref.of[IO, Boolean](false)
        cacheTarget <- Deferred[IO, (NIHDB, File)]

        slices = target.slices
        size = target.size

        cachedSlicesM = for {
          change <- started.tryModify(b => (true, b))

          winner = change match {
            case Some(false) => true
            case Some(true) | None => false   // either we lost a race, or we won but it was already true
          }

          streamM = if (winner) {
            // persist the stream incrementally into a temporary directory
            for {
              dir <- IO(Files.createTempDirectory("BlockStoreColumnarTable-" + hashCode).toFile)
              dbV <- IO {
                NIHDB.create(
                  masterChef,
                  dir,
                  CookThreshold,
                  StorageTimeout,
                  TxLogScheduler).unsafePerformIO
              }

              // I don't really care. If we fail to store, the stream kinda has to die
              db <- dbV.toEither.fold(
                _ => IO.raiseError(new RuntimeException("unable to create nihdb for dir: " + dir)),
                IO.pure(_))
            } yield {
              val persisting = zipWithIndex(slices) mapM {
                case (slice, offset) =>
                  val segments = SegmentsWrapper.sliceToSegments(offset.toLong, slice)

                  IO.fromFutureShift(
                    IO(db.insertSegmentsVerified(offset.toLong, slice.size, segments))).as(slice)
              }

              persisting ++ StreamT.wrapEffect(cacheTarget.complete((db, dir)).as(StreamT.empty[IO, Slice]))
            }
          } else {
            for {
              pair <- cacheTarget.get    // wait for the other thread to finish the persistence
              (db, _) = pair
              proj <- NIHDBProjection.wrap(db)
            } yield proj.getBlockStream(None).map(_.deref(TransSpecModule.paths.Value))
          }
        } yield StreamT.wrapEffect[IO, Slice](streamM)

        cachedSlices = StreamT.wrapEffect[IO, Slice](cachedSlicesM)

        // if we need to force early, then run the stream once for effect and discard the slices
        // this will force subsequent runs to use the cache
        _ <- if (earlyForce)
          cachedSlices.foreach(_ => IO.pure(()))
        else
          IO.pure(())

        dispose = for {
          running <- started.get

          // cheater bypass (both bonus race condition!) for when we haven't run the stream at all
          _ <- if (running) {
            for {
              pair <- cacheTarget.get
              (db, dir) = pair
              _ <- IO.fromFutureShift(IO(db.close))
              _ <- IO(IOUtils.recursiveDelete(dir).unsafePerformIO)
            } yield ()
          } else {
            IO.pure(())
          }
        } yield ()
      } yield Disposable(Table(cachedSlices, size), dispose)
    }
  }

  object fs {
    def listContents(dir: ADir): IO[Set[PathSegment]] = {
      vfs.ls(dir) map { paths =>
        paths.map(firstSegmentName).flatMap(_.toList).toSet
      }
    }

    def exists(path: APath): IO[Boolean] = vfs.exists(path)

    def moveFile(from: AFile, to: AFile, semantics: MoveSemantics): IO[Boolean] =
      vfs.moveFile(from, to, semantics)

    def moveDir(from: ADir, to: ADir, semantics: MoveSemantics): IO[Boolean] =
      vfs.moveDir(from, to, semantics)

    def delete(target: APath): IO[Boolean] = {
      def deleteFile(target: AFile) = {
        for {
          _ <- {
            val ot = for {
              blob <- OptionT(vfs.readPath(target))
              version <- OptionT(vfs.headOfBlob(blob))
              nihdb <- OptionT(IO(Option(dbs.get((blob, version)))))

              removed <- IO(dbs.remove((blob, version), nihdb)).liftM[OptionT]

              _ <- if (removed)
                IO.fromFutureShift(IO(nihdb.close)).liftM[OptionT]
              else
                ().point[OptionT[IO, ?]]
            } yield ()

            ot.run
          }

          back <- vfs.delete(target)
        } yield back
      }

      def deleteDir(target: ADir): IO[Boolean] = {
        for {
          paths <- vfs.ls(target)

          results <- paths traverse { path =>
            delete(target </> path)
          }
        } yield results.forall(_ == true)
      }

      Path.refineType(target).fold(deleteDir, deleteFile)
    }
  }

  private def pathToAFile(path: PrecogPath): Option[AFile] = {
    val parent = path.elements.dropRight(1).foldLeft(Path.rootDir)(_ </> Path.dir(_))
    path.elements.lastOption.map(parent </> Path.file(_))
  }

  private def pathToAFileET[F[_]: Monad](path: PrecogPath): EitherT[F, ResourceError, AFile] = {
    pathToAFile(path) match {
      case Some(afile) =>
        EitherT.rightT[F, ResourceError, AFile](afile.point[F])

      case None =>
        EitherT.leftT[F, ResourceError, AFile] {
          val err: ResourceError = ResourceError.notFound(
            s"invalid path does not contain file element: $path")

          err.point[F]
        }
    }
  }

  trait VFSColumnarTableCompanion extends BlockStoreColumnarTableCompanion {

    def load(table: Table, tpe: JType): EitherT[IO, ResourceError, Table] = {
      for {
        _ <- EitherT.rightT(table.toJson.map(json => log.trace("Starting load from " + json.toList.map(_.toJValue.renderCompact))))
        paths <- EitherT.rightT(pathsM(table))

        projections <- paths.toList traverse { path =>
          for {
            _ <- IO(log.debug("Loading path: " + path)).liftM[ET]

            afile <- pathToAFileET[IO](path)

            nihdb <- EitherT.eitherT[IO, ResourceError, NIHDB](openDB(afile).run map { opt =>
              opt.getOrElse(-\/(ResourceError.notFound(s"unable to locate dataset at path $path")))
            })

            projection <- NIHDBProjection.wrap(nihdb).liftM[ET]
          } yield projection
        }
      } yield {
        val length = projections.map(_.length).sum
        val stream = projections.foldLeft(StreamT.empty[IO, Slice]) { (acc, proj) =>
          // FIXME: Can Schema.flatten return Option[Set[ColumnRef]] instead?
          val constraints =
            Some(Schema.flatten(tpe, proj.structure.toList))

          log.debug("Appending from projection: " + proj)
          acc ++ proj.getBlockStream(constraints)
        }

        Table(stream, ExactSize(length))
      }
    }
  }
}
