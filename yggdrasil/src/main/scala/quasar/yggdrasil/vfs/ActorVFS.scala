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

package quasar.yggdrasil.vfs

import quasar.yggdrasil._
import quasar.yggdrasil.table.Slice
import quasar.yggdrasil.vfs.ResourceError._

import quasar.niflheim._

import quasar.precog.common._
import quasar.precog.common.ingest._
import quasar.precog.common.security._
import quasar.precog.util._

import quasar.yggdrasil.nihdb.NIHDBProjection
import quasar.yggdrasil.table.ColumnarTableModule

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props, ReceiveTimeout}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import quasar.blueeyes.MimeType
import quasar.blueeyes.json._
import quasar.blueeyes.json.serialization._
import quasar.blueeyes.json.serialization.DefaultSerialization._
import quasar.blueeyes.util.Clock

import org.slf4s.Logging

import scala.annotation.tailrec
import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import scalaz._
import scalaz.EitherT._
import scalaz.effect.IO
import scalaz.std.scalaFuture

import Scalaz.{futureInstance => _, _}   // have to import like this because of syntax ambiguities

import scala.concurrent.{Future, Await}

import java.io.{File, IOException, FileInputStream, FileOutputStream}
import java.util.UUID
import java.util.concurrent.{ScheduledThreadPoolExecutor, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger

case class IngestData(messages: Seq[(Long, EventMessage)])

sealed trait PathActionResponse
sealed trait ReadResult extends PathActionResponse
sealed trait WriteResult extends PathActionResponse
sealed trait MetadataResult extends PathActionResponse

case class UpdateSuccess(path: Path) extends WriteResult
case class PathChildren(path: Path, children: Set[PathMetadata]) extends MetadataResult
case class PathOpFailure(path: Path, error: ResourceError) extends ReadResult with WriteResult with MetadataResult

trait ActorVFSModule extends VFSModule[Future, Slice] {
  type Projection = NIHDBProjection

  def permissionsFinder: PermissionsFinder[Future]
  // def jobManager: JobManager[Future]
  def resourceBuilder: ResourceBuilder

  case class ReadSuccess(path: Path, resource: Resource) extends ReadResult

  /**
   * Used to access resources. This is needed because opening a NIHDB requires
   * more than just a basedir, but also things like the chef, txLogScheduler, etc.
   * This also goes for blobs, where the metadata log requires the txLogScheduler.
   */
  class ResourceBuilder(
    actorSystem: ActorSystem,
    clock: Clock,
    chef: ActorRef,
    cookThreshold: Int,
    storageTimeout: FiniteDuration,
    txLogSchedulerSize: Int = 20) extends Logging { // default for now, should come from config in the future

    private final val txLogScheduler = new ScheduledThreadPoolExecutor(txLogSchedulerSize,
      new ThreadFactory {
        private val counter = new AtomicInteger(0)

        def newThread(r: Runnable): Thread = {
          val t = new Thread(r)
          t.setName("HOWL-sched-%03d".format(counter.getAndIncrement()))

          t
        }
      })

    private implicit val ifm = scalaFuture.futureInstance(actorSystem.dispatcher)

    private def ensureDescriptorDir(versionDir: File): IO[File] = IO {
      if (versionDir.isDirectory || versionDir.mkdirs) versionDir
      else throw new IOException("Failed to create directory for projection: %s".format(versionDir))
    }

    // Resource creation/open and discovery
    def createNIHDB(versionDir: File, authorities: Authorities): IO[ResourceError \/ NIHDBResource] = {
      for {
        nihDir <- ensureDescriptorDir(versionDir)
        nihdbV <- NIHDB.create(chef, authorities, nihDir, cookThreshold, storageTimeout, txLogScheduler)(actorSystem)
      } yield {
        nihdbV.disjunction leftMap {
          ResourceError.fromExtractorError("Failed to create NIHDB in %s as %s".format(versionDir.toString, authorities))
        } map {
          NIHDBResource(_)
        }
      }
    }

    def openNIHDB(descriptorDir: File): IO[ResourceError \/ NIHDBResource] = {
      NIHDB.open(chef, descriptorDir, cookThreshold, storageTimeout, txLogScheduler)(actorSystem) map {
        _ map {
          _.disjunction map { NIHDBResource(_) } leftMap {
            ResourceError.fromExtractorError("Failed to open NIHDB from %s".format(descriptorDir.toString))
          }
        } getOrElse {
          \/.left(NotFound("No NIHDB projection found in %s".format(descriptorDir)))
        }
      }
    }

    final val blobMetadataFilename = "blob_metadata"

    def isBlob(versionDir: File): Boolean = (new File(versionDir, blobMetadataFilename)).exists

    /**
     * Open the blob for reading in `baseDir`.
     */
    def openBlob(versionDir: File): IO[ResourceError \/ FileBlobResource] = IO {
      //val metadataStore = PersistentJValue(versionDir, blobMetadataFilename)
      //val metadata = metadataStore.json.validated[BlobMetadata]
      JParser.parseFromFile(new File(versionDir, blobMetadataFilename)).leftMap(Extractor.Error.thrown).
      flatMap(_.validated[BlobMetadata]).
      disjunction.map { metadata =>
        FileBlobResource(new File(versionDir, "data"), metadata) //(actorSystem.dispatcher)
      } leftMap {
        ResourceError.fromExtractorError("Error reading metadata from versionDir %s".format(versionDir.toString))
      }
    }

    /**
     * Creates a blob from a data stream.
     */
    def createBlob[M[+_]](versionDir: File, mimeType: MimeType, authorities: Authorities, data: StreamT[M, Array[Byte]])(implicit M: Monad[M], IOT: IO ~> M): M[ResourceError \/ FileBlobResource] = {
      def write(out: FileOutputStream, size: Long, stream: StreamT[M, Array[Byte]]): M[ResourceError \/ Long] = {
        stream.uncons.flatMap {
          case Some((bytes, tail)) =>
            try {
              out.write(bytes)
              write(out, size + bytes.length, tail)
            } catch {
              case (ioe: IOException) =>
                out.close()
                \/.left(IOError(ioe)).point[M]
            }

          case None =>
            out.close()
            \/.right(size).point[M]
        }
      }

      Monad[M].map(42.point[M]) { _ => 12 }

      for {
        _ <- IOT { IOUtils.makeDirectory(versionDir) }
        file = (new File(versionDir, "data"))
        _ = log.debug("Creating new blob at " + file)
        writeResult <- write(new FileOutputStream(file), 0L, data)
        blobResult <- IOT {
          writeResult traverse { size =>
            log.debug("Write complete on " + file)
            val metadata = BlobMetadata(mimeType, size, clock.now(), authorities)
            //val metadataStore = PersistentJValue(versionDir, blobMetadataFilename)
            //metadataStore.json = metadata.serialize
            IOUtils.writeToFile(metadata.serialize.renderCompact, new File(versionDir, blobMetadataFilename), false) map { _ =>
              FileBlobResource(file, metadata)
            }
          }
        }
      } yield blobResult
    }
  }


  case class NIHDBResource(val db: NIHDB) extends ProjectionResource with Logging {
    val mimeType: MimeType = FileContent.XQuirrelData

    def authorities = db.authorities

    def append(batch: NIHDB.Batch): IO[Unit] = db.insert(Seq(batch))

    def projection(implicit M: Monad[Future]) = NIHDBProjection.wrap(db)

    def recordCount(implicit M: Monad[Future]) = M.map(projection)(_.length)

    def asByteStream(mimeType: MimeType)(implicit M: Monad[Future]) = OptionT {
      M.map(projection) { p =>
        val slices = p.getBlockStream(None) map VFS.derefValue
        ColumnarTableModule.byteStream(slices, Some(mimeType))
      }
    }
  }

  object FileBlobResource {
    val ChunkSize = 100 * 1024

    def IOF(implicit M: Monad[Future]): IO ~> Future = new NaturalTransformation[IO, Future] {
      def apply[A](io: IO[A]) = M.point(io.unsafePerformIO)
    }
  }

  /**
   * A blob of data that has been persisted to disk.
   */
  final case class FileBlobResource(dataFile: File, metadata: BlobMetadata) extends BlobResource {
    import FileContent._
    import FileBlobResource._

    val authorities: Authorities = metadata.authorities
    val mimeType: MimeType = metadata.mimeType
    val byteLength = metadata.size

    /** Suck the file into a String */
    def asString(implicit M: Monad[Future]): OptionT[Future, String] = OptionT(M point {
      stringTypes.contains(mimeType).option(IOUtils.readFileToString(dataFile)).sequence.unsafePerformIO
    })

    /** Stream the file off disk. */
    def ioStream: StreamT[IO, Array[Byte]] = {
      @tailrec
      def readChunk(fin: FileInputStream, skip: Long): Option[Array[Byte]] = {
        val remaining = skip - fin.skip(skip)
        if (remaining == 0) {
          val bytes = new Array[Byte](ChunkSize)
          val read = fin.read(bytes)

          if (read < 0) None
          else if (read == bytes.length) Some(bytes)
          else Some(java.util.Arrays.copyOf(bytes, read))
        } else {
          readChunk(fin, remaining)
        }
      }

      StreamT.unfoldM[IO, Array[Byte], Long](0L) { offset =>
        IO(new FileInputStream(dataFile)).bracket(f => IO(f.close())) { in =>
          IO(readChunk(in, offset) map { bytes =>
            (bytes, offset + bytes.length)
          })
        }
      }
    }

    override def fold[A](blobResource: BlobResource => A, projectionResource: ProjectionResource => A) = blobResource(this)

    def asByteStream(mimeType: MimeType)(implicit M: Monad[Future]) = OptionT {
      M.point {
        Option(ioStream.trans(IOF))
      }
    }
  }

  class VFSCompanion extends VFSCompanionLike {
    def toJsonElements(slice: Slice) = slice.toJsonElements
    def derefValue(slice: Slice) = slice.deref(TransSpecModule.paths.Value)
    def blockSize(slice: Slice) = slice.size
    def pathStructure(selector: CPath)(implicit M: Monad[Future]) = { (projection: Projection) =>
      right {
        M.map(projection.structure) { children =>
          PathStructure(projection.reduce(Reductions.count, selector), children.map(_.selector))
        }
      }
    }
  }

  object VFS extends VFSCompanion

  class ActorVFS(projectionsActor: ActorRef, projectionReadTimeout: Timeout, sliceIngestTimeout: Timeout)(implicit M: Monad[Future]) extends VFS {

    def writeAll(data: Seq[(Long, EventMessage)]): IO[Unit] = {
      IO { projectionsActor ! IngestData(data) }
    }

    def writeAllSync(data: Seq[(Long, EventMessage)]): EitherT[Future, ResourceError, Unit] = EitherT {
      implicit val timeout = sliceIngestTimeout

      // it's necessary to group by path then traverse since each path will respond to ingest independently.
      // -- a bit of a leak of implementation detail, but that's the actor model for you.
      val ingested = (data groupBy { case (offset, msg) => msg.path }).toStream traverse { case (path, subset) =>
        (projectionsActor ? IngestData(subset)).mapTo[WriteResult]
      }

      M.map(ingested) { allResults =>
        val errors: List[ResourceError] = allResults.toList collect { case PathOpFailure(_, error) => error }
        errors.toNel.map(ResourceError.all).toLeftDisjunction(Unit)
      }
    }

    def readResource(path: Path, version: Version): EitherT[Future, ResourceError, Resource] = {
      implicit val t = projectionReadTimeout
      EitherT {
        M.map((projectionsActor ? Read(path, version)).mapTo[ReadResult]) {
          case ReadSuccess(_, resource) => \/.right(resource)
          case PathOpFailure(_, error) => \/.left(error)
        }
      }
    }

    def findDirectChildren(path: Path): EitherT[Future, ResourceError, Set[PathMetadata]] = {
      implicit val t = projectionReadTimeout
      EitherT {
        M.map((projectionsActor ? FindChildren(path)).mapTo[MetadataResult]) {
          case PathChildren(_, children) => \/.right(for (pm <- children; p0 <- (pm.path - path)) yield { pm.copy(path = p0) })
          case PathOpFailure(_, error) => \/.left(error)
        }
      }
    }

    def findPathMetadata(path: Path): EitherT[Future, ResourceError, PathMetadata] = {
      implicit val t = projectionReadTimeout
      EitherT {
        M.map((projectionsActor ? FindPathMetadata(path)).mapTo[MetadataResult]) {
          case PathChildren(_, children) =>
            children.headOption flatMap { pm =>
              (pm.path - path) map { p0 => pm.copy(path = p0) }
            } toRightDisjunction {
              ResourceError.notFound("Cannot return metadata for path %s".format(path.path))
            }
          case PathOpFailure(_, error) =>
            \/.left(error)
        }
      }
    }


    def currentVersion(path: Path) = {
      implicit val t = projectionReadTimeout
      (projectionsActor ? CurrentVersion(path)).mapTo[Option[VersionEntry]]
    }
  }

  case class IngestBundle(data: Seq[(Long, EventMessage)], perms: Map[APIKey, Set[WritePermission]])

  class PathRoutingActor(baseDir: File, shutdownTimeout: Duration, quiescenceTimeout: Duration, maxOpenPaths: Int, clock: Clock) extends Actor with Logging {
    import quasar.precog.util.cache._
    import quasar.precog.util.cache.Cache._
    import com.google.common.cache.RemovalCause

    private implicit val M: Monad[Future] = scalaFuture.futureInstance(context.dispatcher)

    private[this] val pathLRU = Cache.simple[Path, Unit](
      MaxSize(maxOpenPaths),
      OnRemoval({(p: Path, _: Unit, _: RemovalCause) => pathActors.get(p).foreach(_ ! ReceiveTimeout) }))

    private[this] var pathActors = Map.empty[Path, ActorRef]

    // not atomic
    def stopPath(path: Path): Unit = {
      pathActors.get(path).map(_ ! PoisonPill)
      pathActors -= path
    }

    override def postStop = {
      log.info("Shutdown of path actors complete")
    }

    private[this] def targetActor(path: Path): IO[ActorRef] = {
      pathActors.get(path).map(IO(_)) getOrElse {
        val pathDir = VFSPathUtils.pathDir(baseDir, path)

        for {
          _ <- IOUtils.makeDirectory(pathDir)
          _ = log.debug("Created new path dir for %s : %s".format(path, pathDir))
          vlog <- VersionLog.open(pathDir)
          actorV <- vlog traverse { versionLog =>
            log.debug("Creating new PathManagerActor for " + path)

            val newActor = context.actorOf(Props(new PathManagerActor(path, VFSPathUtils.versionsSubdir(pathDir), versionLog, shutdownTimeout, quiescenceTimeout, clock, self)))
            IO { pathActors += (path -> newActor); pathLRU += (path -> ()); newActor }
          }
        } yield {
          actorV valueOr {
            case Extractor.Thrown(t) => throw t
            case error => throw new Exception(error.message)
          }
        }
      }
    }

    def receive = {
      case FindChildren(path) =>
        log.debug("Received request to find children of %s".format(path.path))
        VFSPathUtils.findChildren(baseDir, path) map { children =>
          sender ! PathChildren(path, children)
        } except {
          case t: Throwable =>
            log.error("Error obtaining path children for " + path, t)
            IO { sender ! PathOpFailure(path, IOError(t)) }
        } unsafePerformIO

      case FindPathMetadata(path) =>
        log.debug("Received request to find metadata for path %s".format(path.path))
        val requestor = sender
        val eio = VFSPathUtils.currentPathMetadata(baseDir, path) map { pathMetadata =>
          requestor ! PathChildren(path, Set(pathMetadata))
        } leftMap { error =>
          requestor ! PathOpFailure(path, error)
        }

        eio.run.unsafePerformIO

      case op: PathOp =>
        val requestor = sender
        val io = targetActor(op.path) map { _.tell(op, requestor) } except {
          case t: Throwable =>
            log.error("Error obtaining path actor for " + op.path, t)
            IO { requestor ! PathOpFailure(op.path, IOError(t)) }
        }

        io.unsafePerformIO

      case IngestData(messages) =>
        log.debug("Received %d messages for ingest".format(messages.size))
        val requestor = sender
        val groupedAndPermissioned = messages.groupBy({ case (_, event) => event.path }).toStream traverse {
          case (path, pathMessages) =>
            targetActor(path) map { pathActor =>
              pathMessages.map(_._2.apiKey).distinct.toStream traverse { apiKey =>
                permissionsFinder.writePermissions(apiKey, path, clock.instant()) map { apiKey -> _ }
              } map { perms =>
                val allPerms: Map[APIKey, Set[WritePermission]] = perms.map(Map(_)).suml
                val (totalArchives, totalEvents, totalStoreFiles) = pathMessages.foldLeft((0, 0, 0)) {
                  case ((archived, events, storeFiles), (_, IngestMessage(_, _, _, data, _, _, _))) => (archived, events + data.size, storeFiles)
                  case ((archived, events, storeFiles), (_, am: ArchiveMessage)) => (archived + 1, events, storeFiles)
                  case ((archived, events, storeFiles), (_, sf: StoreFileMessage)) => (archived, events, storeFiles + 1)
                }
                log.debug("Sending %d archives, %d storeFiles, and %d events to %s".format(totalArchives, totalStoreFiles, totalEvents, path))
                pathActor.tell(IngestBundle(pathMessages, allPerms), requestor)
              }
            } except {
              case t: Throwable =>
                IO(Future(log.error("Failure during version log open on " + path, t)))
            }
        }

        groupedAndPermissioned.unsafePerformIO
    }
  }

  /**
    * An actor that manages resources under a given path. The baseDir is the version
    * subdir for the path.
    */
  final class PathManagerActor(path: Path, baseDir: File, versionLog: VersionLog, shutdownTimeout: Duration, quiescenceTimeout: Duration, clock: Clock, routingActor: ActorRef) extends Actor with Logging {
    context.setReceiveTimeout(quiescenceTimeout)

    private[this] implicit val futureM = scalaFuture.futureInstance(context.dispatcher)

    // Keeps track of the resources for a given version/authority pair
    // TODO: make this an LRU cache
    private[this] var versions = Map[UUID, Resource]()

    override def postStop = {
      val closeAll = versions.values.toStream traverse {
        case NIHDBResource(db) => db.close(context.system)
        case _ => Future.successful(())
      }

      Await.result(closeAll, shutdownTimeout)
      versionLog.close
      log.info("Shutdown of path actor %s complete".format(path))
    }

    private def versionDir(version: UUID) = new File(baseDir, version.toString)

    private def canCreate(path: Path, permissions: Set[WritePermission], authorities: Authorities): Boolean = {
      log.trace("Checking write permission for " + path + " as " + authorities + " among " + permissions)
      PermissionsFinder.canWriteAs(permissions filter { _.path.isEqualOrParentOf(path) }, authorities)
    }

    private def promoteVersion(version: UUID): IO[Unit] = {
      // we only promote if the requested version is in progress
      if (versionLog.isCompleted(version)) {
        IO(())
      } else {
        versionLog.completeVersion(version)
      }
    }

    private def openResource(version: UUID): EitherT[IO, ResourceError, Resource] = {
      versions.get(version) map { r =>
        log.debug("Located existing resource for " + version)
        right[IO, ResourceError, Resource](IO(r))
      } getOrElse {
        log.debug("Opening new resource for " + version)
        versionLog.find(version) map {
          case VersionEntry(v, _, _) =>
            val dir = versionDir(v)

            val openf: File => IO[ResourceError \/ Resource] =
              if (NIHDB.hasProjection(dir))
                (resourceBuilder.openNIHDB _).andThen(_.map(x => x: ResourceError \/ Resource))
              else
                (resourceBuilder.openBlob _).andThen(_.map(x => x: ResourceError \/ Resource))

            for {
              resource <- EitherT {
                openf(dir) flatMap { resourceV =>
                  IO(resourceV foreach { r => versions += (version -> r) }) >> IO(resourceV)
                }
              }
            } yield resource
        } getOrElse {
          left[IO, ResourceError, Resource](IO(Corrupt("No version %s found to exist for resource %s.".format(version, path.path))))
        }
      }
    }

    private def performCreate(apiKey: APIKey, data: PathData, version: UUID, writeAs: Authorities, complete: Boolean): IO[PathActionResponse] = {
      implicit val ioId = NaturalTransformation.refl[IO]
      for {
        _ <- versionLog.addVersion(VersionEntry(version, data.typeName, clock.instant()))
        created <- data match {
          case BlobData(bytes, mimeType) =>
            resourceBuilder.createBlob[Lambda[`+a` => IO[a @uncheckedVariance]]](versionDir(version), mimeType, writeAs, bytes :: StreamT.empty[IO, Array[Byte]])

          case NIHDBData(data) =>
            resourceBuilder.createNIHDB(versionDir(version), writeAs) flatMap {
              _ traverse { nihdbr =>
                nihdbr.db.insert(data) >> IO(nihdbr)
              }
            }
        }
        _ <- created traverse { resource =>
          for {
            _ <- IO { versions += (version -> resource) }
            _ <- complete.whenM(versionLog.completeVersion(version) >> versionLog.setHead(version) >> maybeExpireCache(apiKey, resource))
          } yield ()
        }
      } yield {
        created.fold(
          error => PathOpFailure(path, error),
          (_: Resource) => UpdateSuccess(path)
        )
      }
    }

    private def maybeExpireCache(apiKey: APIKey, resource: Resource): IO[Unit] = {
      resource.fold(
        blobr => IO {
          if (blobr.mimeType == FileContent.XQuirrelScript) {
            // invalidate the cache
            val cachePath = path / Path(".cached") //TODO: factor out this logic
            //FIXME: remove eventId from archive messages?
            routingActor ! ArchiveMessage(apiKey, cachePath, None, EventId.fromLong(0l), clock.instant())
          }
        },
        nihdbr => IO(())
      )
    }

    private def maybeCompleteJob(msg: EventMessage, terminal: Boolean, response: PathActionResponse) = {
      //TODO: Add job progress updates
      // TODO reenable job management
      (response == UpdateSuccess(msg.path) && terminal).option(msg.jobId).join traverse { _ => Future.successful(()) /*jobManager.finish(_, clock.now())*/ } map { _ => response }
    }

    def processEventMessages(msgs: Stream[(Long, EventMessage)], permissions: Map[APIKey, Set[WritePermission]], requestor: ActorRef): IO[Unit] = {
      log.debug("About to persist %d messages; replying to %s".format(msgs.size, requestor.toString))

      def openNIHDB(version: UUID): EitherT[IO, ResourceError, ProjectionResource] = {
        openResource(version) flatMap {
          _.fold(
            blob => left(IO(NotFound("Located resource on %s is a BLOB, not a projection" format path.path))),
            db => right(IO(db))
          )
        }
      }

      def persistNIHDB(createIfAbsent: Boolean, offset: Long, msg: IngestMessage, streamId: UUID, terminal: Boolean): IO[Unit] = {
        def batch(msg: IngestMessage) = NIHDB.Batch(offset, msg.data.map(_.value))

        if (versionLog.find(streamId).isDefined) {
          openNIHDB(streamId).fold[IO[Unit]](
            error => IO(requestor ! PathOpFailure(path, error)),
            resource => for {
              _ <- resource.append(batch(msg))
              // FIXME: completeVersion and setHead should be one op
              _ <- terminal.whenM(versionLog.completeVersion(streamId) >> versionLog.setHead(streamId))
            } yield {
              log.trace("Sent insert message for " + msg + " to nihdb")
              // FIXME: We aren't actually guaranteed success here because NIHDB might do something screwy.
              maybeCompleteJob(msg, terminal, UpdateSuccess(msg.path)).pipeTo(requestor)
            }
          ).join
        } else if (createIfAbsent) {
            log.trace("Creating new nihdb database for streamId " + streamId)
            performCreate(msg.apiKey, NIHDBData(List(batch(msg))), streamId, msg.writeAs, terminal) map { response =>
              maybeCompleteJob(msg, terminal, response) pipeTo requestor
              Unit
            }
        } else {
          //TODO: update job
          log.warn("Cannot create new database for " + streamId)
          IO(requestor ! PathOpFailure(path, IllegalWriteRequestError("Cannot create new resource. %s not applied.".format(msg.toString))))
        }
      }

      def persistFile(createIfAbsent: Boolean, offset: Long, msg: StoreFileMessage, streamId: UUID, terminal: Boolean): IO[Unit] = {
        log.debug("Persisting file on %s for offset %d".format(path, offset))
        // TODO: I think the semantics here of createIfAbsent aren't
        // quite right. If we're in a replay we don't want to return
        // errors if we're already complete
        if (createIfAbsent) {
          performCreate(msg.apiKey, BlobData(msg.content.data, msg.content.mimeType), streamId, msg.writeAs, terminal) map { response =>
            maybeCompleteJob(msg, terminal, response).pipeTo(requestor)
          }
        } else {
          //TODO: update job
          IO(requestor ! PathOpFailure(path, IllegalWriteRequestError("Cannot overwrite existing resource. %s not applied.".format(msg.toString))))
        }
      }

      msgs traverse {
        case (offset, msg @ IngestMessage(apiKey, path, _, _, _, _, streamRef)) =>
          streamRef match {
            case StreamRef.Create(streamId, terminal) =>
              log.trace("Received create for %s stream %s: current: %b, complete: %b".format(path.path, streamId, versionLog.current.isEmpty, versionLog.isCompleted(streamId)))
              persistNIHDB(versionLog.current.isEmpty && !versionLog.isCompleted(streamId), offset, msg, streamId, terminal)

            case StreamRef.Replace(streamId, terminal) =>
              log.trace("Received replace for %s stream %s: complete: %b".format(path.path, streamId, versionLog.isCompleted(streamId)))
              persistNIHDB(!versionLog.isCompleted(streamId), offset, msg, streamId, terminal)

            case StreamRef.Append =>
              log.trace("Received append for %s".format(path.path))
              val streamId = versionLog.current.map(_.id).getOrElse(UUID.randomUUID())
              for {
                _ <- persistNIHDB(canCreate(msg.path, permissions(apiKey), msg.writeAs), offset, msg, streamId, false)
                _ <- versionLog.completeVersion(streamId) >> versionLog.setHead(streamId)
              } yield ()
          }

        case (offset, msg @ StoreFileMessage(_, path, _, _, _, _, _, streamRef)) =>
          streamRef match {
            case StreamRef.Create(streamId, terminal) =>
              if (! terminal) {
                log.warn("Non-terminal BLOB for %s will not currently behave correctly!".format(path))
              }
              persistFile(versionLog.current.isEmpty && !versionLog.isCompleted(streamId), offset, msg, streamId, terminal)

            case StreamRef.Replace(streamId, terminal) =>
              if (! terminal) {
                log.warn("Non-terminal BLOB for %s will not currently behave correctly!".format(path))
              }
              persistFile(!versionLog.isCompleted(streamId), offset, msg, streamId, terminal)

            case StreamRef.Append =>
              IO(requestor ! PathOpFailure(path, IllegalWriteRequestError("Append is not yet supported for binary files."))) >> IO(Unit: Unit)
          }

        case (offset, ArchiveMessage(apiKey, path, jobId, _, timestamp)) =>
          versionLog.clearHead >> IO(requestor ! UpdateSuccess(path)) >> IO(())
      } map { _ => () }
    }

    def versionOpt(version: Version) = version match {
      case Version.Archived(id) => Some(id)
      case Version.Current => versionLog.current.map(_.id)
    }

    def receive = {
      case ReceiveTimeout =>
        log.info("Resource entering state of quiescence after receive timeout.")
        val quiesce = versions.values.toStream collect { case NIHDBResource(db) => db } traverse (_.quiesce)
        quiesce.unsafePerformIO

      case IngestBundle(messages, permissions) =>
        log.debug("Received ingest request for %d messages.".format(messages.size))
        processEventMessages(messages.toStream, permissions, sender).unsafePerformIO

      case msg @ Read(_, version) =>
        log.debug("Received Read request " + msg)

        val requestor = sender
        val io: IO[ReadResult] = version match {
          case Version.Current =>
            versionLog.current map { v =>
              openResource(v.id).fold(PathOpFailure(path, _): ReadResult, ReadSuccess(path, _): ReadResult)
            } getOrElse {
              IO(PathOpFailure(path, NotFound("No current version found for path %s".format(path.path))): ReadResult)
            }

          case Version.Archived(id) =>
            openResource(id).fold(PathOpFailure(path, _), ReadSuccess(path, _))
        }

        io.map(requestor ! _).unsafePerformIO

      case CurrentVersion(_) =>
        sender ! versionLog.current
    }
  }
}
