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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar.concurrent.Pools._
import quasar.contrib.scalaz.concurrent._
import quasar.effect.Failure
import quasar.fp._
import quasar.fp.ski._
import quasar.fs._

import java.lang.{Boolean => JBoolean}
import java.util.LinkedList
import scala.Predef.classOf
import scala.collection.JavaConverters._

import com.mongodb._
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.client.model._
import com.mongodb.async._
import com.mongodb.async.client._
import org.bson.{BsonBoolean, BsonDocument, BSONException, Document}
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.{Strategy, Task}
import scalaz.stream._

final class MongoDbIO[A] private (protected val r: ReaderT[Task, MongoClient, A]) {
  def map[B](f: A => B): MongoDbIO[B] =
    new MongoDbIO(r map f)

  def flatMap[B](f: A => MongoDbIO[B]): MongoDbIO[B] =
    new MongoDbIO(r flatMap (a => f(a).r))

  def attempt: MongoDbIO[Throwable \/ A] =
    new MongoDbIO(r mapK (_.attempt))

  def attemptMongo: MongoErrT[MongoDbIO, A] =
    EitherT(attempt >>= {
      case -\/(me: MongoException) => unhandledFSError(me).left.point[MongoDbIO]
      case -\/(be: BSONException)  => unhandledFSError(be).left.point[MongoDbIO]
      case -\/(t)                  => MongoDbIO.fail(t)
      case \/-(a)                  => a.right.point[MongoDbIO]
    })

  def run(c: MongoClient): Task[A] =
    r.run(c)

  def runF[S[_]](
    c: MongoClient
  )(implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): Free[S, A] = {
    val mongoErr = Failure.Ops[PhysicalError, S]
    mongoErr.unattempt(free.lift(attemptMongo.run.run(c)).into[S])
  }
}

object MongoDbIO {

  /** Returns the stream of results of aggregating documents according to the
    * given aggregation pipeline.
    */
  def aggregated(
    src: Collection,
    pipeline: List[Bson.Doc],
    allowDiskUse: Boolean
  ): Process[MongoDbIO, BsonDocument] =
    aggregateIterable(src, pipeline, allowDiskUse)
      .liftM[Process]
      .flatMap(iterableToProcess)

  /** Aggregates documents according to the given aggregation pipeline, which
    * must end with an `\$out` stage specifying the collection where results
    * may be found.
    */
  def aggregate(
    src: Collection,
    pipeline: List[Bson.Doc],
    allowDiskUse: Boolean
  ): MongoDbIO[Unit] =
    aggregateIterable(src, pipeline, allowDiskUse)
      .flatMap(c => async[java.lang.Void](c.toCollection(_)))
      .void

  def collectionExists(c: Collection): MongoDbIO[Boolean] =
    collectionsIn(c.database)
      .exists(_.collection == c.collection)
      .runLastOr(false)

  /** All discoverable collections on the server. */
  def collections: Process[MongoDbIO, Collection] =
    databaseNames flatMap collectionsIn

  /** The collections in the named database. */
  def collectionsIn(dbName: DatabaseName): Process[MongoDbIO, Collection] =
    database(dbName).liftM[Process]
      .flatMap(db => iterableToProcess(db.listCollectionNames).map(CollectionName(_)))
      .map(Collection(dbName, _))

  /** Creates the given collection. */
  def createCollection(c: Collection): MongoDbIO[Unit] =
    database(c.database) flatMap (db =>
      async[java.lang.Void](db.createCollection(c.collection.value, _)).void)

  /** Names of all discoverable databases on the server. */
  def databaseNames: Process[MongoDbIO, DatabaseName] =
    client.liftM[Process]
      .flatMap(c => iterableToProcess(c.listDatabaseNames).map(DatabaseName(_)))
      .onFailure {
        case t: MongoCommandException =>
          credential.liftM[Process]
            .flatMap(y => Process.emit(DatabaseName(y.getSource)))

        case t =>
          Process.fail(t)
      }

  def dropCollection(c: Collection): MongoDbIO[Unit] =
    collection(c) flatMap (mc => async(mc.drop).void)


  /** Ensure the given collection exists, creating it if not. */
  def ensureCollection(c: Collection): MongoDbIO[Unit] =
    collectionExists(c).ifM(().point[MongoDbIO], createCollection(c))

  /** Returns the first document in the collection. */
  def first(coll: Collection): OptionT[MongoDbIO, BsonDocument] =
    OptionT(find(coll) >>= (c => async(c.limit(1).first) map (Option(_))))

  /** Inserts the given documents into the collection. */
  def insert[F[_]: Foldable](coll: Collection, docs: F[BsonDocument]): MongoDbIO[Unit] = {
    val docList = new LinkedList[BsonDocument]
    val insertOpts = (new InsertManyOptions()).ordered(false)

    Foldable[F].traverse_(docs)(d => docList.add(d): Id[Boolean])

    if (docList.isEmpty)
      ().point[MongoDbIO]
    else
      collection(coll)
        .flatMap(c => async[java.lang.Void](c.insertMany(docList, insertOpts, _)))
        .void
  }

  /** Attempts to insert as many of the given documents into the collection as
    * possible. The number of documents inserted is returned, if possible, and
    * may be smaller than the original amount if any documents failed to insert.
    */
  def insertAny[F[_]: Foldable](coll: Collection, docs: F[BsonDocument]): OptionT[MongoDbIO, Int] = {
    val docList = new LinkedList[WriteModel[BsonDocument]]
    val writeOpts = (new BulkWriteOptions()).ordered(false)

    Foldable[F].traverse_(docs)(d => docList.add(new InsertOneModel(d)): Id[Boolean])

    if (docList.isEmpty)
      OptionT.none
    else
      OptionT(collection(coll)
        .flatMap(c => async[BulkWriteResult](c.bulkWrite(docList, writeOpts, _)))
        .map(r => r.wasAcknowledged option r.getInsertedCount))
  }

  /** Executes the map-reduce job described by `cfg`, sourcing documents from
    * `src` and writing the output to `dst`.
    */
  def mapReduce(
    src: Collection,
    dst: MapReduce.OutputCollection,
    cfg: MapReduce
  ): MongoDbIO[Unit] = {
    import MapReduce._, Action._

    type F[A] = State[MapReduceIterable[BsonDocument], A]
    val ms = MonadState[F, MapReduceIterable[BsonDocument]]

    def withAction(actOut: ActionedOutput): F[Unit] =
      actOut.database.traverse_[F](n => ms.modify(_.databaseName(n.value)))     *>
      actOut.shardOutputCollection.traverse_[F](s => ms.modify(_.sharded(s))) *>
      actOut.action.nonAtomic.traverse_[F](s => ms.modify(_.nonAtomic(s)))    *>
      ms.modify(_.action(actOut.action match {
        case Replace   => MapReduceAction.REPLACE
        case Merge(_)  => MapReduceAction.MERGE
        case Reduce(_) => MapReduceAction.REDUCE
      }))

    mapReduceIterable(src, cfg) flatMap { it =>
      val itWithOutput =
        dst.withAction.traverse_(withAction)
          .exec(it.collectionName(dst.collection.value))

      async[java.lang.Void](itWithOutput.toCollection).void
    }
  }

  /** Rename `src` to `dst` using the given semantics. */
  def rename(src: Collection, dst: Collection, semantics: RenameSemantics): MongoDbIO[Unit] = {
    import RenameSemantics._

    val dropDst = semantics match {
      case Overwrite    => true
      case FailIfExists => false
    }

    if (src === dst)
      ().point[MongoDbIO]
    else
      collection(src)
        .flatMap(c => async[java.lang.Void](c.renameCollection(
          dst.asNamespace,
          (new RenameCollectionOptions).dropTarget(dropDst),
          _)))
        .void
  }

  /** Returns the version of the MongoDB server the client is connected to. */
  def serverVersion: MongoDbIO[ServerVersion] = {
    def lookupVersion(dbName: DatabaseName): MongoDbIO[String \/ ServerVersion] = {
      val cmd = Bson.Doc(ListMap("buildinfo" -> Bson.Int32(1)))

      runCommand(dbName, cmd).attemptMongo.run.map(
        _.leftMap(_.cause.getMessage)
          .flatMap(doc =>
            Option(doc getString "version")
              .toRightDisjunction("Unable to determine server version, buildInfo response is missing the 'version' field")
              .flatMap(v => ServerVersion.fromString(v.getValue))))
    }

    val finalize: ((Vector[String], Vector[ServerVersion])) => MongoDbIO[ServerVersion] = {
      case (errs, vers) =>
        vers.headOption.map(_.point[MongoDbIO]) orElse
        // FIXME: Shouldn’t be creating fresh MongoExceptions
        errs.headOption.map(msg => fail[ServerVersion](new MongoException(msg))) getOrElse
        fail(new MongoException("No database found."))
    }

    // NB: use "admin" DB as fallback if no database is known to exist.
    (databaseNames ++ Process.emit(DatabaseName("admin")))
      .evalMap(lookupVersion)
      .takeThrough(_.isLeft)
      .runLog
      .map(_.toVector.separate)
      .flatMap(finalize)
  }

  def collectionStatistics(coll: Collection): MongoDbIO[CollectionStatistics] = {
    val cmd = Bson.Doc(ListMap("collStats" -> coll.collection.bson))

    def longValue(doc: BsonDocument, field: String): String \/ Long =
      \/.fromTryCatchNonFatal(Option(doc.getNumber(field)).map(_.longValue) \/>
        s"expected field: $field").fold(_.getMessage.left, ι)

    def booleanValue(doc: BsonDocument, field: String): Boolean =
      doc.get(field, BsonBoolean.FALSE) != BsonBoolean.FALSE

    runCommand(coll.database, cmd).map(doc =>
      (for {
        count    <- longValue(doc, "count")
        dataSize <- longValue(doc, "size")
        sharded  =  booleanValue(doc, "sharded")
      } yield CollectionStatistics(count, dataSize, sharded)))
        .flatMap(_.fold(
          err => fail(new MongoException("could not read collection statistics: " + err)),
          _.point[MongoDbIO]))
  }

  private def collect[A](iter: MongoIterable[A]): MongoDbIO[List[A]] = {
    import scala.collection.JavaConverters._
    async[java.util.ArrayList[A]](cb => iter.into[java.util.ArrayList[A]](new java.util.ArrayList[A], cb)).map(_.asScala.toList)
  }

  /** Set of indexes on a collection, including only simple index types and
    * ignoring the rest.
    */
  def indexes(coll: Collection): MongoDbIO[Set[Index]] = {
    // TODO: split on ".", but note that MongoDB seems to treat such keys
    // special anyway, at least when arrays are present.
    def decodeField(s: String): BsonField = BsonField.Name(s)

    val decodeType: PartialFunction[java.lang.Object, IndexType] = {
      case x: java.lang.Number if x.intValue ≟ 1  => IndexType.Ascending
      case x: java.lang.Number if x.intValue ≟ -1 => IndexType.Descending
      case "hashed"                               => IndexType.Hashed
    }

    def decodeIndex(doc: Document): Option[Index] =
      (Option(doc.get("name")).flatMap {
        case s: String => s.some
        case _ => None
      } ⊛
        Option(doc.get("key")).flatMap {
          case kd: Document =>
            kd.asScala.toList.toNel.flatMap(_.traverse {
              case (k, v) => decodeType.lift(v).strengthL(decodeField(k))
            })
          case _ => None
        })(Index(
          _,
          _,
          Option(doc.get("unique")).fold(
            false) {
            case java.lang.Boolean.TRUE => true
            case _                      => false
          }))

    collection(coll)
      .flatMap(c => collect[Document](c.listIndexes))
      .map(_.flatMap(decodeIndex(_).toList).toSet)
  }

  def fail[A](t: Throwable): MongoDbIO[A] =
    liftTask(Task.fail(t))

  def runNT(client: MongoClient) = λ[MongoDbIO ~> Task](_ run client)

  val liftTask = λ[Task ~> MongoDbIO](t => lift(_ => t))

  /** Returns the underlying, configured aggregate iterable for applying the
    * given pipeline to the source collection.
    */
  private[mongodb] def aggregateIterable(
    src: Collection,
    pipeline: List[Bson.Doc],
    allowDiskUse: Boolean
  ): MongoDbIO[AggregateIterable[BsonDocument]] =
    collection(src) map (c =>
      c.aggregate(pipeline.asJava)
        .allowDiskUse(new JBoolean(allowDiskUse)))

  private[mongodb] def mapReduceIterable(
    src: Collection,
    cfg: MapReduce
  ): MongoDbIO[MapReduceIterable[BsonDocument]] = {
    type IT       = MapReduceIterable[BsonDocument]
    type CfgIt[A] = State[IT, A]
    val ms = MonadState[CfgIt, IT]

    def foldIt[A](a: Option[A])(f: (IT, A) => IT): CfgIt[Unit] =
      ms.modify(a.foldLeft(_)(f))

    val nonEmptyScope =
      cfg.scope.nonEmpty option cfg.scope

    val sortRepr =
      cfg.inputSort map (ts =>
        Bson.Doc(ListMap(
          ts.list.toList.map(_.bimap(_.asText, sortDirToBson(_))): _*
        )).repr)

    val configuredIt =
      foldIt(cfg.selection)((i, s) => i.filter(s.bson.repr))           *>
      foldIt(sortRepr)(_ sort _)                                       *>
      foldIt(cfg.limit)(_ limit _.toInt)                               *>
      foldIt(cfg.finalizer)((i, f) => i.finalizeFunction(f.pprint(0))) *>
      foldIt(nonEmptyScope)((i, s) => i.scope(Bson.Doc(s).repr))       *>
      foldIt(cfg.jsMode)(_ jsMode _)                                   *>
      foldIt(cfg.verbose)(_ verbose _)

    collection(src) map { c =>
      configuredIt exec c.mapReduce(cfg.map.pprint(0), cfg.reduce.pprint(0))
    }
  }

  private[mongodb] def find(c: Collection): MongoDbIO[FindIterable[BsonDocument]] =
    collection(c) map (_.find)

  private[mongodb] def async[A](f: SingleResultCallback[A] => Unit): MongoDbIO[A] = {
    val back = for {
      a <- Task.async[A](cb => f(new DisjunctionCallback(cb)))
      _ <- shift
    } yield a

    liftTask(back)
  }

  implicit val mongoDbInstance: Monad[MongoDbIO] with Catchable[MongoDbIO] =
    new Monad[MongoDbIO] with Catchable[MongoDbIO] {
      override def map[A, B](fa: MongoDbIO[A])(f: A => B) = fa map f
      def point[A](a: => A) = new MongoDbIO(Kleisli(_ => Task.now(a)))
      def bind[A, B](fa: MongoDbIO[A])(f: A => MongoDbIO[B]) = fa flatMap f
      def fail[A](t: Throwable) = MongoDbIO.fail(t)
      def attempt[A](fa: MongoDbIO[A]) = fa.attempt
    }

  ////

  private def apply[A](f: MongoClient => A): MongoDbIO[A] =
    lift(c => Task.delay(f(c)))

  private def lift[A](f: MongoClient => Task[A]): MongoDbIO[A] =
    new MongoDbIO(Kleisli(f))

  private def client: MongoDbIO[MongoClient] =
    MongoDbIO(ι)

  // TODO: Make a basic credential type in scala and expose this method.
  private val credential: MongoDbIO[MongoCredential] =
    MongoDbIO(_.getSettings.getCredential)

  private[mongodb] def collection(c: Collection): MongoDbIO[MongoCollection[BsonDocument]] =
    database(c.database).map(_.getCollection(c.collection.value, classOf[BsonDocument]))

  private def database(named: DatabaseName): MongoDbIO[MongoDatabase] =
    MongoDbIO(_ getDatabase named.value)

  private def runCommand(dbName: DatabaseName, cmd: Bson.Doc): MongoDbIO[BsonDocument] =
    database(dbName) flatMap (db => async[BsonDocument](db.runCommand(cmd, classOf[BsonDocument], _)))

  private def iterableToProcess[A](it: MongoIterable[A]): Process[MongoDbIO, A] = {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def go(c: AsyncBatchCursor[A]): Process[MongoDbIO, A] =
      Process.eval(async(c.next))
        .flatMap(r => Option(r).cata(
          as => Process.emitAll(as.asScala.toVector) ++ go(c),
          Process.halt))

    Process.eval(async(it.batchCursor)) flatMap (cur =>
      go(cur) onComplete Process.eval_(MongoDbIO(_ => cur.close())))
  }

  private final class DisjunctionCallback[A](f: Throwable \/ A => Unit)(implicit S: Strategy)
    extends SingleResultCallback[A] {

    def onResult(result: A, error: Throwable): Unit =
      f(Option(error) <\/ result)
  }
}
