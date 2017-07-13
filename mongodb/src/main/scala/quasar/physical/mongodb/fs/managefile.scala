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

package quasar.physical.mongodb.fs

import slamdata.Predef._
import quasar.NameGenerator
import quasar.fp.ski.κ
import quasar.contrib.pathy._
import quasar.fp.TaskRef
import quasar.fs._
import quasar.physical.mongodb._

import com.mongodb.{MongoException, MongoCommandException, MongoServerException}
import com.mongodb.async.client.MongoClient
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object managefile {
  import ManageFile._, FileSystemError._, PathError._, MongoDbIO._, fsops._

  type ManageIn           = (TmpPrefix, TaskRef[Long])
  type ManageInT[F[_], A] = ReaderT[F, ManageIn, A]
  type MongoManage[A]     = ManageInT[MongoDbIO, A]

  /** TODO: There are still some questions regarding Path
    *   1) We should assume all paths will be canonicalized and can do so
    *      with a ManageFile ~> ManageFile that canonicalizes everything.
    *
    *   2) Currently, parsing a directory like "/../foo/bar/" as an absolute
    *      dir succeeds, this should probably be changed to fail.
    */

  /** Interpret `ManageFile` using MongoDB. */
  val interpret: ManageFile ~> MongoManage = new (ManageFile ~> MongoManage) {
    def apply[A](fs: ManageFile[A]) = fs match {
      case Move(scenario, semantics) =>
        scenario.fold(moveDir(_, _, semantics), moveFile(_, _, semantics))
          .run.liftM[ManageInT]

      case Delete(path) =>
        refineType(path).fold(deleteDir, deleteFile)
          .run.liftM[ManageInT]

      // TODO: For some reason, compiler is having trouble finding Functor/Apply
      //       instances within this block.
      case TempFile(path) =>
        val checkPath =
          EitherT.fromDisjunction[MongoManage](Collection.dbNameFromPath(path))
            .bimap(pathErr(_), κ(()))

        val mkTemp =
          freshName.liftM[FileSystemErrT] map { n =>
            refineType(path).fold(
              _ </> file(n),
              f => fileParent(f) </> file(n))
          }

        checkPath.flatMap(κ(mkTemp)).run
    }
  }

  /** Run [[MongoManage]] with the given `MongoClient`. */
  def run[S[_]](
    client: MongoClient
  )(implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): Task[MongoManage ~> Free[S, ?]] =
    (tmpPrefix |@| TaskRef(0L)) { (prefix, ref) =>
      new (MongoManage ~> Free[S, ?]) {
        def apply[A](fs: MongoManage[A]) =
          fs.run((prefix, ref)).runF(client)
      }
    }

  ////

  private val moveToRename: MoveSemantics => RenameSemantics = {
    case MoveSemantics.Overwrite     => RenameSemantics.Overwrite
    case MoveSemantics.FailIfExists  => RenameSemantics.FailIfExists
    case MoveSemantics.FailIfMissing => RenameSemantics.Overwrite
  }

  private def moveDir(src: ADir, dst: ADir, sem: MoveSemantics): MongoFsM[Unit] = {
    // TODO: Need our own error type instead of reusing the one from the driver.
    def filesMismatchError(srcs: Vector[AFile], dsts: Vector[AFile]): MongoException = {
      val pp = posixCodec.printPath _
      new MongoException(
        s"Mismatched files when moving '${pp(src)}' to '${pp(dst)}': srcFiles = ${srcs map pp}, dstFiles = ${dsts map pp}")
    }

    def moveAllUserCollections = for {
      colls    <- userCollectionsInDir(src)
      srcFiles =  colls map (_.asFile)
      dstFiles =  srcFiles.map(_ relativeTo (src) map (dst </> _)).unite
      _        <- srcFiles.alignBoth(dstFiles).sequence.cata(
                    _.traverse { case (s, d) => moveFile(s, d, sem) },
                    MongoDbIO.fail(filesMismatchError(srcFiles, dstFiles)).liftM[FileSystemErrT])
    } yield ()

    if (src === dst)
      ().point[MongoFsM]
    else if (depth(src) == 1)
      dbNameFromPathM(src) flatMap { dbName =>
        moveAllUserCollections *> dropDatabase(dbName).liftM[FileSystemErrT]
      }
    else
      moveAllUserCollections
  }

  private def moveFile(src: AFile, dst: AFile, sem: MoveSemantics)
                      : MongoFsM[Unit] = {

    // TODO: Is there a more structured indicator for these errors, the code
    //       appears to be '-1', which is suspect.
    val srcNotFoundErr = "source namespace does not exist"
    val dstExistsErr = "target namespace exists"

    /** Error codes obtained from MongoDB `renameCollection` docs:
      * See http://docs.mongodb.org/manual/reference/command/renameCollection/
      */
    def reifyMongoErr(m: MongoDbIO[Unit]): MongoFsM[Unit] =
      EitherT(m.attempt flatMap {
        case -\/(e: MongoServerException) if e.getCode == 10026 =>
          pathErr(pathNotFound(src)).left.point[MongoDbIO]

        case -\/(e: MongoServerException) if e.getCode == 10027 =>
          pathErr(pathExists(dst)).left.point[MongoDbIO]

        case -\/(e: MongoCommandException) if e.getErrorMessage == srcNotFoundErr =>
          pathErr(pathNotFound(src)).left.point[MongoDbIO]

        case -\/(e: MongoCommandException) if e.getErrorMessage == dstExistsErr =>
          pathErr(pathExists(dst)).left.point[MongoDbIO]

        case -\/(t) =>
          fail(t)

        case \/-(_) =>
          ().right.point[MongoDbIO]
      })

    def ensureDstExists(dstColl: Collection): MongoFsM[Unit] =
      EitherT(collectionsIn(dstColl.database)
                .filter(_ === dstColl)
                .runLast
                .map(_.toRightDisjunction(pathErr(pathNotFound(dst))).void))

    if (src === dst)
      collFromFileM(src) flatMap (srcColl =>
        collectionExists(srcColl).liftM[FileSystemErrT].ifM(
          if (MoveSemantics.failIfExists nonEmpty sem)
            MonadError[MongoFsM, FileSystemError].raiseError(pathErr(pathExists(src)))
          else
            ().point[MongoFsM]
          ,
          MonadError[MongoFsM, FileSystemError].raiseError(pathErr(pathNotFound(src)))))
    else
      for {
        srcColl <- collFromFileM(src)
        dstColl <- collFromFileM(dst)
        rSem    =  moveToRename(sem)
        _       <- if (MoveSemantics.failIfMissing nonEmpty sem)
                     ensureDstExists(dstColl)
                   else
                     ().point[MongoFsM]
        _       <- reifyMongoErr(rename(srcColl, dstColl, rSem))
      } yield ()
  }

  // TODO: Really need a Path#fold[A] method, which will be much more reliable
  //       than this process of deduction.
  private def deleteDir(dir: ADir): MongoFsM[Unit] =
    Collection.dbNameFromPath(dir).toOption match {
      case Some(n) if depth(dir) == 1 =>
        dropDatabase(n).liftM[FileSystemErrT]

      case Some(_) =>
        collectionsInDir(dir)
          .flatMap(_.traverse_(dropCollection(_).liftM[FileSystemErrT]))

      case None if depth(dir) == 0 =>
        dropAllDatabases.liftM[FileSystemErrT]

      case None =>
        nonExistentParent(dir)
    }

  private def deleteFile(file: AFile): MongoFsM[Unit] =
    collFromFileM(file) flatMap (c =>
      collectionExists(c).liftM[FileSystemErrT].ifM(
        dropCollection(c).liftM[FileSystemErrT],
        pathErr(pathNotFound(file)).raiseError[MongoFsM, Unit]))

  private def freshName: MongoManage[String] =
    for {
      in <- MonadReader[MongoManage, ManageIn].ask
      (prefix, ref) = in
      n  <- liftTask(ref.modifyS(i => (i + 1, i))).liftM[ManageInT]
    } yield prefix.run + n.toString

  private def tmpPrefix: Task[TmpPrefix] =
    NameGenerator.salt map (s => TmpPrefix(s"__quasar.tmp_${s}_"))
}
