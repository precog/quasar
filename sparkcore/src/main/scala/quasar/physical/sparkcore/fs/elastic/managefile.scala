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

package quasar.physical.sparkcore.fs.elastic


import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.fs._,
  ManageFile._,
  MoveScenario._,
  FileSystemError._,
  PathError._
import quasar.fs.impl.ensureMoveSemantics

import pathy._, Path._
import scalaz._, Scalaz._

object managefile {

  def interpreter[S[_]](implicit
    s0: ElasticCall.Ops[S]
  ): ManageFile ~> Free[S, ?] =
    new (ManageFile ~> Free[S, ?]) {
      def apply[A](mf: ManageFile[A]): Free[S, A] = mf match {
        case Move(FileToFile(sf, df), semantics) =>
          (ensureMoveSemantics[Free[S, ?]](sf, df, pathExists[S] _, semantics).toLeft(()) *>
            moveFile(sf, df).liftM[FileSystemErrT]).run
        case Move(DirToDir(sd, dd), semantics) =>
          (ensureMoveSemantics[Free[S, ?]](sd, dd, pathExists _, semantics).toLeft(()) *>
            moveDir(sd, dd).liftM[FileSystemErrT]).run
        case Delete(path) => delete(path)
        case TempFile(near) => tempFile(near)
      }
    }

  def pathExists[S[_]](path: APath)(implicit 
    elastic: ElasticCall.Ops[S]
    ): Free[S, Boolean] = 
    refineType(path).fold(
      d => elastic.listIndices.map(_.contains(dir2Index(d))),
      f => elastic.typeExists(file2ES(f))
    )

  def moveFile[S[_]](sf: AFile, df: AFile)(implicit
    elastic: ElasticCall.Ops[S]
  ): Free[S, Unit] = for {
    src                          <- file2ES(sf).point[Free[S, ?]]
    dst                          <- file2ES(df).point[Free[S, ?]]
    dstIndexExists               <- elastic.indexExists(dst.index)
    dstTypeExists                <- elastic.typeExists(dst)
    _                            <- if(dstTypeExists) elastic.deleteType(dst)
                                    else if(!dstIndexExists) elastic.createIndex(dst.index)
                                    else ().point[Free[S, ?]]
    _                            <- elastic.copyType(src, dst)
    _                            <- elastic.deleteType(src)
  } yield ()

  private def copyIndex[S[_]](srcIndex: String, dstIndex: String)(implicit
    elastic: ElasticCall.Ops[S]
  ): Free[S, Unit] = for {
    types          <- elastic.listTypes(srcIndex)
    dstIndexExists <- elastic.indexExists(dstIndex)
    _              <- if(!dstIndexExists) elastic.createIndex(dstIndex) else ().point[Free[S, ?]]
    _              <- types.map { tn =>
      moveFile(toFile(IndexType(srcIndex, tn)), toFile(IndexType(dstIndex, tn)))
    }.sequence
  } yield ()

  def moveDir[S[_]](sd: ADir, dd: ADir)(implicit
    elastic: ElasticCall.Ops[S]
  ): Free[S, Unit] = {

    def calculateDestinationIndex(index: String): String = {
      val destinationPath = posixCodec.unsafePrintPath(dd) ++ index.diff(posixCodec.unsafePrintPath(sd))
      dirPath2Index(destinationPath)
    }

    for {
      src      <- dir2Index(sd).point[Free[S, ?]]
      toMove   <- elastic.listIndices.map(_.filter(i => i.startsWith(src)))
      _        <- toMove.map(i => copyIndex(i, calculateDestinationIndex(i))).sequence
      _        <- toMove.map(elastic.deleteIndex(_)).sequence
    } yield ()
  }

  def delete[S[_]](path: APath)(implicit
    elastic: ElasticCall.Ops[S]
  ): Free[S, FileSystemError \/ Unit] =
    refineType(path).fold(d => deleteDir(d),f => deleteFile(f))

  private def deleteFile[S[_]](file: AFile)(implicit
    elastic: ElasticCall.Ops[S]
  ): Free[S, FileSystemError \/ Unit] = for {
    indexType <- file2ES(file).point[Free[S, ?]]
    exists <- elastic.typeExists(indexType)
    res <- if(exists) elastic.deleteType(indexType).map(_.right) else pathErr(pathNotFound(file)).left[Unit].point[Free[S, ?]]
  } yield res

  private def deleteDir[S[_]](dir: ADir)(implicit
    elastic: ElasticCall.Ops[S]
  ): Free[S, FileSystemError \/ Unit] = for {
    indices <- elastic.listIndices
    index = dir2Index(dir)
    result <- if(indices.isEmpty) pathErr(pathNotFound(dir)).left[Unit].point[Free[S, ?]] else {
      indices.filter(_.startsWith(index)).map(elastic.deleteIndex(_)).sequence.as(().right[FileSystemError])
    }
  } yield result

  def tempFile[S[_]](near: APath): Free[S, FileSystemError \/ AFile] = {
    val randomFileName = s"q${scala.math.abs(scala.util.Random.nextInt(9999))}"
    val aDir: ADir = refineType(near).fold(d => d, fileParent(_))
      (aDir </> file(randomFileName)).right[FileSystemError].point[Free[S, ?]]
  }

}
