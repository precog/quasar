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

package quasar.fs.mount.cache

import slamdata.Predef._
import quasar.{Data, Variables}
import quasar.contrib.pathy._
import quasar.effect.{Failure, KeyValueStoreSpec, Write}
import quasar.fp._, free._
import quasar.fs.{FileSystem, FileSystemFailure, FileSystemError, InMemory, ManageFile}
import quasar.fs.InMemory.InMemState
import quasar.fs.mount.MountConfig
import quasar.fs.mount.cache.VCache.VCacheExpW
import quasar.fs.mount.cache.ViewCacheArbitrary._
import quasar.metastore._
import quasar.sql._

import java.time.Instant

import doobie.imports._
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

abstract class VCacheSpec extends KeyValueStoreSpec[AFile, ViewCache] with MetaStoreFixture {
  import VCache.VCacheKVS

  sequential

  type Eff[A] = (ManageFile :\: FileSystemFailure :\: ConnectionIO :/: VCacheExpW)#M[A]

  def interp(files: List[AFile]): Task[(Eff ~> ConnectionIO, Task[InMemState])] =
    (
      InMemory.runInspect(InMemState.fromFiles(files.strengthR(Vector[Data]()).toMap)) ⊛
      TaskRef(Tags.Min(Option.empty[VCache.Expiration]))
    )((inMem, r) =>
      inMem.leftMap(inMemFS =>
        (taskToConnectionIO compose inMemFS compose
          InMemory.fileSystem compose injectNT[ManageFile, FileSystem])            :+:
        (taskToConnectionIO compose
          Failure.showRuntimeError[Task, FileSystemError])                         :+:
        reflNT[ConnectionIO]                                                       :+:
        (taskToConnectionIO compose Write.fromTaskRef(r))))

  def eval[A](program: Free[VCacheKVS, A]): A = evalWithFiles(program, Nil)._1

  def evalWithFiles[A](program: Free[VCacheKVS, A], files: List[AFile]): (A, Task[InMemState]) =
    (interp(files) >>= { case (i, s) =>
      program.foldMap(foldMapNT(i) compose VCache.interp[Eff]).transact(transactor).strengthR(s)
    }).unsafePerformSync

  val vcache = VCacheKVS.Ops[VCacheKVS]

  "Put deletes existing cache files" >> {
    val f = rootDir </> file("f")
    val dataFile = rootDir </> file("dataFile")
    val tmpDataFile = rootDir </> file("tmpDataFile")
    val expr = sqlB"α"
    val viewCache = ViewCache(
      MountConfig.ViewConfig(expr, Variables.empty), None, None, 0, None, None,
      600L, Instant.ofEpochSecond(0), ViewCache.Status.Pending, None, dataFile, tmpDataFile.some)

    evalWithFiles(
      vcache.put(f, viewCache) >> vcache.put(f, viewCache),
      List(dataFile, tmpDataFile)
    )._2.unsafePerformSync.contents must_= Map.empty
  }

  "CompareAndPut deletes cache files when expect matches and files don't match" >> {
    val f = rootDir </> file("f")
    val dataFile = rootDir </> file("dataFile")
    val tmpDataFile = rootDir </> file("tmpDataFile")
    val expr = sqlB"α"
    val viewCache = ViewCache(
      MountConfig.ViewConfig(expr, Variables.empty), None, None, 0, None, None,
      600L, Instant.ofEpochSecond(0), ViewCache.Status.Pending, None, dataFile, tmpDataFile.some)

    evalWithFiles(
      vcache.put(f, viewCache) >>
      vcache.compareAndPut(
        f, viewCache.some,
        viewCache.copy(
          dataFile = rootDir </> file("otherDataFile"),
          tmpDataFile = (rootDir </> file("otherTmpDataFile")).some)),
      List(dataFile, tmpDataFile)
    )._2.unsafePerformSync.contents must_= Map.empty
  }

  "CompareAndPut preserves cache files when expect matches and files match" >> {
    val f = rootDir </> file("f")
    val dataFile = rootDir </> file("dataFile")
    val tmpDataFile = rootDir </> file("tmpDataFile")
    val expr = sqlB"α"
    val viewCache = ViewCache(
      MountConfig.ViewConfig(expr, Variables.empty), None, None, 0, None, None,
      600L, Instant.ofEpochSecond(0), ViewCache.Status.Pending, None, dataFile, tmpDataFile.some)

    evalWithFiles(
      vcache.put(f, viewCache) >>
      vcache.compareAndPut(f, viewCache.some, viewCache),
      List(dataFile, tmpDataFile)
    )._2.unsafePerformSync.contents.keys.toList must_= List(dataFile, tmpDataFile)
  }

  "CompareAndPut preserves cache files when expect doesn't match" >> {
    val f = rootDir </> file("f")
    val dataFile = rootDir </> file("dataFile")
    val tmpDataFile = rootDir </> file("tmpDataFile")
    val expr = sqlB"α"
    val viewCache = ViewCache(
      MountConfig.ViewConfig(expr, Variables.empty), None, None, 0, None, None,
      600L, Instant.ofEpochSecond(0), ViewCache.Status.Pending, None, dataFile, tmpDataFile.some)

    evalWithFiles(
      vcache.compareAndPut(f, viewCache.some, viewCache),
      List(dataFile, tmpDataFile)
    )._2.unsafePerformSync.contents.keys.toList must_= List(dataFile, tmpDataFile)
  }

  "Delete deletes cache files" >> {
    val f = rootDir </> file("f")
    val dataFile: AFile = rootDir </> file("dataFile")
    val tmpDataFile = rootDir </> file("tmpDataFile")
    val expr = sqlB"α"
    val viewCache = ViewCache(
      MountConfig.ViewConfig(expr, Variables.empty), None, None, 0, None, None,
      600L, Instant.ofEpochSecond(0), ViewCache.Status.Pending, None, dataFile, tmpDataFile.some)

    evalWithFiles(
      vcache.put(f, viewCache) >> vcache.delete(f),
      List(dataFile, tmpDataFile)
    )._2.unsafePerformSync.contents must_= Map.empty
  }
}

class VCacheH2Spec extends VCacheSpec with H2MetaStoreFixture {
  val schema: quasar.db.Schema[Int] = quasar.metastore.Schema.schema
}
