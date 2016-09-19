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

package quasar.physical.marklogic.fs

import quasar.Predef._
import quasar.{Data, DataCodec}
import quasar.contrib.pathy._
import quasar.fp.free.lift
import quasar.fs._
import quasar.physical.marklogic.uuid._
import quasar.physical.marklogic.xcc._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.xml._
import quasar.physical.marklogic.xquery.syntax._

import scala.math.{ceil, log}

import eu.timepit.refined.auto._
import com.marklogic.xcc._
import com.marklogic.xcc.exceptions.XQueryException
import com.marklogic.xcc.types.XSBoolean
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

// NB: MarkLogic appears to possibly execute independent expressions in parallel,
//     which can cause side-effects intended to be sequenced after other operations
//     to fail, thus it seems to be better to sequence them via `SessionIO` (i.e.
//     make multiple requests) rather than to sequence them as expressions in XQuery.
object ops {
  import expr.{func, if_, for_, let_}, axes.child

  val prop           = NSPrefix(NCName("prop"))
  val propProperties = prop(NCName("properties"))
  val propDirectory  = prop(NCName("directory"))

  def appendToFile[S[_]](
    dstFile: AFile,
    data: Vector[Data]
  )(implicit
    S0: SessionIO :<: S,
    S1: GenUUID :<: S
  ): Free[S, Vector[FileSystemError]] = {
    val dstDir = asDir(dstFile)

    val createOptions = {
      val copts = new ContentCreateOptions()
      copts.setFormatJson()
      copts
    }

    // NB: Ensures all the filenames are the same length, regardless of
    //     the value of their sequence number, so that they compare properly.
    val seqFmt = {
      val width = ceil(log(data.size.toDouble) / log(16)).toInt
      if (width === 0) "" else s"%0${width}x"
    }

    val jsonData: Vector[FileSystemError \/ String] =
      data map { d =>
        DataCodec.render(d)(DataCodec.Precise)
          .leftMap(e => FileSystemError.writeFailed(d, e.message))
      }

    def mkContent(cid: String, seqNum: Int, str: String): Content = {
      val fname = cid + seqFmt.format(seqNum)
      val uri = pathUri(dstDir </> file(fname))
      ContentFactory.newContent(uri, str, createOptions)
    }

    for {
      cid  <- chunkId
      (errs, contents) = jsonData.separate
      cs   =  contents.zipWithIndex map { case (json, i) => mkContent(cid, i, json) }
      exs  <- lift(SessionIO.insertContentCollectErrors(cs)).into[S]
    } yield if (exs.isEmpty) errs else (FileSystemError.partialWrite(errs.length) +: errs)
  }

  def createFile(file: AFile): SessionIO[PathError \/ Unit] = {
    val uri = pathUri(asDir(file))

    // TODO: If dir exists and is empty, then shouldn't error
    handleXcc(SessionIO.executeQuery_(xdmp.directoryCreate(uri.xs)) as ().right[PathError]) {
      case qex: XQueryException if qex.getCode === "XDMP-DIREXISTS" => PathError.pathExists(file).left
    }
  }

  def deleteFile(file: AFile): SessionIO[Unit] = {
    val uri = pathUri(asDir(file))

    val deleteChildDocuments =
      SessionIO.executeQuery_(
        fn.map(
          func("$d") { xdmp.documentDelete(fn.baseUri("$d".xqy)) },
          xdmp.directory(uri.xs, "1".xs)))

    val deleteMLDirIfEmpty =
      SessionIO.executeQuery_(deleteIfEmptyXqy(uri.xs))

    deleteChildDocuments *> deleteMLDirIfEmpty.void
  }

  def deleteDir(dir: ADir): SessionIO[Unit] =
    SessionIO.executeQuery_(xdmp.directoryDelete(pathUri(dir).xs)).void

  def exists(path: APath): SessionIO[Boolean] = {
    val uri = pathUri(refineType(path).map(asDir).merge)

    val xqy = fn.exists(fn.filter(func("$uri") { fn.startsWith("$uri".xqy, uri.xs) }, cts.uris))

    SessionIO.resultsOf_(xqy) flatMap { items => SessionIO.liftT {
      Task.delay(items(0).asInstanceOf[XSBoolean].asPrimitiveBoolean)
    }}
  }

  def ls(dir: ADir): SessionIO[Set[PathSegment]] = {
    val uri = pathUri(dir)

    def isMLDir(u: XQuery) =
      fn.exists(xdmp.documentProperties(u) `/` child(propProperties) `/` child(propDirectory))

    val prefixPathsXqy =
      fn.filter(
        func("$u") { isMLDir("$u".xqy) and fn.startsWith("$u".xqy, uri.xs) },
        cts.uris(uri.xs, IList("properties".xs)))

    def childPathsXqy(prefixed: XQuery) =
      fn.distinctValues(fn.map(
        func("$u") {
          fn.concat(
            uri.xs,
            fn.tokenize(fn.substringAfter("$u".xqy, uri.xs), "/".xs)("1".xqy),
            "/".xs)
        },
        prefixed))

    def isDirXqy(pathUri: XQuery) = {
      val hasChildMLDirs =
        fn.exists(
          for_("$d" -> xdmp.directory(pathUri, "1".xs))
            .where_("$d".xqy("property::directory".xqy))
            .return_("$d".xqy))

      fn.not(isMLDir(pathUri)) or hasChildMLDirs
    }

    def isFileXqy(pathUri: XQuery) =
      fn.exists(
        for_("$d" -> xdmp.directory(pathUri, "1".xs))
          .where_("$d".xqy(fn.not("property::directory".xqy)))
          .return_("$d".xqy))

    def filesXqy(dirUris: XQuery) =
      fn.map(
        func("$u") { fn.substring("$u".xqy, "1".xqy, some(fn.stringLength("$u".xqy) - "1".xqy)) },
        fn.filter(func("$f") { isFileXqy("$f".xqy) }, dirUris))

    val xqy = let_(
      "$pathUris"      -> prefixPathsXqy,
      "$childPathUris" -> childPathsXqy("$pathUris".xqy),
      "$dirUris"       -> fn.filter(func("$u") { isDirXqy("$u".xqy) }, "$childPathUris".xqy),
      "$fileUris"      -> filesXqy("$childPathUris".xqy)
    ) return_ (
      mkSeq_("$dirUris".xqy, "$fileUris".xqy)
    )

    def parseDir(s: String): Option[PathSegment] =
      posixCodec.parseAbsDir(s) flatMap dirName map (_.left)

    def parseFile(s: String): Option[PathSegment] =
      posixCodec.parseAbsFile(s) map (f => fileName(f).right)

    SessionIO.resultsOf_(xqy) map (_ foldMap { item =>
      val itemStr = item.asString
      parseDir(itemStr).orElse(parseFile(itemStr)).toSet
    })
  }

  def moveFile(src: AFile, dst: AFile): SessionIO[Unit] = {
    val srcUri = pathUri(asDir(src))
    val dstUri = pathUri(asDir(dst))

    def doMove =
      SessionIO.executeQuery_(mkSeq_(
        if_(fn.exists(xdmp.documentProperties(dstUri.xs)("/prop:properties/prop:directory".xqy)))
          .then_ {
            for_("$d" -> xdmp.directory(dstUri.xs, "1".xs))
              .return_(xdmp.documentDelete(xdmp.nodeUri("$d".xqy)))
          } else_ {
            xdmp.directoryCreate(dstUri.xs)
          },

        for_("$d" -> xdmp.directory(srcUri.xs, "1".xs))
          .let_(
            "$oldName" -> xdmp.nodeUri("$d".xqy),
            "$newName" -> fn.concat(dstUri.xs, fn.tokenize("$oldName".xqy, "/".xs)(fn.last)))
          .return_(mkSeq_(
            xdmp.documentInsert("$newName".xqy, fn.doc("$oldName".xqy)),
            xdmp.documentDelete("$oldName".xqy)))))

    def deleteSrcIfEmpty =
      SessionIO.executeQuery_(deleteIfEmptyXqy(srcUri.xs))

    if (src === dst) ().point[SessionIO] else doMove *> deleteSrcIfEmpty.void
  }

  def readFile(file: AFile): Process[ContentSourceIO, Data] = {
    val uri = pathUri(asDir(file))

    val xqy = cts.search(
      fn.doc(),
      cts.directoryQuery(uri.xs),
      IList(cts.indexOrder(cts.uriReference, "ascending".xs)))

    ContentSourceIO.resultStream(SessionIO.evaluateQuery_(xqy))
      .map(xdmitem.toData)
  }

  ////

  private def asDir(file: AFile): ADir =
    fileParent(file) </> dir(fileName(file).value)

  private def chunkId[S[_]](implicit S: GenUUID :<: S): Free[S, String] =
    GenUUID.Ops[S].asks(id => toSequentialString(id) getOrElse toOpaqueString(id))

  private def pathUri(path: APath): String =
    posixCodec.printPath(path)

  private def deleteIfEmptyXqy(dirUri: XQuery): XQuery =
    if_ (fn.not(fn.exists(xdmp.directory(dirUri, "infinity".xs))))
      .then_ { xdmp.directoryDelete(dirUri) }
      .else_ { expr.emptySeq }
}
