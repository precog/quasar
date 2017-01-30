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
import quasar.Data
import quasar.contrib.pathy._
import quasar.effect.Capture
import quasar.effect.uuid._
import quasar.fp.numeric.{Natural, Positive}
import quasar.fs._
import quasar.physical.marklogic.ErrorMessages
import quasar.physical.marklogic.xcc._
import quasar.physical.marklogic.xml._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._

import scala.math.{ceil, log}

import eu.timepit.refined.auto._
import com.marklogic.xcc._
import com.marklogic.xcc.types.XSBoolean
import pathy.Path._
import scalaz._, Scalaz._

// NB: MarkLogic appears to possibly execute independent expressions in parallel,
//     which can cause side-effects intended to be sequenced after other operations
//     to fail, thus it seems to be better to sequence them monadically (i.e.
//     make multiple requests) rather than to sequence them as expressions in XQuery.
object ops {
  import expr.{func, if_, for_, let_}, axes.child

  val prop           = NSPrefix(NCName("prop"))
  val propProperties = prop(NCName("properties"))
  val propDirectory  = prop(NCName("directory"))

  def appendToFile[F[_]: Monad: Capture: SessionReader: UuidReader: XccErr, FMT](
    dstFile: AFile,
    data: Vector[Data]
  )(implicit
    C: AsContent[FMT, Data]
  ): F[Vector[FileSystemError]] = {
    val dstDir = asDir(dstFile)

    // NB: Ensures all the filenames are the same length, regardless of
    //     the value of their sequence number, so that they compare properly.
    val seqFmt = {
      val width = ceil(log(data.size.toDouble) / log(16)).toInt
      if (width === 0) "" else s"%0${width}x"
    }

    def mkContent(cid: String, seqNum: Int, d: Data): FileSystemError \/ Content = {
      val fname      = cid + seqFmt.format(seqNum)
      val uri        = pathUri(dstDir </> file(fname))
      val contentUri = ContentUri.getOption(uri) \/> s"Malformed content URI: $uri".wrapNel

      (contentUri >>= (C.asContent[ErrorMessages \/ ?](_, d)))
        .leftMap(msgs => FileSystemError.writeFailed(d, msgs intercalate ", "))
    }

    for {
      res <- chunkId[F] ∘ (cid => data.zipWithIndex map { case (d, i) => mkContent(cid, i, d) })
      (errs, contents) = res.separate
      exs <- session.insertContentCollectErrors[F, Vector](contents)
    } yield if (exs.isEmpty) errs else (FileSystemError.partialWrite(exs.length) +: errs)
  }

  def createFile[F[_]: Monad: Capture: SessionReader](file: AFile)(implicit X: XccErr[F]): F[PathError \/ Unit] = {
    val uri = pathUri(asDir(file))

    // TODO: If dir exists and is empty, then shouldn't error
    X.handleError(session.executeQuery_[F](xdmp.directoryCreate(uri.xs)) as ().right[PathError]) {
      case XccError.XQueryError(_, c) if c.getCode === "XDMP-DIREXISTS" => PathError.pathExists(file).left.point[F]
      case other                                                        => X.raiseError(other)
    }
  }

  def deleteFile[F[_]: Monad: Capture: SessionReader: XccErr](file: AFile): F[Unit] = {
    val uri = pathUri(asDir(file))

    val deleteChildDocuments =
      session.executeQuery_[F](
        fn.map(
          func("$d") { xdmp.documentDelete(fn.baseUri("$d".xqy)) },
          xdmp.directory(uri.xs, "1".xs)))

    val deleteMLDirIfEmpty =
      session.executeQuery_[F](deleteIfEmptyXqy(uri.xs))

    deleteChildDocuments *> deleteMLDirIfEmpty.void
  }

  def deleteDir[F[_]: Monad: Capture: SessionReader: XccErr](dir: ADir): F[Unit] =
    session.executeQuery_[F](xdmp.directoryDelete(pathUri(dir).xs)).void

  def exists[F[_]: Monad: SessionReader: XccErr](path: APath)(implicit C: Capture[F]): F[Boolean] = {
    val uri = pathUri(refineType(path).map(asDir).merge)
    val xqy = fn.exists(fn.filter(func("$uri") { fn.startsWith("$uri".xqy, uri.xs) }, cts.uris))

    session.resultsOf_[F](xqy) >>= (items => C.delay(items(0).asInstanceOf[XSBoolean].asPrimitiveBoolean))
  }

  def ls[F[_]: Monad: Capture: SessionReader: XccErr](dir: ADir): F[Set[PathSegment]] = {
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
      val d = $("d")

      val hasChildMLDirs =
        fn.exists(
          for_(d in xdmp.directory(pathUri, "1".xs))
          .where_((~d)("property::directory".xqy))
          .return_(~d))

      fn.not(isMLDir(pathUri)) or hasChildMLDirs
    }

    def isFileXqy(pathUri: XQuery) = {
      val d = $("d")

      fn.exists(
        for_(d in xdmp.directory(pathUri, "1".xs))
        .where_((~d)(fn.not("property::directory".xqy)))
        .return_(~d))
    }

    def filesXqy(dirUris: XQuery) =
      fn.map(
        func("$u") { fn.substring("$u".xqy, "1".xqy, some(fn.stringLength("$u".xqy) - "1".xqy)) },
        fn.filter(func("$f") { isFileXqy("$f".xqy) }, dirUris))

    val (pathUris, childPathUris, dirUris, fileUris) = ($("pathUris"), $("childPathUris"), $("dirUris"), $("fileUris"))

    val xqy = let_(
      pathUris      := prefixPathsXqy,
      childPathUris := childPathsXqy(~pathUris),
      dirUris       := fn.filter(func("$u") { isDirXqy("$u".xqy) }, ~childPathUris),
      fileUris      := filesXqy(~childPathUris)
    ) return_ (
      mkSeq_(~dirUris, ~fileUris)
    )

    def parseDir(s: String): Option[PathSegment] =
      posixCodec.parseAbsDir(s) flatMap dirName map (_.left)

    def parseFile(s: String): Option[PathSegment] =
      posixCodec.parseAbsFile(s) map (f => fileName(f).right)

    session.resultsOf_[F](xqy) map (_ foldMap { item =>
      val itemStr = item.asString
      parseDir(itemStr).orElse(parseFile(itemStr)).toSet
    })
  }

  def moveFile[F[_]: Monad: Capture: SessionReader: XccErr](src: AFile, dst: AFile): F[Unit] = {
    val srcUri = pathUri(asDir(src))
    val dstUri = pathUri(asDir(dst))

    def doMove = {
      val (d, oldName, newName) = ($("d"), $("oldName"), $("newName"))

      session.executeQuery_[F](mkSeq_(
        if_(fn.exists(xdmp.documentProperties(dstUri.xs)("/prop:properties/prop:directory".xqy)))
          .then_ {
            for_(d in xdmp.directory(dstUri.xs, "1".xs))
            .return_(xdmp.documentDelete(xdmp.nodeUri(~d)))
          } else_ {
            xdmp.directoryCreate(dstUri.xs)
          },

        for_(
          d in xdmp.directory(srcUri.xs, "1".xs))
        .let_(
          oldName := xdmp.nodeUri(~d),
          newName := fn.concat(dstUri.xs, fn.tokenize(~oldName, "/".xs)(fn.last)))
        .return_(mkSeq_(
          xdmp.documentInsert(~newName, fn.doc(~oldName)),
          xdmp.documentDelete(~oldName)))))
    }

    def deleteSrcIfEmpty =
      session.executeQuery_[F](deleteIfEmptyXqy(srcUri.xs))

    if (src === dst) ().point[F] else doMove *> deleteSrcIfEmpty.void
  }

  def readFile[F[_]: Monad: Capture: CSourceReader: XccErr](
    chunkSize: Positive
  )(
    file: AFile,
    offset: Natural,
    limit: Option[Positive]
  ): F[ResultCursor] = {
    val uri = pathUri(asDir(file))

    val contents = cts.search(
      fn.doc(),
      cts.directoryQuery(uri.xs),
      IList(cts.indexOrder(cts.uriReference, "ascending".xs)))

    val ltd = (offset.get + 1, limit.map(_.get.xqy)) match {
      case (1, None) => contents
      case (o, l)    => fn.subsequence(contents, o.xqy, l)
    }

    contentsource.resultCursor[F](chunkSize)(session.evaluateQuery_[ReaderT[F, Session, ?]](ltd).run)
  }

  ////

  private def chunkId[F[_]: Functor: UuidReader]: F[String] =
    UuidReader[F].asks(id => toSequentialString(id) getOrElse toOpaqueString(id))

  private def deleteIfEmptyXqy(dirUri: XQuery): XQuery =
    if_ (fn.not(fn.exists(xdmp.directory(dirUri, "infinity".xs))))
      .then_ { xdmp.directoryDelete(dirUri) }
      .else_ { expr.emptySeq }
}
