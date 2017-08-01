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

package quasar.physical.marklogic.fs

import slamdata.Predef._
import quasar.{Planner => QPlanner}
import quasar.contrib.pathy._
import quasar.contrib.scalaz._
import quasar.effect.uuid._
import quasar.fp.numeric._
import quasar.fs.{FileSystemError, MonadFsErr}
import quasar.physical.marklogic.ErrorMessages
import quasar.physical.marklogic.qscript._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.physical.marklogic.xcc._

import com.marklogic.xcc.types.{XdmItem, XSString}
import eu.timepit.refined.auto._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.stream.Process

object ops {
  import expr.{func, let_}

  /** Appends the given contents to the file, which must already exist. */
  def appendToFile[F[_]: Monad: Xcc: UuidReader: PrologW: PrologL, FMT: SearchOptions, A](
    file: AFile,
    contents: A
  )(implicit
    C:  AsContent[FMT, A],
    SP: StructuralPlanner[F, FMT]
  ): F[ErrorMessages \/ Vector[XccError]] = {
    def appendQuery(src: AFile): F[MainModule] =
      main10ml {
        SP.leftShift(fileRoot[FMT](src))
          .flatMap(lib.appendChildNodes[F, FMT].apply(fileRoot[FMT](file), _))
      }

    val tmpFile = UuidReader[F] asks { uuid =>
      renameFile(file, fn => FileName(s"${fn.value}-$uuid"))
    }

    Xcc[F].transact(for {
      tmp  <- tmpFile
      errs <- insertFile[F, FMT, A](tmp, contents)
      main <- appendQuery(tmp)
      _    <- Xcc[F].execute(main)
      _    <- deleteFile(tmp)
    } yield errs)
  }

  /** Deletes the given directory and all descendants, recursively. */
  def deleteDir[F[_]: Xcc: Applicative, FMT: SearchOptions](dir: ADir): F[Executed] = {
    val file = $("file")

    Xcc[F].executeQuery(fn.map(
      func(file.render) { xdmp.documentDelete(fn.baseUri(~file)) },
      directoryDocuments[FMT](pathUri(dir).xs, true)))
  }

  /** Deletes the given file, erroring if it doesn't exist. */
  def deleteFile[F[_]: Xcc](file: AFile): F[Executed] =
    Xcc[F].executeQuery(xdmp.documentDelete(pathUri(file).xs))

  /** Returns whether any file descendants of the given dir have the specified format. */
  def descendantsHavingFormatExist[F[_]: Xcc: Functor, FMT: SearchOptions](dir: ADir): F[Boolean] = {
    val main = main10ml(lib.descendantsHavingFormatExist[W, FMT] apply pathUri(dir).xs)
    Xcc[F].results(main.value) map booleanResult
  }

  /** The set of child directories and files of the given directory. */
  def directoryContents[F[_]: Bind: Xcc, FMT: SearchOptions](dir: ADir): F[Set[PathSegment]] = {
    def parseDir(s: String): Option[PathSegment] =
      UriPathCodec.parseRelDir(s) flatMap dirName map (_.left)

    def parseFile(s: String): Option[PathSegment] =
      UriPathCodec.parseRelFile(s) map (f => fileName(f).right)

    val nextUri =
      uriLexiconEnabled[F] map (_.fold(
        lib.descendantUriFromLexicon[W].fn,
        lib.descendantUriFromDocQuery[W].fn))

    val asSegments: XdmItem => Set[PathSegment] = {
      case item: XSString =>
        val str = item.asString
        (parseDir(str) orElse parseFile(str)).toSet

      case _ => Set()
    }

    Xcc[F].transact(for {
      f     <- nextUri
      mm    =  main10ml((f >>= lib.directoryContents[W, FMT])(pathUri(dir).xs))
      items <- Xcc[F].results(mm.value)
      segs  =  items foldMap asSegments
    } yield segs)
  }

  /** Returns whether the file exists, regardless of format. */
  def fileExists[F[_]: Functor: Xcc](file: AFile): F[Boolean] =
    Xcc[F].queryResults(fn.docAvailable(pathUri(file).xs)) map booleanResult

  /** Returns whether the file having the given format exists. */
  def fileHavingFormatExists[F[_]: Functor: Xcc, FMT: SearchOptions](file: AFile): F[Boolean] =
    Xcc[F].queryResults(fn.exists(fileNode[FMT](file))) map booleanResult

  /** Insert the given contents into the file, overwriting any existing contents
    * and creating the file otherwise. Returns any errors encountered, either
    * with the contents or during the process of insertion itself.
    */
  def insertFile[F[_]: Monad: Xcc, FMT, A](
    file: AFile,
    contents: A
  )(implicit
    C: AsContent[FMT, A]
  ): F[ErrorMessages \/ Vector[XccError]] = {
    val uri         = pathUri(file)
    val contentUri  = ContentUri.getOption(uri) \/> s"Malformed content URI: $uri".wrapNel
    val content     = contentUri >>= (C.asContent[ErrorMessages \/ ?](_, contents))

    EitherT.fromDisjunction[F](content)
      .flatMapF(c => Xcc[F].insert[Id](c) map (_.right[ErrorMessages]))
      .run
  }

  /** Move `src` to `dst` overwriting any existing contents. */
  def moveDir[F[_]: Monad: Xcc, FMT: SearchOptions](src: ADir, dst: ADir): F[Executed] = {
    val (file, srcUri, dstUri) = ($("file"), $("srcUri"), $("dstUri"))

    val doMoveDir = lib.moveFile[W, FMT].fn map { moveF =>
      fn.map(
        func(file.render) {
          let_(
            srcUri := fn.baseUri(~file),
            dstUri := fn.concat(
                        pathUri(dst).xs,
                        fn.substringAfter(~srcUri, pathUri(src).xs)))
          .return_(moveF(~srcUri, ~dstUri))
        },
        directoryDocuments[FMT](pathUri(src).xs, true))
    }

    (src =/= dst) whenM Xcc[F].execute(main10ml(doMoveDir).value) as Executed.executed
  }

  /** Move `src` to `dst` overwriting any existing contents. */
  def moveFile[F[_]: Monad: Xcc, FMT: SearchOptions](src: AFile, dst: AFile): F[Executed] = {
    val main = main10ml(lib.moveFile[W, FMT] apply (pathUri(src).xs, pathUri(dst).xs))
    (src =/= dst) whenM Xcc[F].execute(main.value) as Executed.executed
  }

  /** Returns whether the given path exists having the specified format. */
  def pathHavingFormatExists[F[_]: Functor: Xcc, FMT: SearchOptions](path: APath): F[Boolean] =
    refineType(path).fold(descendantsHavingFormatExist[F, FMT], fileHavingFormatExists[F, FMT])

  /** Attempts to pretty print the given expression. */
  def prettyPrint[F[_]: Monad: MonadFsErr: Xcc](xqy: XQuery): F[Option[XQuery]] = {
    val prettyPrinted =
      Xcc[F].queryResults(xdmp.prettyPrint(XQuery(s"'$xqy'")))
        .map(_.headOption collect { case s: XSString => XQuery(s.asString) })

    Xcc[F].handleWith(prettyPrinted) {
      case err @ XccError.QueryError(_, cause) =>
        MonadFsErr[F].raiseError(
          FileSystemError.qscriptPlanningFailed(QPlanner.InternalError(
            err.shows, Some(cause))))

      // NB: As this is only for pretty printing, if we fail for some other reason
      //     just return an empty result.
      case _ => none[XQuery].point[F]
    }
  }

  /** Stream of the left-shifted contents of the given file. */
  def readFile[F[_]: Monad: Xcc: PrologL, FMT: SearchOptions](
    file: AFile,
    offset: Natural,
    limit: Option[Positive]
  )(implicit
    SP: StructuralPlanner[F, FMT]
  ): Process[F, XdmItem] = {
    val query = SP.leftShift(fileRoot[FMT](file)) map { items =>
      (offset.value + 1, limit.map(_.value.xqy)) match {
        case (1, None) => items
        case (o, l)    => fn.subsequence(items, o.xqy, l)
      }
    }

    main10ml(query).liftM[Process].flatMap(Xcc[F].evaluate)
  }

  /** Appends contents to an existing file, creating it otherwise. */
  def upsertFile[F[_]: Monad: Xcc: UuidReader: PrologW: PrologL, FMT: SearchOptions, A](
    file: AFile,
    contents: A
  )(implicit
    C:  AsContent[FMT, A],
    SP: StructuralPlanner[F, FMT]
  ): F[ErrorMessages \/ Vector[XccError]] =
    fileExists[F](file).ifM(
      appendToFile[F, FMT, A](file, contents),
      insertFile[F, FMT, A](file, contents))

  /** Returns whether the URI lexicon is enabled. */
  def uriLexiconEnabled[F[_]: Functor: Xcc]: F[Boolean] = {
    val main = main10ml {
      admin.getConfiguration[W] >>= (admin.databaseGetUriLexicon[W](_, xdmp.database()))
    }

    Xcc[F].results(main.value) map booleanResult
  }

  ////

  private type W[A] = Writer[Prologs, A]

  private def main10ml[F[_]: Functor: PrologL](query: F[XQuery]): F[MainModule] =
    MainModule fromWritten (query strengthL Version.`1.0-ml`)
}
