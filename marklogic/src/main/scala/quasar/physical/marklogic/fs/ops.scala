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

import quasar.Predef._
import quasar.contrib.pathy._
import quasar.contrib.scalaz._
import quasar.effect.uuid._
import quasar.fp.numeric._
import quasar.physical.marklogic.ErrorMessages
import quasar.physical.marklogic.qscript._
import quasar.physical.marklogic.xml.NCName
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.physical.marklogic.xcc._, Xcc.ops._

import com.marklogic.xcc.types.{XdmItem, XSBoolean, XSString}
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

  /** Returns whether the server is configured to create directories automatically. */
  def automaticDirectoryCreationEnabled[F[_]: Functor: Xcc]: F[Boolean] = {
    val main = main10ml {
      admin.getConfiguration[W]
        .flatMap(admin.databaseGetDirectoryCreation[W](_, xdmp.database()))
    }

    Xcc[F].results(main.value) map {
      case Vector(createMode: XSString) => createMode.asString === "automatic"
      case _                            => false
    }
  }

  /** Deletes the given directory and all descendants, recursively. */
  def deleteDir[F[_]: Xcc: Applicative, FMT: SearchOptions](dir: ADir): F[Executed] = {
    val file = $("file")

    val deleteFiles =
      fn.map(
        func(file.render) { xdmp.documentDelete(fn.baseUri(~file)) },
        directoryDocuments[FMT](pathUri(dir).xs, true))

    (Xcc[F].executeQuery(deleteFiles) *> deleteEmptyDescendantDirectories[F](dir)).transact
  }

  /** Delete all empty descendant directories. */
  def deleteEmptyDescendantDirectories[F[_]: Xcc](dir: ADir): F[Executed] = {
    val query =
      lib.emptyDescendantDirectories[W].apply(pathUri(dir).xs)
        .map(empties => fn.map(xdmp.ns(NCName("document-delete")) :# 1, empties))

    Xcc[F].execute(main10ml(query).value)
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
  def directoryContents[F[_]: Functor: Xcc, FMT: SearchOptions](dir: ADir): F[Set[PathSegment]] = {
    def parseDir(s: String): Option[PathSegment] =
      UriPathCodec.parseRelDir(s) flatMap dirName map (_.left)

    def parseFile(s: String): Option[PathSegment] =
      UriPathCodec.parseRelFile(s) map (f => fileName(f).right)

    val main = main10ml(lib.directoryContents[W, FMT] apply pathUri(dir).xs)

    Xcc[F].results(main.value) map (_ foldMap {
      case item: XSString =>
        val str = item.asString
        (parseDir(str) orElse parseFile(str)).toSet

      case _ => Set()
    })
  }

  /** Ensure the given directory exists, creating it if necessary. */
  def ensureDirectory[F[_]: Monad: Xcc](dir: ADir): F[Executed] = {
    import XccError.Code

    def isDirExists(code: String) =
      Code.string.getOption(code) exists (_ === Code.DirExists)

    Xcc[F].executeQuery(xdmp.directoryCreate(pathUri(dir).xs)) handle {
      case XccError.QueryError(_, c) if isDirExists(c.getCode) => Executed.executed
    }
  }

  /** Ensure the given directory and all ancestors exist, creating them if necessary. */
  def ensureLineage[F[_]: Monad: Xcc](dir: ADir): F[Executed] = {
    def ensureLineage0(d: ADir): F[Executed] =
      ensureDirectory[F](d) <* parentDir(d).traverse(ensureLineage0)

    automaticDirectoryCreationEnabled[F]
      .ifM(ensureDirectory[F](dir), ensureLineage0(dir))
      .transact
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
    val decodeDir: XdmItem => Option[ADir] = {
      case str: XSString => UriPathCodec.parseAbsDir(str.asString) map (sandboxAbs(_))
      case _             => None
    }

    val (file, srcUri, dstUri) = ($("file"), $("srcUri"), $("dstUri"))

    val dstDirs = (
      lib.moveFile[W, FMT].apply(~srcUri, ~dstUri) |@|
      lib.fileParent[W].apply(~dstUri)
    ) { (doMove, parentDir) =>
        fn.distinctValues(fn.map(
          func(file.render) {
            let_(
              srcUri := fn.baseUri(~file),
              dstUri := fn.concat(
                          pathUri(dst).xs,
                          fn.substringAfter(~srcUri, pathUri(src).xs)),
              $("_") := doMove)
            .return_(parentDir)
          },
          directoryDocuments[FMT](pathUri(src).xs, true)))
    }

    val doMoveDir = for {
      items <- Xcc[F].results(main10ml(dstDirs).value)
      dirs  =  items.map(decodeDir).unite
      _     <- dirs traverse_ (ensureLineage[F](_).void)
      _     <- deleteEmptyDescendantDirectories[F](src)
    } yield ()

    (src =/= dst) whenM doMoveDir.transact as Executed.executed
  }

  /** Move `src` to `dst` overwriting any existing contents. */
  def moveFile[F[_]: Monad: Xcc, FMT: SearchOptions](src: AFile, dst: AFile): F[Executed] = {
    val main = main10ml(lib.moveFile[W, FMT] apply (pathUri(src).xs, pathUri(dst).xs))
    (src =/= dst) whenM Xcc[F].execute(main.value) as Executed.executed
  }

  /** Returns whether the given path exists having the specified format. */
  def pathHavingFormatExists[F[_]: Functor: Xcc, FMT: SearchOptions](path: APath): F[Boolean] =
    refineType(path).fold(descendantsHavingFormatExist[F, FMT], fileHavingFormatExists[F, FMT])

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

  ////

  private type W[A] = Writer[Prologs, A]

  private val booleanResult: Vector[XdmItem] => Boolean = {
    case Vector(b: XSBoolean) => b.asPrimitiveBoolean
    case _                    => false
  }

  private def main10ml[F[_]: Functor: PrologL](query: F[XQuery]): F[MainModule] =
    MainModule fromWritten (query strengthL Version.`1.0-ml`)
}
