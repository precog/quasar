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

package quasar.config

import slamdata.Predef._
import quasar.config.FsPath._
import quasar.contrib.pathy._
import quasar.Errors.ETask

import java.nio.file.{Path => _, _}
import java.nio.charset._
import scala.util.Properties._

import argonaut._
import monocle.Lens
import monocle.syntax.fields._
import pathy._, Path._
import scalaz.{Lens => _, _}, Scalaz._
import scalaz.concurrent.Task
import simulacrum.typeclass

@typeclass trait ConfigOps[C] {
  import ConfigOps._, ConfigError._

  def name: String

  def metaStoreConfig: Lens[C, Option[MetaStoreConfig]]

  def default: Task[C]

  def fromOptionalFile(configFile: Option[FsFile])(implicit d: DecodeJson[C]): CfgTask[C] = {
    configFile.fold(fromDefaultPath)(fromFile)
  }

  def fromFile(path: FsFile)(implicit D: DecodeJson[C]): CfgTask[C] =
    for {
      cfgText <- textFromFile(path.some)
      config  <- EitherT.fromDisjunction[Task](fromString(cfgText.text))
                   .leftMap(malformedRsn.modify(rsn =>
                      s"Failed to parse ${cfgText.displayPath}: $rsn"))
      _       <- Task.delay(
                   println(s"Read $name config from ${cfgText.displayPath}")
                 ).liftM[CfgErrT]
    } yield config

  /** Loads configuration from one of the OS-specific default paths. */
  def fromDefaultPath(implicit D: DecodeJson[C]): CfgTask[C] = {
    def load(path: Task[FsFile]): CfgTask[C] =
      path.liftM[CfgErrT] flatMap fromFile

    merr.handleError(load(defaultPath)) {
      case fnf @ FileNotFound(_) =>
        merr.handleError(load(legacyDefaultPath)) {
          // Because we only want to expose the current 'default' path, not the
          // legacy one, if both fail.
          case FileNotFound(_) => merr.raiseError(fnf)
          case err             => merr.raiseError(err)
        }
      case err => merr.raiseError(err)
    }
  }

  def toFile(config: C, path: Option[FsFile])(implicit E: EncodeJson[C]): Task[Unit] =
    textToFile(asString(config), path)
}

object ConfigOps {
  import ConfigError._

  final case class ConfigText(text: String, displayPath: String)

  /** NB: Paths read from environment/props are assumed to be absolute. */
  def defaultPathForOS(file: RFile)(os: OS): Task[FsFile] = {
    def localAppData: OptionT[Task, FsPath.Aux[Abs, Dir, Sandboxed]] =
      OptionT(Task.delay(envOrNone("LOCALAPPDATA")))
        .flatMap(s => OptionT(parseWinAbsAsDir(s).point[Task]))

    def homeDir: OptionT[Task, FsPath.Aux[Abs, Dir, Sandboxed]] =
      OptionT(Task.delay(propOrNone("user.home")))
        .flatMap(s => OptionT(parseAbsAsDir(os, s).point[Task]))

    val dirPath: RDir = os.fold(
      currentDir,
      dir("Library") </> dir("Application Support"),
      dir(".config"))

    val baseDir = OptionT.some[Task, Boolean](os.isWin)
      .ifM(localAppData, OptionT.none)
      .orElse(homeDir)
      .map(_.forgetBase)
      .getOrElse(Uniform(currentDir))

    baseDir map (_ </> dirPath </> file)
  }

  def textFromFile(file: Option[FsFile]): CfgTask[ConfigText] = {
    def attemptReadFile(f: FsFile, fStr: String): CfgTask[String] = {
      val read = Task.delay {
        new String(Files.readAllBytes(Paths.get(fStr)), StandardCharsets.UTF_8)
      }

      EitherT(read map (_.right[ConfigError]) handle {
        case ex: java.nio.file.NoSuchFileException =>
          fileNotFound(f).left
      })
    }

    for {
      codec   <- systemCodec.liftM[CfgErrT]
      f       <- file.fold(defaultPath)(Task.now).liftM[CfgErrT]
      strPath =  printFsPath(codec, f)
      text    <- attemptReadFile(f, strPath)
    } yield ConfigText(text, strPath)
  }

  def jsonFromFile(path: Option[FsFile]): CfgTask[Json] =
    textFromFile(path) >>= (cfgText =>
      EitherT.fromDisjunction[Task](fromString[Json](cfgText.text))
        .leftMap(malformedRsn.modify(rsn => s"Failed to parse ${cfgText.displayPath}: $rsn")))

  def textToFile(text: String, path: Option[FsFile]): Task[Unit] =
     for {
       codec <- systemCodec
       pStr  <- path.fold(defaultPath)(Task.now)
       jPath <- Task.delay(Paths.get(printFsPath(codec, pStr)))
       _     <- Task.delay {
                  Option(jPath.getParent) foreach (Files.createDirectories(_))
                  Files.write(jPath, text.getBytes(StandardCharsets.UTF_8))
                }
     } yield ()

  def jsonToFile(json: Json, path: Option[FsFile]): Task[Unit] =
    textToFile(asString(json), path)

  def fromString[A](value: String)(implicit D: DecodeJson[A]): ConfigError \/ A =
    \/.fromEither(Parse.decodeEither[A](value).leftMap(malformedConfig(value, _)))

  def asString[A](a: A)(implicit E: EncodeJson[A]): String =
    E.encode(a).pretty(quasar.fp.multiline)

  ////

  private def merr = MonadError[ETask[ConfigError,?], ConfigError]
  private val malformedRsn = malformedConfig composeLens _2

  /** The default path to the configuration file for the current operating system. */
  private def defaultPath: Task[FsFile] =
    OS.currentOS >>= defaultPathForOS(dir("quasar") </> file("quasar-config.json"))

  /**
   * The default path in a previous version of the software, used to ease the
   * transition to the new location.
   */
  private def legacyDefaultPath: Task[FsFile] =
    OS.currentOS >>= defaultPathForOS(dir("SlamData") </> file("slamengine-config.json"))

}
