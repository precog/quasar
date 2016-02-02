/*
 * Copyright 2014 - 2015 SlamData Inc.
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

import quasar.Predef._
import quasar.config.FsPath._
import quasar.fp._
import quasar.fs.{Path => _, _}
import quasar.fs.mount.MountingsConfig2

import java.nio.file.{Path => _, _}
import java.nio.charset._
import scala.util.Properties._

import argonaut._, Argonaut._
import monocle.Lens
import monocle.syntax.fields._
import monocle.std.tuple2._
import pathy._, Path._
import scalaz.{Lens => _, _}, Scalaz._
import scalaz.concurrent.Task

trait ConfigOps[C] {
  import ConfigOps._, ConfigError._

  val default: C
  def mountingsLens: Lens[C, MountingsConfig2]

  def fromFile(path: FsFile)(implicit D: DecodeJson[C]): CfgTask[C] = {
    def attemptReadFile(f: String): CfgTask[String] = {
      val read = Task.delay {
        new String(Files.readAllBytes(Paths.get(f)), StandardCharsets.UTF_8)
      }

      EitherT(read map (_.right[ConfigError]) handle {
        case ex: java.nio.file.NoSuchFileException =>
          fileNotFound(path).left
      })
    }

    for {
      codec   <- systemCodec.liftM[CfgErrT]
      strPath =  printFsPath(codec, path)
      text    <- attemptReadFile(strPath)
      config  <- EitherT.fromDisjunction[Task](fromString(text))
                   .leftMap(malformedRsn.modify(rsn => s"Failed to parse $strPath: $rsn"))
      _       <- Task.delay(println(s"Read config from $strPath")).liftM[CfgErrT]
    } yield config
  }

  def fromFileOrDefaultPaths(path: Option[FsFile])(implicit D: DecodeJson[C]): CfgTask[C] = {
    def load(path: Task[FsFile]): CfgTask[C] =
      path.liftM[CfgErrT] flatMap fromFile

    path.cata(fromFile,load(defaultPath) orElse_bug_free load(alternatePath))
  }

  def get(path: Option[FsFile])(implicit D: DecodeJson[C]): Task[C] =
    fromFileOrDefaultPaths(path) getOrElse default

  def toFile(config: C, path: Option[FsFile])(implicit E: EncodeJson[C]): Task[Unit] =
    for {
      codec <- systemCodec
      pStr  <- path.fold(defaultPath)(Task.now)
      text  =  asString(config)
      jPath <- Task.delay(Paths.get(printFsPath(codec, pStr)))
      _     <- Task.delay {
                 Option(jPath.getParent) foreach (Files.createDirectories(_))
                 Files.write(jPath, text.getBytes(StandardCharsets.UTF_8))
               }
    } yield ()

  def fromString(value: String)(implicit D: DecodeJson[C]): ConfigError \/ C =
    Parse.decodeEither[C](value).leftMap(malformedConfig(value, _))

  def asString(config: C)(implicit E: EncodeJson[C]): String =
    E.encode(config).pretty(quasar.fp.multiline)

  ////

  private val malformedRsn = malformedConfig composeLens _2

  /**
   * The default path to the configuration file for the current operating system.
   * NB: Paths read from environment/props are assumed to be absolute.
   */
  private def defaultPath: Task[FsFile] =
    OS.currentOS >>= defaultPathForOS(dir("quasar") </> file("quasar-config.json"))

  private def alternatePath: Task[FsFile] =
    OS.currentOS >>= defaultPathForOS(dir("SlamData") </> file("slamengine-config.json"))
}

object ConfigOps {
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
}
