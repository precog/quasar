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

import com.mongodb.ConnectionString
import quasar.Predef._
import quasar.fp._
import quasar._, Evaluator._, Errors._, generic._
import quasar.fs.{Path => EnginePath}

import java.io.{File => JFile}
import scala.util.Properties._
import argonaut._, Argonaut._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import shapeless.{Path => _, _}, labelled._, ops.record._
import simulacrum.typeclass
import pathy._, Path._

final case class SDServerConfig(port: Int)

object SDServerConfig {
  val DefaultPort = 20223

  implicit def Codec = CodecJson(
      (s: SDServerConfig) =>
        ("port" := s.port) ->:
        jEmptyObject,
      c => for {
        port <- (c --\ "port").as[Option[Int]]
      } yield SDServerConfig(port.getOrElse(DefaultPort)))

  implicit object empty extends Empty[SDServerConfig] { def empty = SDServerConfig(SDServerConfig.DefaultPort) }
}

final case class Credentials(username: String, password: String)

object Credentials {
  implicit def Codec = casecodec2(Credentials.apply, Credentials.unapply)("username", "password")
}

sealed trait BackendConfig {
  def validate(path: EnginePath): EnvironmentError \/ Unit
}
final case class MongoDbConfig(uri: ConnectionString) extends BackendConfig {
  def validate(path: EnginePath) = for {
    _ <- if (path.relative) -\/(InvalidConfig("Not an absolute path: " + path)) else \/-(())
    _ <- if (!path.pureDir) -\/(InvalidConfig("Not a directory path: " + path)) else \/-(())
  } yield ()
}
object MongoConnectionString {
  def parse(uri: String): String \/ ConnectionString =
    \/.fromTryCatchNonFatal(new ConnectionString(uri)).leftMap(_.toString)

  def decode(uri: String): DecodeResult[ConnectionString] = {
    DecodeResult(parse(uri).leftMap(κ((s"invalid connection URI: $uri", CursorHistory(Nil)))))
  }
  implicit val codec = CodecJson[ConnectionString](
    c => jString(c.getURI),
    cursor => cursor.as[String].flatMap(decode))
}
object MongoDbConfig {
  import MongoConnectionString.codec
  implicit def Codec = casecodec1(MongoDbConfig.apply, MongoDbConfig.unapply)("connectionUri")
}

object BackendConfig {
  implicit def BackendConfig = CodecJson[BackendConfig](
    encoder = _ match {
      case x @ MongoDbConfig(_) => ("mongodb", MongoDbConfig.Codec.encode(x)) ->: jEmptyObject
    },
    decoder = _.get[MongoDbConfig]("mongodb").map(v => v: BackendConfig))
}

final case class Config(
  server:    SDServerConfig,
  mountings: Map[EnginePath, BackendConfig])

object Config {
  implicit val MapCodec = CodecJson[Map[EnginePath, BackendConfig]](
    encoder = map => map.map(t => t._1.pathname -> t._2).asJson,
    decoder = cursor => implicitly[DecodeJson[Map[String, BackendConfig]]].decode(cursor).map(_.map(t => EnginePath(t._1) -> t._2)))

  implicit val configCodecJson = casecodec2(Config.apply, Config.unapply)("server", "mountings")

  implicit object empty extends Empty[Config] { def empty = Config(Empty[SDServerConfig].empty, Map()) }

  implicit val show = ConfigOps.show[Config]
}

@typeclass trait Empty[A] {
  def empty: A
}

object ConfigOps {
  def show[C: EncodeJson] = new Show[C] {
    override def shows(f: C): String = EncodeJson.of[C].encode(f).pretty(quasar.fp.multiline)
  }
}

class ConfigOps
  [C, S, LC <: HList, LS <: HList]
  (ev: Ev[C, S, LC, LS]) {
  import ev._
  import FsPath._

  def defaultPathForOS(file: RelFile[Sandboxed])(os: OS): Task[FsPath[File, Sandboxed]] = {
    def localAppData: OptionT[Task, FsPath.Aux[Abs, Dir, Sandboxed]] =
      OptionT(Task.delay(envOrNone("LOCALAPPDATA")))
        .flatMap(s => OptionT(parseWinAbsAsDir(s).point[Task]))

    def homeDir: OptionT[Task, FsPath.Aux[Abs, Dir, Sandboxed]] =
      OptionT(Task.delay(propOrNone("user.home")))
        .flatMap(s => OptionT(parseAbsAsDir(os, s).point[Task]))

    val dirPath: RelDir[Sandboxed] = os.fold(
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

  /**
   * The default path to the configuration file for the current operating system.
   *
   * NB: Paths read from environment/props are assumed to be absolute.
   */
  private def defaultPath: Task[FsPath[File, Sandboxed]] =
    OS.currentOS >>= defaultPathForOS(dir("quasar") </> file("quasar-config.json"))

  private def alternatePath: Task[FsPath[File, Sandboxed]] =
    OS.currentOS >>= defaultPathForOS(dir("SlamData") </> file("slamengine-config.json"))

  def fromFile(path: FsPath[File, Sandboxed]): EnvTask[C] = {
    import java.nio.file._
    import java.nio.charset._

    for {
      codec  <- liftE[EnvironmentError](systemCodec)
      strPath = printFsPath(codec, path)
      text   <- liftE[EnvironmentError](Task.delay(
                  new String(Files.readAllBytes(Paths.get(strPath)), StandardCharsets.UTF_8)))
      config <- EitherT(Task.now(fromString(text).leftMap {
                  case InvalidConfig(message) => InvalidConfig("Failed to parse " + path + ": " + message)
                  case e => e
                }))
    } yield config
  }

  def fromFileOrEmpty(path: Option[FsPath[File, Sandboxed]]): EnvTask[C] = {
    def loadOr(path: FsPath[File, Sandboxed], alt: EnvTask[C]): EnvTask[C] =
      handleWith(fromFile(path)) {
        case _: java.nio.file.NoSuchFileException => alt
      }

    val empty = liftE[EnvironmentError](Task.now(Empty[C].empty))

    path match {
      case Some(path) =>
        loadOr(path, empty)
      case None =>
        liftE(defaultPath).flatMap { p =>
          loadOr(p, liftE(alternatePath).flatMap { p =>
            loadOr(p, empty)
          })
        }
    }
  }

  def loadAndTest(path: FsPath[File, Sandboxed]): EnvTask[C] = for {
    config <- fromFile(path)
    _      <- rec(config).mountings.values.toList.map(Backend.test).sequenceU
  } yield config

  def toFile(config: C, path: Option[FsPath[File, Sandboxed]])(implicit encoder: EncodeJson[C]): Task[Unit] = {
    import java.nio.file._
    import java.nio.charset._

    for {
      codec <- systemCodec
      p1    <- path.fold(defaultPath)(Task.now)
      cfg   <- Task.delay {
        val text = config.shows
        val p = Paths.get(printFsPath(codec, p1))
        ignore(Option(p.getParent).map(Files.createDirectories(_)))
        ignore(Files.write(p, text.getBytes(StandardCharsets.UTF_8)))
        ()
      }
    } yield cfg
  }

  def fromString(value: String): EnvironmentError \/ C =
    Parse.decodeEither[C](value).leftMap(InvalidConfig(_))
}

object Ev {
  val serverWitness = Witness('server)
  val mountingsWitness = Witness('mountings)
  val portWitness = Witness('port)

  def construct
    [C, S, LC <: HList, LS <: HList](
    config: C,
    server: S)
    (implicit
    _SC: Show[C],
    _EC: Empty[C],
    _ES: Empty[S],
    _CC: CodecJson[C],
    _LGC: LabelledGeneric.Aux[C, LC],
    _LGS: LabelledGeneric.Aux[S, LS],
    _SM: Selector.Aux[LC, Ev.mountingsWitness.T, Map[EnginePath, BackendConfig]],
    _SS: Selector.Aux[LC, Ev.serverWitness.T, S],
    _SP: Selector.Aux[LS, Ev.portWitness.T, Int],
    _US: Updater.Aux[LC, FieldType[Ev.serverWitness.T, S], LC],
    _UM: Updater.Aux[LC, FieldType[Ev.mountingsWitness.T, Map[EnginePath, BackendConfig]], LC],
    _UP: Updater.Aux[LS, FieldType[Ev.portWitness.T, Int], LS]) =
    new Ev[C, S, LC, LS] {
    implicit val SC = _SC
    implicit val EC = _EC
    implicit val ES = _ES
    implicit val CC = _CC
    implicit val LGC = _LGC
    implicit val LGS = _LGS
    implicit val SM = _SM
    implicit val SS = _SS
    implicit val SP = _SP
    implicit val US = _US
    implicit val UM = _UM
    implicit val UP = _UP
    }

  val ev = Ev.construct(Empty[Config].empty, Empty[SDServerConfig].empty)
}

trait Ev[C, S, LC <: HList, LS <: HList] {
  implicit def SC: Show[C]
  implicit def EC: Empty[C]
  implicit def ES: Empty[S]
  implicit def CC: CodecJson[C]
  implicit def LGC: LabelledGeneric.Aux[C, LC]
  implicit def LGS: LabelledGeneric.Aux[S, LS]
  implicit def SM: Selector.Aux[LC, Ev.mountingsWitness.T, Map[EnginePath, BackendConfig]]
  implicit def SS: Selector.Aux[LC, Ev.serverWitness.T, S]
  implicit def SP: Selector.Aux[LS, Ev.portWitness.T, Int]
  implicit def US: Updater.Aux[LC, FieldType[Ev.serverWitness.T, S], LC]
  implicit def UM: Updater.Aux[LC, FieldType[Ev.mountingsWitness.T, Map[EnginePath, BackendConfig]], LC]
  implicit def UP: Updater.Aux[LS, FieldType[Ev.portWitness.T, Int], LS]
}

object configConfOps extends ConfigOps(Ev.ev)
