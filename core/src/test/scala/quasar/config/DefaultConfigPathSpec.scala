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

package quasar.config

import quasar.Predef._
import quasar.SKI._

import scala.util.Properties

import org.specs2.mutable
import pathy.Path._
import scalaz.syntax.functor._
import scalaz.syntax.std.option._
import scalaz.concurrent.Task

// NB: Not possible to test windows deterministically at this point as cannot
//     programatically set environment variables like we can with properties.
class DefaultConfigPathSpec extends mutable.Specification {
  import ConfigOps.defaultPathForOS, FsPath._

  sequential

  val comp = "quasar-config.json"
  val macp = "Library/Application Support"
  val posixp = ".config"

  def printPosix[T](fp: FsPath[T, Sandboxed]) =
    printFsPath(posixCodec, fp)

  def getProp(n: String): Task[Option[String]] =
    Task.delay(Properties.propOrNone(n))

  def setProp(n: String, v: String): Task[Unit] =
    Task.delay(Properties.setProp(n, v)).void

  def clearProp(n: String): Task[Unit] =
    Task.delay(Properties.clearProp(n)).void

  def withProp[A](n: String, v: String, t: => Task[A]): Task[A] =
    for {
      prev <- getProp(n)
      _    <- setProp(n, v)
      a    <- t onFinish κ(prev.cata(setProp(n, _), Task.now(())))
    } yield a

  def withoutProp[A](n: String, t: => Task[A]): Task[A] =
    for {
      prev <- getProp(n)
      _    <- clearProp(n)
      a    <- t onFinish κ(prev.cata(setProp(n, _), Task.now(())))
    } yield a

  "defaultPathForOS" should {
    "OS X" >> {
      "when home dir" in {
        val p = withProp("user.home", "/home/foo", defaultPathForOS(file("quasar-config.json"))(OS.mac))
        printPosix(p.run) ==== s"/home/foo/$macp/$comp"
      }

      "no home dir" in {
        val p = withoutProp("user.home", defaultPathForOS(file("quasar-config.json"))(OS.mac))
        printPosix(p.run) ==== s"./$macp/$comp"
      }
    }

    "POSIX" >> {
      "when home dir" in {
        val p = withProp("user.home", "/home/bar", defaultPathForOS(file("quasar-config.json"))(OS.posix))
        printPosix(p.run) ==== s"/home/bar/$posixp/$comp"
      }

      "when home dir with trailing slash" in {
        val p = withProp("user.home", "/home/bar/", defaultPathForOS(file("quasar-config.json"))(OS.posix))
        printPosix(p.run) ==== s"/home/bar/$posixp/$comp"
      }

      "no home dir" in {
        val p = withoutProp("user.home", defaultPathForOS(file("quasar-config.json"))(OS.posix))
        printPosix(p.run) ==== s"./$posixp/$comp"
      }
    }
  }
}
