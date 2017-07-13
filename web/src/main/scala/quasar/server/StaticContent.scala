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

package quasar.server

import slamdata.Predef._
import quasar.main._

import java.net.{JarURLConnection, URI, URLDecoder}

import scalaz.std.option._
import scalaz.syntax.monad._
import scalaz.concurrent.Task

/** Describes the virtual and physical location of static content served
  * by Quasar.
  */
final case class StaticContent(loc: String, path: String)

object StaticContent {
  def fromCliOptions(defaultLoc: String, opts: CliOptions): MainTask[Option[StaticContent]] = {
    def path(p: String): Task[String] =
      if (opts.contentPathRelative) jarPath.map(_ + p)
      else p.point[Task]

    (opts.contentLoc, opts.contentPath) match {
      case (None, None) =>
        none.point[MainTask]
      case (Some(_), None) =>
        MainTask.raiseError("content-location specified but not content-path")
      case (loc, Some(cp)) =>
        path(cp).map(p => some(StaticContent(loc getOrElse defaultLoc, p))).liftM[MainErrT]
    }
  }

  ////

  /** NB: This is a terrible thing. Is there a better way to find the path to
    *     a jar?
    */
  private val jarPath: Task[String] = Task.delay {
    // NB: This is the “right” way to get a `java.net.JarURLconnection`.
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    def altPath(uri: URI) =
      uri.toURL.openConnection
        .asInstanceOf[JarURLConnection]
        .getJarFileURL.getPath

    val uri  = getClass.getProtectionDomain.getCodeSource.getLocation.toURI
    val path = URLDecoder.decode(Option(uri.getPath) getOrElse altPath(uri), "UTF-8")

    (new java.io.File(path)).getParentFile.getPath + "/"
  }
}
